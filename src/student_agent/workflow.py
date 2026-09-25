from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from . import OUTPUT_SCHEMA_VERSION
from .mcp_gateway import EvidenceGateway, MCPToolError
from .trace import TraceWriter

ISSUES = {
    "canceled_order_paid",
    "unavailable_order_paid",
    "late_delivery_seller",
    "late_delivery_logistics",
    "valid_split_payment",
    "payment_mismatch",
    "duplicate_charge",
    "refund_pending",
    "refund_failed",
    "unsupported_claim",
}


def _records(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, dict):
        for key in ("items", "orders", "payments", "events", "refunds", "results", "data"):
            nested = value.get(key)
            if isinstance(nested, list):
                return [item for item in nested if isinstance(item, dict)]
        return [value]
    return []


def _value(record: Any, names: tuple[str, ...]) -> Any:
    if isinstance(record, dict):
        for name in names:
            value = record.get(name)
            if value is not None:
                return value
        for child in record.values():
            found = _value(child, names)
            if found is not None:
                return found
    elif isinstance(record, list):
        for child in record:
            found = _value(child, names)
            if found is not None:
                return found
    return None


def _timeline_events(value: Any) -> list[dict[str, Any]]:
    """Read the event ledger, not the payment summary beside it."""
    if isinstance(value, dict) and "events" in value:
        return _records(value["events"])
    return _records(value)


def _all_values(record: Any, names: tuple[str, ...]) -> list[Any]:
    values: list[Any] = []
    if isinstance(record, dict):
        for key, value in record.items():
            if key in names and value is not None:
                values.append(value)
            else:
                values.extend(_all_values(value, names))
    elif isinstance(record, list):
        for child in record:
            values.extend(_all_values(child, names))
    return values


def _money(value: Any) -> Decimal | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return Decimal(str(value).replace(",", "."))
    except (InvalidOperation, ValueError):
        return None


def _amount(records: list[dict[str, Any]], names: tuple[str, ...]) -> Decimal | None:
    amounts: list[Decimal] = []
    seen: set[str] = set()
    for record in records:
        value = _value(record, names)
        parsed = _money(value)
        if parsed is None:
            continue
        identity = _value(
            record, ("transaction_id", "payment_id", "capture_id", "refund_id", "event_id")
        )
        token = (
            str(identity)
            if identity is not None
            else json.dumps(record, sort_keys=True, default=str)
        )
        if token not in seen:
            seen.add(token)
            amounts.append(parsed)
    return sum(amounts, Decimal("0")) if amounts else None


def _decimal_json(value: Decimal | None) -> float | None:
    if value is None:
        return None
    return float(value.quantize(Decimal("0.01")))


def _ids(records: list[dict[str, Any]], names: tuple[str, ...]) -> list[str]:
    result: list[str] = []
    for record in records:
        value = _value(record, names)
        if isinstance(value, (str, int)) and str(value) and str(value) not in result:
            result.append(str(value))
    return result


def _time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed
    except ValueError:
        return None


def _ref(evidence: dict[str, Any]) -> str:
    return str(evidence["evidence_ref"])


async def solve_case(
    case: dict[str, Any], gateway: EvidenceGateway, trace: TraceWriter
) -> dict[str, Any]:
    case_id = str(case["case_id"])
    request = case.get("customer_request") or {}
    claims = [claim for claim in request.get("claims", []) if isinstance(claim, dict)]
    candidates = case.get("candidate_order_ids") or []
    claimed_id = request.get("claimed_order_id")
    hint = case.get("customer_unique_id_hint")
    evidence: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    transport_broken = False

    async def fetch(actor: str, tool: str, **arguments: str) -> dict[str, Any] | None:
        nonlocal transport_broken
        if transport_broken:
            return None
        try:
            result = await gateway.call(tool, case_id=case_id, **arguments)
        except MCPToolError:
            errors.append(tool + ":tool_error")
            trace.emit(
                case_id=case_id,
                event_type="handoff",
                actor=actor,
                target="coordinator",
                decision_code="tool_unavailable",
                attributes={"tool": tool, "error_type": "MCPToolError"},
            )
            return None
        except Exception as exc:
            errors.append(tool + ":transport_error")
            transport_errors = getattr(gateway, "transport_errors", None)
            if isinstance(transport_errors, set):
                transport_errors.add(case_id)
            trace.emit(
                case_id=case_id,
                event_type="handoff",
                actor=actor,
                target="coordinator",
                decision_code="evidence_unavailable",
                attributes={"tool": tool, "error_type": type(exc).__name__[:60]},
            )
            transport_broken = True
            return None
        evidence[result["evidence_ref"]] = result
        trace.emit(
            case_id=case_id,
            event_type="tool_result_consumed",
            actor=actor,
            tool_name=tool,
            evidence_refs=[result["evidence_ref"]],
            attributes={"domain": result["domain"]},
        )
        return result

    async def fetch_group(
        calls: list[tuple[str, str, dict[str, str]]],
    ) -> list[dict[str, Any] | None]:
        semaphore = asyncio.Semaphore(4)

        async def bounded_fetch(
            actor: str, tool: str, arguments: dict[str, str]
        ) -> dict[str, Any] | None:
            async with semaphore:
                return await fetch(actor, tool, **arguments)

        return await asyncio.gather(
            *(bounded_fetch(actor, tool, arguments) for actor, tool, arguments in calls)
        )

    trace.emit(
        case_id=case_id,
        event_type="task_assigned",
        actor="coordinator",
        target="entity-agent",
        decision_code="resolve_candidates",
    )
    candidate_ids = [
        candidate for candidate in candidates[:20] if isinstance(candidate, str) and candidate
    ]
    initial_calls = [
        (
            "policy-agent",
            "get_policy",
            {"policy_version": str(case.get("policy_version", ""))},
        )
    ]
    history_index: int | None = None
    if case.get("investigation_scope", {}).get("include_customer_history") and hint:
        history_index = len(initial_calls)
        initial_calls.append(
            ("entity-agent", "get_customer_history", {"customer_unique_id": str(hint)})
        )
    candidate_start = len(initial_calls)
    initial_calls.extend(
        ("entity-agent", "get_order", {"order_id": candidate}) for candidate in candidate_ids
    )
    initial_results = await fetch_group(initial_calls)
    policy_evidence = initial_results[0]
    history_evidence = initial_results[history_index] if history_index is not None else None
    policy_data = policy_evidence.get("data", {}) if policy_evidence else {}
    rules = policy_data.get("rules", {}) if isinstance(policy_data, dict) else {}

    orders: dict[str, tuple[dict[str, Any], str]] = {}
    for candidate, item in zip(candidate_ids, initial_results[candidate_start:], strict=True):
        if item is None:
            continue
        order_records = _records(item.get("data"))
        record = next(
            (value for value in order_records if str(_value(value, ("order_id",))) == candidate),
            order_records[0] if len(order_records) == 1 else None,
        )
        if record is not None:
            orders[candidate] = (record, _ref(item))

    history_records = _records(history_evidence.get("data")) if history_evidence else []
    history_order_ids = set(_ids(history_records, ("order_id",)))

    hint_match: list[str] = []
    if hint:
        hint_match = [
            order_id
            for order_id, (record, _) in orders.items()
            if str(_value(record, ("customer_unique_id",))) == str(hint)
        ]
    if len(hint_match) == 1:
        resolved_id = hint_match[0]
        resolution_status = "resolved"
    elif claimed_id in hint_match:
        resolved_id = str(claimed_id)
        resolution_status = "resolved"
    elif not hint_match and hint and len(history_order_ids.intersection(orders)) == 1:
        resolved_id = next(iter(history_order_ids.intersection(orders)))
        resolution_status = "resolved"
    elif (
        not hint_match
        and hint
        and claimed_id in orders
        and _value(orders[claimed_id][0], ("customer_unique_id",)) is None
    ):
        # The submitted order claim is usable when the source omits the join key;
        # an explicit mismatch with the customer hint is never enough to resolve it.
        resolved_id = str(claimed_id)
        resolution_status = "resolved"
    elif not hint and claimed_id in orders:
        resolved_id = str(claimed_id)
        resolution_status = "resolved"
    elif not hint and len(orders) == 1:
        resolved_id = next(iter(orders))
        resolution_status = "resolved"
    elif len(orders) > 1:
        resolved_id = None
        resolution_status = "ambiguous"
    else:
        resolved_id = None
        resolution_status = "not_found"

    rejected = [
        order_id
        for order_id, (record, _) in orders.items()
        if (resolved_id is None or order_id != resolved_id)
        and hint
        and _value(record, ("customer_unique_id",)) is not None
        and str(_value(record, ("customer_unique_id",))) != str(hint)
    ]
    entity_confidence = (
        0.94
        if len(hint_match) == 1
        else 0.88
        if resolved_id and resolved_id in history_order_ids
        else 0.70
        if resolved_id
        else 0.25
    )
    trace.emit(
        case_id=case_id,
        event_type="handoff",
        actor="entity-agent",
        target="investigation-coordinator",
        decision_code=resolution_status,
        evidence_refs=[orders[resolved_id][1]] if resolved_id else [],
    )

    order: dict[str, Any] = orders[resolved_id][0] if resolved_id else {}
    item_evidence = payment_evidence = shipment_evidence = None
    payment_timeline = refund_timeline = None
    if resolved_id is not None:
        trace.emit(
            case_id=case_id,
            event_type="task_assigned",
            actor="coordinator",
            target="order-product-agent",
            decision_code="investigate_resolved_order",
        )
        specialist_calls = [
            (
                "order-product-agent",
                "get_order_items",
                {"order_id": resolved_id},
            )
        ]
        specialist_keys = ["item"]
        if case.get("investigation_scope", {}).get("include_product_context"):
            specialist_calls.append(
                (
                    "order-product-agent",
                    "get_product_context",
                    {"order_id": resolved_id},
                )
            )
            specialist_keys.append("product")
        specialist_calls.extend(
            [
                (
                    "shipment-agent",
                    "get_shipment_summary",
                    {"order_id": resolved_id},
                ),
                (
                    "payment-agent",
                    "get_order_payments",
                    {"order_id": resolved_id},
                ),
                (
                    "payment-agent",
                    "get_payment_timeline",
                    {"order_id": resolved_id},
                ),
                (
                    "payment-agent",
                    "get_refund_timeline",
                    {"order_id": resolved_id},
                ),
            ]
        )
        specialist_keys.extend(["shipment", "payments", "payment_timeline", "refund_timeline"])
        specialist_results = await fetch_group(specialist_calls)
        specialist_evidence = dict(zip(specialist_keys, specialist_results, strict=True))
        item_evidence = specialist_evidence.get("item")
        shipment_evidence = specialist_evidence.get("shipment")
        payment_evidence = specialist_evidence.get("payments")
        payment_timeline = specialist_evidence.get("payment_timeline")
        refund_timeline = specialist_evidence.get("refund_timeline")

    trace.emit(
        case_id=case_id,
        event_type="handoff",
        actor="investigation-coordinator",
        target="policy-agent",
        decision_code="adjudicate_claims",
        evidence_refs=list(evidence),
    )

    item_records = _records(item_evidence.get("data")) if item_evidence else []
    payment_records = _records(payment_evidence.get("data")) if payment_evidence else []
    shipment_records = _records(shipment_evidence.get("data")) if shipment_evidence else []
    payment_events = _timeline_events(payment_timeline.get("data")) if payment_timeline else []
    refund_events = _timeline_events(refund_timeline.get("data")) if refund_timeline else []
    shipment_data = shipment_evidence.get("data") if shipment_evidence else {}
    data_conflicts: list[dict[str, Any]] = []

    def record_conflict(
        field: str,
        sources: list[str],
        selected_source: str | None,
        resolution_code: str,
    ) -> None:
        if len(data_conflicts) < 5:
            data_conflicts.append(
                {
                    "field": field,
                    "sources": sources,
                    "selected_source": selected_source,
                    "resolution_code": resolution_code,
                }
            )

    if rejected and history_evidence:
        record_conflict(
            "customer_unique_id",
            ["get_order", "get_customer_history"],
            None,
            "customer_history_join_failed",
        )

    order_status = str(_value(order, ("order_status", "status")) or "").casefold()
    reported_captured = _amount(
        payment_records,
        ("captured_total_brl", "captured_amount_brl", "payment_value", "amount_captured"),
    )
    capture_records = [
        record
        for record in payment_events
        if any(
            marker in str(_value(record, ("event_type", "status")) or "").casefold()
            for marker in ("capture", "captured")
        )
        and not any(
            token in str(_value(record, ("event_type", "status")) or "").casefold()
            for token in ("fail", "revers", "void", "cancel")
        )
    ]
    timeline_captured = _amount(
        capture_records,
        (
            "captured_total_brl",
            "captured_amount_brl",
            "amount_captured",
            "amount_brl",
            "amount",
        ),
    )
    if (
        reported_captured is not None
        and timeline_captured is not None
        and abs(reported_captured - timeline_captured) > Decimal("0.01")
    ):
        record_conflict(
            "captured_total_brl",
            ["get_order_payments", "get_payment_timeline"],
            "get_payment_timeline",
            "capture_event_ledger_precedence",
        )
    captured = timeline_captured if timeline_captured is not None else reported_captured
    refunded = _amount(
        refund_events,
        ("refunded_total_brl", "refund_amount_brl", "refund_value", "amount_refunded"),
    )
    if refunded is None and refund_timeline is not None:
        refunded = Decimal("0")
    item_total = Decimal("0")
    has_item_total = False
    for record in item_records:
        price = _money(_value(record, ("price", "item_price_brl")))
        freight = _money(_value(record, ("freight_value", "freight_brl")))
        if price is not None:
            item_total += price + (freight or Decimal("0"))
            has_item_total = True
    expected_total = item_total if has_item_total else None

    refund_states = {
        str(_value(record, ("status", "event_type", "refund_status")) or "").casefold()
        for record in refund_events
    }
    capture_ids = _ids(capture_records, ("capture_id", "transaction_id", "payment_id"))
    duplicate_capture = (
        len(capture_ids) > 1
        and captured is not None
        and expected_total is not None
        and captured > expected_total + Decimal("0.01")
    )
    refund_pending = any("pending" in value or "process" in value for value in refund_states)
    refund_failed = any("fail" in value or "reject" in value for value in refund_states)
    refund_complete = any(
        any(token in value for token in ("complete", "success", "refunded"))
        for value in refund_states
    )

    order_shipped_at = _time(
        _value(order, ("order_delivered_carrier_date", "carrier_handoff_at", "seller_handoff_at"))
    )
    shipment_shipped_at = _time(
        _value(
            shipment_data,
            (
                "seller_handoff_at",
                "carrier_handoff_at",
                "delivered_carrier_at",
                "order_delivered_carrier_date",
            ),
        )
    )
    order_delivered_at = _time(_value(order, ("order_delivered_customer_date", "delivered_at")))
    shipment_delivered_at = _time(
        _value(
            shipment_data,
            ("delivered_at", "delivered_customer_at", "order_delivered_customer_date"),
        )
    )
    order_estimated_at = _time(
        _value(order, ("order_estimated_delivery_date", "estimated_delivery_at"))
    )
    shipment_estimated_at = _time(
        _value(shipment_data, ("estimated_delivery_at", "order_estimated_delivery_date"))
    )
    for field, order_value, shipment_value in (
        ("carrier_handoff_at", order_shipped_at, shipment_shipped_at),
        ("delivered_at", order_delivered_at, shipment_delivered_at),
        ("estimated_delivery_at", order_estimated_at, shipment_estimated_at),
    ):
        if order_value and shipment_value and order_value != shipment_value:
            record_conflict(
                field,
                ["get_order", "get_shipment_summary"],
                "get_shipment_summary",
                "shipment_timeline_precedence",
            )
    shipped_at = shipment_shipped_at or order_shipped_at
    delivered_at = shipment_delivered_at or order_delivered_at
    estimated_at = shipment_estimated_at or order_estimated_at
    late_seller_ids: list[str] = []
    has_seller_deadline = False
    if shipped_at:
        for record in item_records:
            deadline = _time(_value(record, ("shipping_limit_date", "seller_handoff_deadline")))
            seller_id = _value(record, ("seller_id",))
            if deadline is None:
                continue
            has_seller_deadline = True
            if (
                shipped_at > deadline
                and isinstance(seller_id, (str, int))
                and str(seller_id) not in late_seller_ids
            ):
                late_seller_ids.append(str(seller_id))
    shipment_text = " ".join(
        str(value).casefold()
        for value in _all_values(shipment_data, ("status", "event_type", "description"))
    )
    if any(token in shipment_text for token in ("lost", "lost_in_transit")):
        shipment_verdict = "lost"
    elif "return" in shipment_text:
        shipment_verdict = "returned"
    elif delivered_at and estimated_at and delivered_at > estimated_at:
        if not shipped_at or not has_seller_deadline:
            shipment_verdict = "insufficient_evidence"
        else:
            shipment_verdict = "seller_delay" if late_seller_ids else "logistics_delay"
    elif delivered_at and estimated_at:
        shipment_verdict = "on_time"
    else:
        shipment_verdict = "insufficient_evidence"

    if refund_failed:
        payment_verdict = "refund_failed"
    elif refund_pending:
        payment_verdict = "refund_pending"
    elif refund_complete:
        payment_verdict = "refunded"
    elif duplicate_capture:
        payment_verdict = "duplicate_capture"
    elif (
        captured is not None
        and expected_total is not None
        and abs(captured - expected_total) > Decimal("0.01")
    ):
        payment_verdict = "capture_mismatch"
    elif captured is not None:
        payment_verdict = "reconciled"
    else:
        payment_verdict = "insufficient_evidence"

    confirmed: list[str] = []
    if (
        ("canceled" in order_status or "cancelled" in order_status)
        and refund_timeline is not None
        and captured is not None
        and captured > (refunded or Decimal("0"))
    ):
        confirmed.append("canceled_order_paid")
    if (
        "unavailable" in order_status
        and refund_timeline is not None
        and captured is not None
        and captured > (refunded or Decimal("0"))
    ):
        confirmed.append("unavailable_order_paid")
    if shipment_verdict in {"seller_delay", "logistics_delay"}:
        confirmed.append("late_delivery_" + shipment_verdict.removesuffix("_delay"))
    if payment_verdict == "refund_pending":
        confirmed.append("refund_pending")
    if payment_verdict == "refund_failed":
        confirmed.append("refund_failed")
    if payment_verdict == "duplicate_capture":
        confirmed.append("duplicate_charge")
    if payment_verdict == "capture_mismatch":
        confirmed.append("payment_mismatch")
    if payment_verdict == "reconciled" and len(payment_records) > 1 and expected_total is not None:
        confirmed.append("valid_split_payment")

    claim_topics = [str(claim.get("topic", "")) for claim in claims]
    candidate_topics = [topic for topic in claim_topics if topic in ISSUES]
    if (
        "unsupported_claim" in candidate_topics
        and resolution_status == "resolved"
        and shipment_verdict == "on_time"
        and payment_verdict in {"reconciled", "refunded"}
    ):
        confirmed.append("unsupported_claim")
    primary = next((topic for topic in candidate_topics if topic in confirmed), None)
    if primary is None and resolution_status != "resolved":
        primary = "insufficient_evidence"
    elif primary is None and payment_verdict == "capture_mismatch":
        primary = "payment_mismatch"
    elif primary is None and shipment_verdict in {"seller_delay", "logistics_delay"}:
        primary = "late_delivery_" + shipment_verdict.removesuffix("_delay")
    elif primary is None and payment_verdict in {
        "refund_pending",
        "refund_failed",
        "duplicate_capture",
    }:
        primary = {
            "duplicate_capture": "duplicate_charge",
            "refund_pending": "refund_pending",
            "refund_failed": "refund_failed",
        }[payment_verdict]
    if primary is None:
        primary = "insufficient_evidence"

    rule = rules.get(primary, {}) if isinstance(rules, dict) else {}
    issue_refs = list(dict.fromkeys(evidence))
    status = (
        rule.get("case_status")
        if rule.get("case_status") in {"action_required", "no_action", "needs_investigation"}
        else "needs_investigation"
    )
    if resolution_status != "resolved" or transport_broken:
        status = "needs_investigation"
        primary = "insufficient_evidence"
        rule = rules.get(primary, {}) if isinstance(rules, dict) else {}

    raw_refund = _money(rule.get("refund_brl"))
    recommended = raw_refund if raw_refund is not None else Decimal("0")
    if refund_timeline is None:
        recommended = Decimal("0")
    if captured is not None and refunded is not None:
        recommended = min(recommended, max(Decimal("0"), captured - refunded))
    responsible = rule.get("responsible_parties", []) if isinstance(rule, dict) else []
    responsible = [
        {"party_type": party.get("party_type"), "party_id": party.get("party_id")}
        for party in responsible
        if isinstance(party, dict)
        and party.get("party_type")
        in {"seller", "platform", "logistics_provider", "payment_provider", "customer", "unknown"}
    ]
    if primary == "insufficient_evidence":
        responsible = [{"party_type": "unknown", "party_id": None}]

    actions = [str(rule["recommended_action"])] if rule.get("recommended_action") else []
    if primary == "insufficient_evidence":
        actions = ["investigate_case"]
    claim_assessments: list[dict[str, Any]] = []
    for claim in claims[:5]:
        topic = str(claim.get("topic", ""))
        if topic == "requested_full_refund":
            verdict = (
                "insufficient_evidence"
                if refund_timeline is None or captured is None
                else "supported"
                if captured is not None and recommended >= captured - (refunded or Decimal("0"))
                else (
                    "partially_supported"
                    if recommended > 0
                    else "insufficient_evidence"
                    if payment_verdict == "insufficient_evidence"
                    else "unsupported"
                )
            )
        elif topic == primary or topic in confirmed:
            verdict = "supported"
        elif (
            resolution_status != "resolved"
            or transport_broken
            or (
                topic.startswith("late_delivery_")
                and shipment_verdict in {"insufficient_evidence", "conflicting"}
            )
            or (
                topic in {"payment_mismatch", "duplicate_charge", "valid_split_payment"}
                and payment_verdict == "insufficient_evidence"
            )
            or (
                topic in {"canceled_order_paid", "unavailable_order_paid"}
                and (not order_status or captured is None or refund_timeline is None)
            )
        ):
            verdict = "insufficient_evidence"
        else:
            verdict = "unsupported"
        claim_assessments.append(
            {
                "claim_id": str(claim.get("claim_id", "unknown")),
                "verdict": verdict,
                "confidence": 0.78
                if verdict == "supported"
                else (0.55 if verdict == "partially_supported" else 0.38),
                "evidence_refs": issue_refs[:10],
            }
        )

    item_ids = _ids(item_records, ("order_item_id", "item_id"))
    seller_ids = _ids(item_records, ("seller_id",))
    payment_ids = _ids(payment_records, ("payment_id", "payment_reference", "transaction_id"))
    shipment_ids = _ids(shipment_records, ("shipment_id",))
    related_ids = _ids(history_records, ("order_id",))
    customer_id = _value(order, ("customer_unique_id",))
    confidence = (
        0.82
        if resolution_status == "resolved" and not transport_broken and policy_evidence
        else 0.35
    )
    if len(rejected) or shipment_verdict == "conflicting":
        confidence = min(confidence, 0.62)

    trace.emit(
        case_id=case_id,
        event_type="policy_decided",
        actor="policy-agent",
        decision_code=primary,
        evidence_refs=[policy_evidence["evidence_ref"]] if policy_evidence else [],
    )
    trace.emit(
        case_id=case_id,
        event_type="handoff",
        actor="policy-agent",
        target="verifier-agent",
        decision_code="verify_output_invariants",
        evidence_refs=issue_refs,
    )

    output = {
        "schema_version": OUTPUT_SCHEMA_VERSION,
        "case_id": case_id,
        "assessment": {
            "primary_issue": primary,
            "secondary_issues": [topic for topic in confirmed if topic != primary][:10],
            "case_status": status,
            "confidence": confidence,
        },
        "affected_entities": {
            "order_ids": [resolved_id] if resolved_id else [],
            "item_ids": item_ids,
            "seller_ids": seller_ids,
            "payment_references": payment_ids,
            "shipment_ids": shipment_ids,
        },
        "claim_assessments": claim_assessments,
        "entity_resolution": {
            "status": resolution_status,
            "resolved_order_ids": [resolved_id] if resolved_id else [],
            "rejected_candidates": rejected,
            "confidence": entity_confidence,
        },
        "customer_context": {
            "customer_unique_id": str(customer_id)
            if customer_id is not None
            else (str(hint) if history_evidence else None),
            "related_order_ids": related_ids[:20],
        },
        "shipment_analysis": {
            "verdict": shipment_verdict,
            "late_seller_ids": late_seller_ids if shipment_verdict == "seller_delay" else [],
            "timeline_complete": bool(shipped_at and delivered_at and estimated_at),
        },
        "payment_analysis": {
            "verdict": payment_verdict,
            "captured_total_brl": _decimal_json(captured),
            "refunded_total_brl": _decimal_json(refunded),
            "refundable_total_brl": _decimal_json(recommended),
        },
        "root_cause_analysis": {
            "ranked_causes": [{"cause_code": primary.upper(), "rank": 1}],
            "responsible_parties": responsible,
        },
        "evidence_refs": issue_refs[:30],
        "data_conflicts": data_conflicts,
        "financial_resolution": {
            "currency": "BRL",
            "recommended_refund_brl": _decimal_json(recommended) or 0.0,
            "refund_lines": (
                [
                    {
                        "reason_code": str(rule.get("recommended_action", primary)),
                        "amount_brl": _decimal_json(recommended),
                        "entity_id": resolved_id,
                    }
                ]
                if recommended > 0
                else []
            ),
        },
        "resolution_actions": list(dict.fromkeys(actions))[:8],
    }

    # Keep unavailable tool outcomes visible without fabricating evidence or case facts.
    trace.emit(
        case_id=case_id,
        event_type="verification_completed",
        actor="verifier-agent",
        decision_code="passed_schema_ready" if not transport_broken else "incomplete_evidence",
        evidence_refs=issue_refs,
        attributes={"resolution": resolution_status, "unavailable_tools": len(errors)},
    )
    return output
