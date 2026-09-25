from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

from student_agent.cases import CaseSet
from student_agent.contracts import Contracts
from student_agent.mcp_gateway import MCPToolError
from student_agent.submission import validate_artifacts
from student_agent.trace import TraceWriter
from student_agent.workflow import solve_case


class FakeGateway:
    def __init__(self) -> None:
        self.count = 0

    async def call(self, tool: str, *, case_id: str, **args: str) -> dict[str, Any]:
        if tool == "get_order" and args["order_id"] == "candidate-no-match":
            raise MCPToolError("not found")
        self.count += 1
        data: Any
        domain = {
            "get_policy": "policy",
            "get_order": "order",
            "get_order_items": "item",
            "get_product_context": "product",
            "get_shipment_summary": "shipment",
            "get_order_payments": "payment",
            "get_payment_timeline": "payment",
            "get_refund_timeline": "refund",
            "get_customer_history": "customer",
        }[tool]
        if tool == "get_policy":
            data = {
                "policy_version": "EC_POLICY_V2",
                "currency": "BRL",
                "rules": {
                    "late_delivery_logistics": {
                        "case_status": "action_required",
                        "recommended_action": "refund_freight",
                        "refund_brl": 16,
                        "responsible_parties": [
                            {"party_type": "logistics_provider", "party_id": None}
                        ],
                    }
                },
            }
        elif tool == "get_order":
            data = {
                "order_id": args["order_id"],
                "customer_unique_id": "customer-1",
                "order_status": "delivered",
                "order_delivered_carrier_date": "2018-01-02T10:00:00-03:00",
                "order_delivered_customer_date": "2018-01-06T10:00:00-03:00",
                "order_estimated_delivery_date": "2018-01-05T10:00:00-03:00",
            }
        elif tool == "get_order_items":
            data = [
                {
                    "order_id": args["order_id"],
                    "order_item_id": "1",
                    "product_id": "p1",
                    "seller_id": "s1",
                    "shipping_limit_date": "2018-01-02T12:00:00-03:00",
                    "price": "100.00",
                    "freight_value": "16.00",
                }
            ]
        elif tool == "get_product_context":
            data = {"products": [{"product_id": "p1", "category": "books"}]}
        elif tool == "get_shipment_summary":
            data = {"shipment_id": "shipment-1", "status": "delivered"}
        elif tool == "get_order_payments":
            data = [{"payment_id": "pay-1", "payment_value": "116.00"}]
        elif tool == "get_payment_timeline":
            data = [{"event_type": "capture", "transaction_id": "capture-1", "amount": "116.00"}]
        elif tool == "get_refund_timeline":
            data = []
        else:
            data = [{"order_id": "other-order", "customer_unique_id": "customer-1"}]
        return {
            "schema_version": "day09-mcp-evidence-v1",
            "evidence_ref": "ev_" + str(self.count).zfill(24),
            "result_hash": "sha256:" + "a" * 64,
            "domain": domain,
            "data": data,
        }


def test_solver_resolves_case_emits_realistic_handoffs_and_valid_schema(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    contracts = Contracts(root / "contracts" / "schemas")
    scoring_dir = tmp_path / "contracts" / "scoring"
    scoring_dir.mkdir(parents=True)
    (scoring_dir / "scoring-policy-v2.json").write_bytes(
        (root / "contracts" / "scoring" / "scoring-policy-v2.json").read_bytes()
    )
    trace = TraceWriter(tmp_path / "traces" / "trace.jsonl", contracts)
    trace.emit(case_id="CASE_001", event_type="case_received", actor="coordinator")
    gateway = FakeGateway()
    case = {
        "case_id": "CASE_001",
        "policy_version": "EC_POLICY_V2",
        "candidate_order_ids": ["order-1", "candidate-no-match"],
        "customer_unique_id_hint": "customer-1",
        "customer_request": {
            "claimed_order_id": "order-1",
            "claims": [
                {"claim_id": "claim-a", "topic": "late_delivery_logistics"},
                {"claim_id": "claim-b", "topic": "requested_full_refund"},
            ],
        },
        "investigation_scope": {
            "include_customer_history": True,
            "include_product_context": True,
            "require_independent_verification": True,
        },
    }

    output = asyncio.run(solve_case(case, gateway, trace))
    contracts.validate_output(output, "test output")
    (tmp_path / "outputs").mkdir()
    (tmp_path / "outputs" / "CASE_001.json").write_text(json.dumps(output), encoding="utf-8")
    trace.emit(case_id="CASE_001", event_type="case_finalized", actor="coordinator")
    case_set = CaseSet("test-v1", "l3b", ("CASE_001",), {"CASE_001": case})
    validate_artifacts(tmp_path, case_set, contracts)
    assert output["entity_resolution"]["status"] == "resolved"
    assert output["assessment"]["primary_issue"] == "late_delivery_logistics"
    assert output["shipment_analysis"]["verdict"] == "logistics_delay"
    assert output["financial_resolution"]["recommended_refund_brl"] == 16.0
    assert gateway.count == 9
    events = [
        json.loads(line) for line in (tmp_path / "traces" / "trace.jsonl").read_text().splitlines()
    ]
    assert {
        "case_received",
        "task_assigned",
        "handoff",
        "tool_result_consumed",
        "policy_decided",
        "verification_completed",
    } <= {event["event_type"] for event in events}


def test_metadata_declares_no_runtime_model() -> None:
    root = Path(__file__).resolve().parents[1]
    metadata = json.loads((root / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["uses_external_llm"] is False
    assert metadata["model_parameters"] == 0
    assert metadata["model_name"].startswith("none")


def test_entity_resolution_does_not_select_only_candidate_when_customer_mismatches(
    tmp_path: Path,
) -> None:
    class MismatchedCustomerGateway(FakeGateway):
        async def call(self, tool: str, *, case_id: str, **args: str) -> dict[str, Any]:
            result = await super().call(tool, case_id=case_id, **args)
            if tool == "get_order" and args["order_id"] == "order-1":
                result["data"]["customer_unique_id"] = "different-customer"
            return result

    root = Path(__file__).resolve().parents[1]
    contracts = Contracts(root / "contracts" / "schemas")
    trace = TraceWriter(tmp_path / "trace.jsonl", contracts)
    output = asyncio.run(
        solve_case(
            {
                "case_id": "CASE_001",
                "policy_version": "EC_POLICY_V2",
                "candidate_order_ids": ["order-1", "candidate-no-match"],
                "customer_unique_id_hint": "customer-1",
                "customer_request": {"claimed_order_id": "order-1", "claims": []},
                "investigation_scope": {"include_customer_history": True},
            },
            MismatchedCustomerGateway(),
            trace,
        )
    )

    contracts.validate_output(output, "mismatched customer output")
    assert output["entity_resolution"]["status"] == "not_found"
    assert output["entity_resolution"]["resolved_order_ids"] == []
    assert output["entity_resolution"]["rejected_candidates"] == ["order-1"]


def test_shipment_timeline_is_selected_and_conflict_is_reported(tmp_path: Path) -> None:
    class ConflictingShipmentGateway(FakeGateway):
        async def call(self, tool: str, *, case_id: str, **args: str) -> dict[str, Any]:
            result = await super().call(tool, case_id=case_id, **args)
            if tool == "get_shipment_summary":
                result["data"]["delivered_at"] = "2018-01-04T10:00:00-03:00"
            return result

    root = Path(__file__).resolve().parents[1]
    contracts = Contracts(root / "contracts" / "schemas")
    trace = TraceWriter(tmp_path / "trace.jsonl", contracts)
    output = asyncio.run(
        solve_case(
            {
                "case_id": "CASE_001",
                "policy_version": "EC_POLICY_V2",
                "candidate_order_ids": ["order-1", "candidate-no-match"],
                "customer_unique_id_hint": "customer-1",
                "customer_request": {"claimed_order_id": "order-1", "claims": []},
                "investigation_scope": {"include_customer_history": True},
            },
            ConflictingShipmentGateway(),
            trace,
        )
    )

    contracts.validate_output(output, "conflicting shipment output")
    assert output["shipment_analysis"]["verdict"] == "on_time"
    assert output["data_conflicts"] == [
        {
            "field": "delivered_at",
            "sources": ["get_order", "get_shipment_summary"],
            "selected_source": "get_shipment_summary",
            "resolution_code": "shipment_timeline_precedence",
        }
    ]


def test_missing_refund_evidence_does_not_assume_zero_prior_refunds(
    tmp_path: Path,
) -> None:
    class MissingRefundGateway(FakeGateway):
        async def call(self, tool: str, *, case_id: str, **args: str) -> dict[str, Any]:
            if tool == "get_refund_timeline":
                raise MCPToolError("refund timeline unavailable")
            return await super().call(tool, case_id=case_id, **args)

    root = Path(__file__).resolve().parents[1]
    contracts = Contracts(root / "contracts" / "schemas")
    trace = TraceWriter(tmp_path / "trace.jsonl", contracts)
    output = asyncio.run(
        solve_case(
            {
                "case_id": "CASE_001",
                "policy_version": "EC_POLICY_V2",
                "candidate_order_ids": ["order-1", "candidate-no-match"],
                "customer_unique_id_hint": "customer-1",
                "customer_request": {
                    "claimed_order_id": "order-1",
                    "claims": [
                        {"claim_id": "claim-a", "topic": "late_delivery_logistics"},
                        {"claim_id": "claim-b", "topic": "requested_full_refund"},
                    ],
                },
                "investigation_scope": {"include_customer_history": True},
            },
            MissingRefundGateway(),
            trace,
        )
    )

    contracts.validate_output(output, "missing refund evidence output")
    assert output["payment_analysis"]["refunded_total_brl"] is None
    assert output["financial_resolution"]["recommended_refund_brl"] == 0.0
    assert output["claim_assessments"][1]["verdict"] == "insufficient_evidence"


def test_independent_mcp_lookups_run_with_a_bounded_concurrency(tmp_path: Path) -> None:
    class ConcurrentGateway(FakeGateway):
        def __init__(self) -> None:
            super().__init__()
            self.active = 0
            self.peak = 0

        async def call(self, tool: str, *, case_id: str, **args: str) -> dict[str, Any]:
            self.active += 1
            self.peak = max(self.peak, self.active)
            try:
                await asyncio.sleep(0.01)
                return await super().call(tool, case_id=case_id, **args)
            finally:
                self.active -= 1

    root = Path(__file__).resolve().parents[1]
    contracts = Contracts(root / "contracts" / "schemas")
    trace = TraceWriter(tmp_path / "trace.jsonl", contracts)
    gateway = ConcurrentGateway()
    output = asyncio.run(
        solve_case(
            {
                "case_id": "CASE_001",
                "policy_version": "EC_POLICY_V2",
                "candidate_order_ids": ["order-1", "candidate-no-match"],
                "customer_unique_id_hint": "customer-1",
                "customer_request": {"claimed_order_id": "order-1", "claims": []},
                "investigation_scope": {
                    "include_customer_history": True,
                    "include_product_context": True,
                },
            },
            gateway,
            trace,
        )
    )

    contracts.validate_output(output, "concurrent lookup output")
    assert gateway.peak == 4


def test_timeline_prefers_event_ledger_over_payment_summary() -> None:
    from student_agent.workflow import _timeline_events

    capture = {"event_type": "captured", "amount_brl": "89.00"}
    payload = {"payments": [{"payment_value": "16.00"}], "events": [capture]}
    assert _timeline_events(payload) == [capture]
    assert _timeline_events({"payments": [{"payment_value": "16.00"}], "events": []}) == []
    assert _timeline_events([capture]) == [capture]


def test_item_total_deduplicates_identity_and_rejects_conflicting_versions() -> None:
    from decimal import Decimal

    from student_agent.workflow import _item_total

    row = {"order_item_id": "1", "price": "100", "freight_value": "16"}
    assert _item_total([row, dict(row)]) == (Decimal("116"), False)
    assert _item_total([row, {**row, "freight_value": "18"}]) == (None, True)
    assert _item_total([row, {**row, "order_item_id": "2"}]) == (Decimal("232"), False)


def test_event_conflict_and_failed_refund_are_not_silently_discarded(tmp_path: Path) -> None:
    class EventGateway(FakeGateway):
        async def call(self, tool: str, *, case_id: str, **args: str) -> dict[str, Any]:
            result = await super().call(tool, case_id=case_id, **args)
            if tool == "get_shipment_summary":
                result["data"] = {
                    "delivered_customer_at": "2018-01-04T10:00:00-03:00",
                    "events": [
                        {
                            "event_type": "delivered_late",
                            "status": "confirmed",
                            "actor": "logistics_provider",
                        }
                    ],
                }
            if tool == "get_refund_timeline":
                result["data"] = {
                    "events": [
                        {"event_type": "refund_requested", "status": "failed", "amount_brl": "99"},
                        {
                            "event_type": "refund_completed",
                            "status": "completed",
                            "amount_brl": "5",
                        },
                    ]
                }
            return result

    root = Path(__file__).resolve().parents[1]
    contracts = Contracts(root / "contracts" / "schemas")
    output = asyncio.run(
        solve_case(
            {
                "case_id": "CASE_001",
                "policy_version": "EC_POLICY_V2",
                "candidate_order_ids": ["order-1"],
                "customer_unique_id_hint": "customer-1",
                "customer_request": {
                    "claimed_order_id": "order-1",
                    "claims": [{"claim_id": "a", "topic": "late_delivery_logistics"}],
                },
                "investigation_scope": {"include_customer_history": True},
            },
            EventGateway(),
            TraceWriter(tmp_path / "trace.jsonl", contracts),
        )
    )
    contracts.validate_output(output, "event conflict")
    assert output["assessment"]["primary_issue"] == "late_delivery_logistics"
    assert output["shipment_analysis"]["verdict"] == "conflicting"
    assert output["payment_analysis"]["refunded_total_brl"] == 5.0
    assert output["assessment"]["confidence"] < 0.82
    assert any(x["field"] == "delivery_status" for x in output["data_conflicts"])


def test_duplicate_capture_requires_distinct_events_in_same_period() -> None:
    from decimal import Decimal

    from student_agent.workflow import _duplicate_capture

    first = {"event_at": "2018-01-01T10:00:00Z", "amount_brl": "89"}
    later = {"event_at": "2018-02-01T10:00:00Z", "amount_brl": "89"}
    assert not _duplicate_capture([first, later], Decimal("89"))
    assert not _duplicate_capture([first, dict(first)], Decimal("89"))
    assert _duplicate_capture([first, {**later, "event_at": "2018-01-01T11:00:00Z"}], Decimal("89"))
    split = [
        {**first, "amount_brl": "44.50"},
        {**first, "event_at": "2018-01-01T11:00:00Z", "amount_brl": "44.50"},
    ]
    assert not _duplicate_capture(split, Decimal("89"))
