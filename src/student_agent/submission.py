from __future__ import annotations

import json
import re
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from . import OUTPUT_SCHEMA_VERSION, VARIANT_ID
from .cases import CaseSet, load_case_set
from .contracts import Contracts

SECRET_PATTERN = re.compile(r"sk-team-[A-Za-z0-9_-]{8,}")
MAX_FILE_BYTES = 1024 * 1024
MAX_SUBMISSION_BYTES = 12 * 1024 * 1024


def _json_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{path}: invalid UTF-8 JSON") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{path}: expected a JSON object")
    return value


def build_manifest(case_set: CaseSet) -> dict[str, Any]:
    return {
        "schema_version": "day09-submission-manifest-v2",
        "competition_id": "day09-multiagent-mcp-a2a",
        "variant_id": VARIANT_ID,
        "case_set_version": case_set.version,
        "output_schema_version": OUTPUT_SCHEMA_VERSION,
        "trace_schema_version": "day09-trace-event-v1",
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "client": {"name": "day09-student-starter", "version": "0.1.0"},
    }


def validate_artifacts(
    root: Path, case_set: CaseSet, contracts: Contracts
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    outputs_root = root / "outputs"
    actual = {path.stem: path for path in outputs_root.glob("*.json") if path.is_file()}
    expected = set(case_set.case_ids)
    if set(actual) != expected:
        missing = sorted(expected - set(actual))
        extra = sorted(set(actual) - expected)
        raise ValueError(f"outputs do not match case-set; missing={missing}, extra={extra}")

    outputs: dict[str, dict[str, Any]] = {}
    for case_id in case_set.case_ids:
        output = _json_object(actual[case_id])
        contracts.validate_output(output, f"outputs/{case_id}.json")
        if output.get("case_id") != case_id:
            raise ValueError(f"outputs/{case_id}.json has a mismatched case_id")
        outputs[case_id] = output

    trace_path = root / "traces" / "trace.jsonl"
    try:
        trace_lines = trace_path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError) as exc:
        raise ValueError("traces/trace.jsonl is missing or not UTF-8") from exc
    normalized_lines: list[str] = []
    seen_events: set[str] = set()
    events_by_case: dict[str, list[dict[str, Any]]] = {case_id: [] for case_id in case_set.case_ids}
    for number, line in enumerate(trace_lines, 1):
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"traces/trace.jsonl:{number}: invalid JSON") from exc
        contracts.validate_trace(event, f"traces/trace.jsonl:{number}")
        if event["case_id"] not in expected:
            raise ValueError(f"traces/trace.jsonl:{number}: case is outside this case-set")
        if event["event_id"] in seen_events:
            raise ValueError(f"traces/trace.jsonl:{number}: duplicate event_id")
        seen_events.add(event["event_id"])
        events_by_case[event["case_id"]].append(event)
        normalized_lines.append(json.dumps(event, ensure_ascii=False, separators=(",", ":")))

    scoring = json.loads(
        (root / "contracts" / "scoring" / "scoring-policy-v2.json").read_text(encoding="utf-8")
    )
    required_lifecycle = scoring["workflow_required_events"]
    for case_id, output in outputs.items():
        case_events = events_by_case[case_id]
        event_types = [event["event_type"] for event in case_events]
        missing_events = set(required_lifecycle) - set(event_types)
        if missing_events:
            raise ValueError(f"case {case_id} is missing workflow events: {sorted(missing_events)}")
        positions = [event_types.index(event_type) for event_type in required_lifecycle]
        if positions != sorted(positions):
            raise ValueError(f"case {case_id} has an invalid workflow event order")

        consumed_refs = {
            evidence_ref
            for event in case_events
            if event["event_type"] == "tool_result_consumed"
            for evidence_ref in event.get("evidence_refs", [])
        }
        output_refs = set(output.get("evidence_refs", []))
        if not output_refs or not output_refs <= consumed_refs:
            raise ValueError(f"case {case_id} has missing or untraced output evidence")

        consumed_domains = {
            (event.get("attributes") or {}).get("domain")
            for event in case_events
            if event["event_type"] == "tool_result_consumed"
        }
        scope = case_set.cases[case_id].get("investigation_scope", {})
        required_domains = {"policy"}
        entity_resolution = output.get("entity_resolution", {})
        resolved = entity_resolution.get("status") == "resolved"
        if resolved:
            required_domains.update({"order", "item", "payment", "shipment"})
        if scope.get("include_customer_history"):
            required_domains.add("customer")
        if resolved and scope.get("include_product_context"):
            required_domains.add("product")
        payment_analysis = output.get("payment_analysis", {})
        if (
            payment_analysis.get("verdict")
            in {
                "refund_pending",
                "refund_failed",
                "refunded",
            }
            or payment_analysis.get("refunded_total_brl") is not None
        ):
            required_domains.add("refund")
        if not required_domains <= consumed_domains:
            missing_domains = sorted(required_domains - consumed_domains)
            raise ValueError(
                f"case {case_id} is missing required evidence domains: {missing_domains}"
            )

    serialized = [json.dumps(value, ensure_ascii=False) for value in outputs.values()]
    if SECRET_PATTERN.search("\n".join([*serialized, *normalized_lines])):
        raise ValueError("a Team API Key appears in output or trace")
    return outputs, normalized_lines


def package_submission(root: Path, destination: Path) -> Path:
    root = root.resolve()
    case_set = load_case_set(root)
    contracts = Contracts(root / "contracts" / "schemas")
    outputs, trace_lines = validate_artifacts(root, case_set, contracts)
    manifest = build_manifest(case_set)
    contracts.validate_manifest(manifest)

    payloads = {
        "manifest.json": json.dumps(manifest, separators=(",", ":")).encode(),
        "trace.jsonl": ("\n".join(trace_lines) + ("\n" if trace_lines else "")).encode(),
        **{
            f"outputs/{case_id}.json": json.dumps(
                outputs[case_id], ensure_ascii=False, separators=(",", ":")
            ).encode()
            for case_id in case_set.case_ids
        },
    }
    oversized = [name for name, payload in payloads.items() if len(payload) > MAX_FILE_BYTES]
    if oversized:
        raise ValueError(f"submission files exceed 1 MB: {oversized}")
    if sum(map(len, payloads.values())) > MAX_SUBMISSION_BYTES:
        raise ValueError("submission exceeds the 12 MB uncompressed limit")

    destination = destination.resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, payload in payloads.items():
            archive.writestr(name, payload)
    return destination


def package_output_only(root: Path, destination: Path) -> Path:
    """Build the coach-confirmed legacy upload: one output/ folder, nothing else."""
    root = root.resolve()
    case_set = load_case_set(root)
    contracts = Contracts(root / "contracts" / "schemas")
    validate_artifacts(root, case_set, contracts)
    return _write_output_only_zip(root / "outputs", case_set.case_ids, destination, contracts)


def _write_output_only_zip(
    outputs_root: Path,
    case_ids: tuple[str, ...],
    destination: Path,
    contracts: Contracts,
) -> Path:
    actual = {path.stem: path for path in outputs_root.glob("*.json") if path.is_file()}
    expected = set(case_ids)
    if set(actual) != expected:
        missing = sorted(expected - set(actual))
        extra = sorted(set(actual) - expected)
        raise ValueError(f"outputs do not match case-set; missing={missing}, extra={extra}")
    payloads: dict[str, bytes] = {}
    for case_id in case_ids:
        output = _json_object(actual[case_id])
        contracts.validate_output(output, f"outputs/{case_id}.json")
        if output.get("case_id") != case_id:
            raise ValueError(f"outputs/{case_id}.json has a mismatched case_id")
        payload = json.dumps(output, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if len(payload) > MAX_FILE_BYTES:
            raise ValueError(f"outputs/{case_id}.json exceeds 1 MB")
        if SECRET_PATTERN.search(payload.decode("utf-8")):
            raise ValueError(f"outputs/{case_id}.json contains a Team API Key")
        payloads[f"output/{case_id}.json"] = payload

    if sum(map(len, payloads.values())) > MAX_SUBMISSION_BYTES:
        raise ValueError("output-only submission exceeds the 12 MB uncompressed limit")
    destination = destination.resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, payload in payloads.items():
            archive.writestr(name, payload)
    return destination
