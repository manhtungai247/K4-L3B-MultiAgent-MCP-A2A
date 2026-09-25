from __future__ import annotations

import json
import zipfile
from pathlib import Path

from student_agent.contracts import Contracts
from student_agent.submission import _write_output_only_zip


def test_output_only_zip_contains_only_case_json_under_output(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    contracts = Contracts(root / "contracts" / "schemas")
    case_id = "CASE_001"
    output = {
        "schema_version": "day09-l3b-output-v2",
        "case_id": case_id,
        "assessment": {
            "primary_issue": "insufficient_evidence",
            "secondary_issues": [],
            "case_status": "needs_investigation",
            "confidence": 0.2,
        },
        "affected_entities": {
            "order_ids": [],
            "item_ids": [],
            "seller_ids": [],
            "payment_references": [],
            "shipment_ids": [],
        },
        "entity_resolution": {
            "status": "not_found",
            "resolved_order_ids": [],
            "rejected_candidates": [],
            "confidence": 0.2,
        },
        "customer_context": {"customer_unique_id": None, "related_order_ids": []},
        "shipment_analysis": {
            "verdict": "insufficient_evidence",
            "late_seller_ids": [],
            "timeline_complete": False,
        },
        "payment_analysis": {
            "verdict": "insufficient_evidence",
            "captured_total_brl": None,
            "refunded_total_brl": None,
            "refundable_total_brl": None,
        },
        "root_cause_analysis": {
            "ranked_causes": [],
            "responsible_parties": [{"party_type": "unknown", "party_id": None}],
        },
        "evidence_refs": [],
        "data_conflicts": [],
        "financial_resolution": {
            "currency": "BRL",
            "recommended_refund_brl": 0,
            "refund_lines": [],
        },
        "resolution_actions": [],
    }
    output_dir = tmp_path / "outputs"
    output_dir.mkdir()
    (output_dir / f"{case_id}.json").write_text(json.dumps(output), encoding="utf-8")
    archive_path = tmp_path / "submission.zip"

    _write_output_only_zip(output_dir, (case_id,), archive_path, contracts)

    with zipfile.ZipFile(archive_path) as archive:
        assert archive.namelist() == [f"output/{case_id}.json"]
        assert json.loads(archive.read(f"output/{case_id}.json"))["case_id"] == case_id
