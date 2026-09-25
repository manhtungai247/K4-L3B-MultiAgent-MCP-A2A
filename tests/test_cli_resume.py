from __future__ import annotations

from pathlib import Path

from student_agent.cases import CaseSet
from student_agent.cli import _prepare_resume
from student_agent.contracts import Contracts
from student_agent.trace import TraceWriter


def test_resume_discards_unfinished_case_output_and_trace(tmp_path: Path) -> None:
    repo = Path(__file__).resolve().parents[1]
    (tmp_path / "contracts" / "scoring").mkdir(parents=True)
    (tmp_path / "contracts" / "scoring" / "scoring-policy-v2.json").write_bytes(
        (repo / "contracts" / "scoring" / "scoring-policy-v2.json").read_bytes()
    )
    (tmp_path / "outputs").mkdir()
    trace = TraceWriter(
        tmp_path / "traces" / "trace.jsonl", Contracts(repo / "contracts" / "schemas")
    )
    trace.emit(case_id="CASE_001", event_type="case_received", actor="coordinator")
    (tmp_path / "outputs" / "CASE_001.json").write_text("{}", encoding="utf-8")
    case_set = CaseSet(
        "test-v1",
        "l3b",
        ("CASE_001",),
        {"CASE_001": {"investigation_scope": {}}},
    )

    completed = _prepare_resume(tmp_path, case_set, Contracts(repo / "contracts" / "schemas"))

    assert completed == set()
    assert not (tmp_path / "outputs" / "CASE_001.json").exists()
    assert (tmp_path / "traces" / "trace.jsonl").read_text(encoding="utf-8") == ""
