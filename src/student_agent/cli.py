from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

from .cases import CaseSet, load_case_set
from .config import Settings
from .contracts import Contracts
from .mcp_gateway import connect_gateway
from .submission import package_output_only, package_submission, validate_artifacts
from .trace import TraceWriter
from .workflow import solve_case


def _root(value: str) -> Path:
    return Path(value).resolve()


async def _show_tools(root: Path) -> None:
    settings = Settings.load(root)
    contracts = Contracts(root / "contracts" / "schemas")
    async with connect_gateway(settings.mcp_endpoint, settings.team_api_key, contracts) as gateway:
        for tool in await gateway.list_tools():
            print(tool)


def _prepare_resume(root: Path, case_set: CaseSet, contracts: Contracts) -> set[str]:
    trace_path = root / "traces" / "trace.jsonl"
    expected = set(case_set.case_ids)
    events_by_case: dict[str, list[tuple[dict, str]]] = {case_id: [] for case_id in expected}
    if trace_path.exists():
        for number, line in enumerate(trace_path.read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"traces/trace.jsonl:{number}: invalid JSON") from exc
            contracts.validate_trace(event, f"traces/trace.jsonl:{number}")
            if event["case_id"] not in expected:
                raise ValueError(f"traces/trace.jsonl:{number}: case is outside this case-set")
            events_by_case[event["case_id"]].append((event, line))

    completed: set[str] = set()
    for case_id in case_set.case_ids:
        output_path = root / "outputs" / f"{case_id}.json"
        events = events_by_case[case_id]
        if not output_path.is_file() or not any(
            event["event_type"] == "case_finalized" for event, _ in events
        ):
            continue
        try:
            output = json.loads(output_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        try:
            contracts.validate_output(output, f"outputs/{case_id}.json")
        except ValueError:
            continue
        if output.get("case_id") == case_id:
            completed.add(case_id)

    for path in (root / "outputs").glob("*.json"):
        if path.stem not in completed:
            path.unlink()
    kept_lines = [
        line
        for case_id in case_set.case_ids
        if case_id in completed
        for event, line in events_by_case[case_id]
    ]
    temporary_trace = trace_path.with_suffix(".jsonl.tmp")
    temporary_trace.write_text(
        "\n".join(kept_lines) + ("\n" if kept_lines else ""), encoding="utf-8"
    )
    temporary_trace.replace(trace_path)

    subset = CaseSet(
        case_set.version,
        case_set.variant_id,
        tuple(case_id for case_id in case_set.case_ids if case_id in completed),
        {case_id: case_set.cases[case_id] for case_id in completed},
    )
    validate_artifacts(root, subset, contracts)
    return completed


async def _run(root: Path, *, resume: bool = False) -> None:
    settings = Settings.load(root)
    case_set = load_case_set(root)
    contracts = Contracts(root / "contracts" / "schemas")
    output_root = root / "outputs"
    trace_path = root / "traces" / "trace.jsonl"
    output_root.mkdir(parents=True, exist_ok=True)
    trace_path.parent.mkdir(parents=True, exist_ok=True)
    completed_cases = _prepare_resume(root, case_set, contracts) if resume else set()
    if not resume:
        for stale in output_root.glob("*.json"):
            stale.unlink()
        trace_path.unlink(missing_ok=True)
    trace = TraceWriter(trace_path, contracts)

    required_tools = {
        "get_order",
        "get_order_items",
        "get_order_payments",
        "get_shipment_summary",
        "get_policy",
        "get_customer_history",
        "get_product_context",
        "get_payment_timeline",
        "get_refund_timeline",
    }
    if len(completed_cases) == len(case_set.case_ids):
        _, trace_events = validate_artifacts(root, case_set, contracts)
        print(
            f"OK: already complete: {len(completed_cases)} outputs / "
            f"{len(trace_events)} trace events"
        )
        return

    close_warning = False
    try:
        async with connect_gateway(
            settings.mcp_endpoint, settings.team_api_key, contracts
        ) as gateway:
            discovered_tools = await gateway.list_tools()
            missing_tools = required_tools - set(discovered_tools)
            if missing_tools:
                raise RuntimeError(
                    f"MCP Gateway is missing required tools: {sorted(missing_tools)}"
                )

            for index, case_id in enumerate(case_set.case_ids, 1):
                if case_id in completed_cases:
                    continue
                case = case_set.cases[case_id]
                trace.emit(case_id=case_id, event_type="case_received", actor="coordinator")
                output = await solve_case(case, gateway, trace)
                if case_id in gateway.transport_errors:
                    raise RuntimeError(f"MCP transport failed while investigating {case_id}")
                contracts.validate_output(output, f"outputs/{case_id}.json")
                if output.get("case_id") != case_id:
                    raise ValueError(f"solver returned a mismatched case_id for {case_id}")
                target = output_root / f"{case_id}.json"
                temporary = target.with_suffix(".json.tmp")
                temporary.write_text(
                    json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
                )
                temporary.replace(target)
                trace.emit(case_id=case_id, event_type="case_finalized", actor="coordinator")
                completed_cases.add(case_id)
                print(f"[{index}/{len(case_set.case_ids)}] completed {case_id}")
    except ExceptionGroup as exc:
        if len(completed_cases) != len(case_set.case_ids):
            leaf: BaseException = exc
            while isinstance(leaf, BaseExceptionGroup) and leaf.exceptions:
                leaf = leaf.exceptions[0]
            raise RuntimeError(
                f"MCP stream failed after {len(completed_cases)}/{len(case_set.case_ids)} cases "
                f"({type(leaf).__name__})"
            ) from None
        close_warning = True

    if close_warning:
        print("WARN: MCP stream closed with a warning after all outputs were saved")
    _, trace_events = validate_artifacts(root, case_set, contracts)
    print(f"OK: validated {len(case_set.case_ids)} outputs / {len(trace_events)} trace events")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Day09 L3B student workflow")
    result.add_argument("--root", default=".", help="repository root (default: current directory)")
    commands = result.add_subparsers(dest="command", required=True)
    commands.add_parser("validate-inputs", help="validate case-set.json and all 100 inputs")
    commands.add_parser("mcp-tools", help="authenticate and list discovered MCP tools")
    run = commands.add_parser("run", help="run the implemented workflow for all cases")
    run.add_argument(
        "--resume",
        action="store_true",
        help="keep validated outputs and trace events, then continue unfinished cases",
    )
    commands.add_parser("validate", help="validate outputs and observable trace")
    package = commands.add_parser("package", help="validate and build the submission ZIP")
    package.add_argument("--output", default="dist/submission.zip")
    package.add_argument(
        "--output-only",
        action="store_true",
        help="build the Lab Coach confirmed ZIP containing only output/<case_id>.json",
    )
    return result


def main() -> None:
    args = parser().parse_args()
    root = _root(args.root)
    try:
        if args.command == "validate-inputs":
            case_set = load_case_set(root)
            print(
                f"OK: {case_set.variant_id} / {case_set.version} / {len(case_set.case_ids)} cases"
            )
        elif args.command == "mcp-tools":
            asyncio.run(_show_tools(root))
        elif args.command == "run":
            asyncio.run(_run(root, resume=args.resume))
        elif args.command == "validate":
            case_set = load_case_set(root)
            contracts = Contracts(root / "contracts" / "schemas")
            _, trace = validate_artifacts(root, case_set, contracts)
            print(f"OK: {len(case_set.case_ids)} outputs / {len(trace)} trace events")
        elif args.command == "package":
            builder = package_output_only if args.output_only else package_submission
            destination = builder(root, root / args.output)
            print(f"OK: {destination}")
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    except ExceptionGroup as exc:
        leaf: BaseException = exc
        while isinstance(leaf, BaseExceptionGroup) and leaf.exceptions:
            leaf = leaf.exceptions[0]
        print(f"ERROR: MCP session failed ({type(leaf).__name__})", file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
