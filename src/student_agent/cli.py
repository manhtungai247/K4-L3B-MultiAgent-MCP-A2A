from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

from .cases import load_case_set
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


async def _run(root: Path) -> None:
    settings = Settings.load(root)
    case_set = load_case_set(root)
    contracts = Contracts(root / "contracts" / "schemas")
    output_root = root / "outputs"
    trace_path = root / "traces" / "trace.jsonl"
    output_root.mkdir(parents=True, exist_ok=True)
    trace_path.parent.mkdir(parents=True, exist_ok=True)
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
    completed = 0
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
                completed += 1
                print(f"[{index}/{len(case_set.case_ids)}] completed {case_id}")
    except ExceptionGroup as exc:
        if completed != len(case_set.case_ids):
            leaf: BaseException = exc
            while isinstance(leaf, BaseExceptionGroup) and leaf.exceptions:
                leaf = leaf.exceptions[0]
            raise RuntimeError(
                f"MCP stream failed after {completed}/{len(case_set.case_ids)} cases "
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
    commands.add_parser("run", help="run the implemented workflow for all cases")
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
            asyncio.run(_run(root))
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
