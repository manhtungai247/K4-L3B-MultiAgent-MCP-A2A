# L3B Architecture Record

## 1. System overview

`solve_case` is a deterministic Python coordinator. It does not run an external
LLM. One authenticated MCP session serves the batch; evidence caches and trace
references remain scoped to each case.

```text
Input → Coordinator → Entity/customer checks → Order/product + shipment + payment
            │               │                           │
            ├── policy ──────┴──── evidence refs ────────┤
            └──────────── Policy adjudication → Verifier → output + trace
```

The coordinator first reads the policy, customer history, and each supplied
candidate order. It resolves one order only when the customer unique ID or
customer-history order list supports it. If the order record omits the join key,
the exact claimed order ID can be used; an explicit customer-ID mismatch is
rejected. A case with no defensible match remains `not_found` or `ambiguous`.

After resolution, the coordinator collects order items, scoped product context,
shipment summary, payment records, payment and refund timelines. The policy role
maps evidence-backed findings to the returned policy action, status, refund
amount, and responsible party. Shipment summaries take precedence over order
date fields when they disagree; the payment capture timeline takes precedence
over the payment summary. The output records each observed source conflict and
which source was selected.

## 2. Agent ownership

The roles below are explicit in trace events. They are coordinated in one
Python process over one shared gateway; they are not separate model instances.

| Actor | Responsibility | MCP tools used | Handoff |
| --- | --- | --- | --- |
| Coordinator | Assign work, collect evidence, maintain case scope | `get_policy` | Entity/customer and specialist roles |
| Entity/customer | Resolve candidates and load customer history | `get_order`, `get_customer_history` | Investigation coordinator |
| Order/product | Load line items and scoped product context | `get_order_items`, `get_product_context` | Investigation coordinator |
| Shipment | Compare carrier handoff, delivery, estimate, and seller deadlines | `get_shipment_summary` | Investigation coordinator |
| Payment/refund | Reconcile captures, split payments, and refund lifecycle | `get_order_payments`, `get_payment_timeline`, `get_refund_timeline` | Policy role |
| Policy | Apply the case's returned policy rule to supported findings | `get_policy` result | Verifier |
| Verifier | Check output schema readiness and evidence-to-trace linkage | No extra calls | Finalize case |

## 3. Entity resolution and message protocol

Every request and trace event carries the input `case_id`. The resolver checks
the returned `customer_unique_id` against the input hint and also checks whether
the candidate appears in the queried customer's history. It only rejects a
candidate when the order response explicitly identifies a different customer.
When the source omits that identity field, the claimed order ID is a lower
confidence fallback. No unrelated customer/order history is searched.

Trace handoffs contain role names, decision codes, and evidence references,
not private reasoning or credentials. A failed candidate lookup does not invent
an evidence record. An unresolved or ambiguous entity produces an
`insufficient_evidence` assessment and a `needs_investigation` status.

## 4. Evidence and conflict lifecycle

The gateway validates each evidence envelope against the public MCP schema,
caches it only for that case and exact tool arguments, and returns a deep copy.
Each successfully consumed result emits `tool_result_consumed` with its original
evidence reference and domain. Output references are drawn from these results;
references are never copied across cases or generated locally.

Conflicts are reported for differing order/shipment timestamps, differing
payment-summary/capture-ledger totals, and a customer/history join failure. The
shipment timeline is selected for shipment dates and the payment timeline for
captured totals. Refund status and amount come from the refund timeline. The
chosen source and resolution code appear in `data_conflicts`.

## 5. Failure and efficiency policy

| Failure | Retry budget | Fallback | Trace / package behavior |
| --- | ---: | --- | --- |
| Candidate order lookup returns a tool error | 0 | Continue with other candidates; do not fabricate evidence | Record other successful results; unresolved entity stays unresolved |
| MCP transport failure | Up to 5 connection retries in HTTP transport | Stop fetching for that case; resume skips only cases with valid output and final trace | Emit `evidence_unavailable`; runner stops rather than submit partial results |
| Refund timeline tool error | 0 | Leave refunded total unknown, propose no refund, and mark the full-refund claim insufficient | Emit `tool_unavailable`; do not treat an error as an empty refund ledger |
| Candidate identity is missing or conflicting | 0 | Use exact claimed ID only when the source has no explicit mismatch | `not_found` / `ambiguous`; package checks required evidence |
| Shipment or payment sources disagree | 0 | Use the timeline source described above | Add a `data_conflicts` entry |
| Required evidence domain is absent | 0 | No synthetic or cached cross-case substitute | Local artifact validation rejects packaging |

The runner discovers tools once and keeps one authenticated session for the
batch to avoid reconnect overhead. On interruption, `day09 run --resume` keeps
only cases whose output and lifecycle/evidence trace validate, drops unfinished
case artifacts, and continues with the rest. Each L3B case uses at most ten calls: policy,
customer history, two candidate
orders, and six scoped order/product/shipment/payment/refund lookups. There are
There are no workflow-level retries; the HTTP transport retries connection
failures up to five times. Independent calls run in groups capped at four
concurrent requests. The gateway cache prevents duplicate identical calls
within one case; all calls remain auditable by the competition service.

## 6. Verification invariants

Before packaging, the validator requires one schema-valid output per input case,
valid trace events, the required lifecycle events in order, unique event IDs,
same-case evidence references consumed in trace, and the required domains for
the resolved entity and investigation scope. A refund domain is required when a
refund result or status is asserted. It also rejects Team API Key patterns in
output or trace. The output-only packager
adds only `output/<case_id>.json` files to the ZIP; source, inputs, `.env`,
metadata, traces, and audit material stay outside that upload archive.

## 7. Reproducibility

The workflow uses Python 3.11+, pinned project dependencies, fixed Decimal
arithmetic for currency comparisons, and no random model sampling. Event IDs and
timestamps are generated at runtime. `metadata.json` declares model name
`none - deterministic Python rule engine`, zero model parameters, and no
external LLM. Run with `day09 validate-inputs`, `day09 mcp-tools`, `day09 run`,
`day09 validate`, and `day09 package --output dist/submission.zip`. The V2
packager writes `manifest.json`, `trace.jsonl`, and
`outputs/<case_id>.json`, after validating all artifacts against the L3B
contracts. Do not use `--output-only` for the current portal: its latest upload
validation reports that the V2 manifest is missing from that legacy package.
L3B inputs, schemas, tools, and adjudication contracts stay on the L3B variant
supplied in this checkout.
