# RustWx Agent Backend Contract

RustWx should expose weather capability facts before an agent launches expensive
fetch, decode, render, or heavy-compute work. `agent_preflight` is the first
small contract for that purpose.

The command emits JSON from `rustwx_products::agent_backend` with:

- catalog-backed product identity, kind, maturity, runners, and aliases
- complete, partial, blocked, and unknown status semantics
- basic, intermediate, and advanced tier hints based on product complexity
- instant, interactive, background, and precompute execution lanes
- callable surfaces such as catalog metadata, point sampling, direct maps,
  derived maps, windowed maps, and heavy bundles
- model-specific support/blocker details when `--model` is supplied
- rough fetch/cache/artifact cost hints that do not require network access

This is intentionally a preflight/orchestration contract, not a dashboard
design. User examples should not become scope. The agent app should ask this
surface what RustWx can do, decide what to show instantly, and queue or
precompute the rest.

Example:

```powershell
cargo run -p rustwx-cli --bin agent_preflight -- --model hrrr --product 2m_temperature_10m_winds --product severe_proof_panel
```

For a whole-surface inventory:

```powershell
cargo run -p rustwx-cli --bin agent_preflight -- --model hrrr --all --out proof/agent_preflight/hrrr.json
```
