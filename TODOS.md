
## Atlas OSS Launch (added 2026-04-27 by /plan-eng-review)

### Cursor extension marketplace listing (gated on Premise 5 validation)

**What:** Build + ship a Cursor extension (TypeScript) that registers Atlas via the Cursor extension API and lists in the marketplace.

**Why:** The "one-click marketplace install" was the original Track 3b aspiration. Today's eng review dropped it from v1 per Codex finding 7 (most-likely-to-slip) and the user's accepted scope reduction. If cold-OSS pull validates after launch (>=50 installs + >=1 install-to-Alma-conversation in first 4 weeks per Premise 5), the marketplace listing materially lowers friction for the next 1000 users.

**Pros:** Polished UX for Cursor users; competitive with dbt-mcp / snowflake-mcp marketplace listings; lower friction once you have audience signal.
**Cons:** TypeScript scaffold + marketplace approval pipeline + extension API maintenance — multi-week work; only worth doing post-validation.
**Context:** v1 ships pip install + `alma-atlas install cursor` CLI as the install path. That's already conventional OSS UX. Extension is the next-layer-of-polish bet.
**Depends on:** Premise 5 validation in week 4 cold-dev metrics. If validation fails, this TODO never activates (cold-OSS hypothesis disconfirmed).

### LLM eval comparing raw MCP responses vs CompanionBundle (post-launch)

**What:** Weekly eval suite running 10 representative data-stack questions through an agent twice — once with raw MCP handler responses, once with the new CompanionBundle output. Score answer accuracy, completeness, token efficiency. Use Fintual's anonymized graph as the eval set (with their consent).

**Why:** The whole point of CompanionBundle (Issue 2A + Tension 1) is that it's a better agent context format than raw metadata. That claim deserves measurement. Without an eval, the bundle-vs-raw claim is unvalidated.

**Pros:** Empirical validation of the bundle claim; baseline for future improvements; signal for whether inner-agent should also adopt the bundle format.
**Cons:** Eval infrastructure (test set, scoring rubric, runner) is real work; needs Fintual consent; needs LLM-as-judge or human review.
**Context:** Design accepted CompanionBundle on coupling-decoupling grounds (Tension 1), not on measured agent-quality grounds. Gap worth closing post-launch.
**Depends on:** Atlas Companion in production at Fintual; Fintual signed consent for anonymized graph use; LLM-judge or curated test set with expected answers.

