# Security policy

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security problems.

Report privately via GitHub's **Security Advisories → Report a vulnerability**
on this repo, or by email to the maintainers listed in the git history.

Include, when possible:

- A description of the issue and its impact.
- Steps to reproduce, or a proof-of-concept.
- Any affected versions or commit hashes.
- Your proposed fix, if you have one.

We'll acknowledge within a few days, work on a fix, and coordinate
disclosure with you before publishing any details.

## Scope

In scope:

- The indexer binary (`dmt-indexer`) and its HTTP/WS API surface.
- Ledger-correctness bugs that cause balances to diverge from the TAP
  protocol rules.
- Anything that lets a client read data it shouldn't (e.g. admin
  endpoint auth bypass).
- Anything that lets a client crash or wedge the indexer.

Out of scope:

- Problems that require access to the operator's `config.toml`,
  cookie file, or bitcoind (these are pre-auth primitives, not vulns).
- Denial-of-service via raw traffic volume against an unconfigured
  operator. The API ships with opt-in per-IP rate limiting; tune it
  for your deployment.
- Third-party dependencies — report upstream.
