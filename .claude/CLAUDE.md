# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## Instructions

### General

Write comments/documents in English.
Keep text short and brief.

Keep a comment only if removing it would leave something unexplained: a
constraint or invariant the code can't say for itself. Don't keep one
that just restates what the next line already shows, or that walks
through a struct's fields one by one; the fields already do that.

Write the conclusion, not the deliberation. State what's true now; leave
out how you got there or which alternative you rejected along the way.

State an invariant once, where a reader would naturally look for it.
Don't repeat the same rationale across a struct's doc, its fields, and
every function that touches it.

Prefer ASCII. Avoid non-ASCII characters such as:
- em dashes (—)
- curly quotes ("“”")
- ellipses (…)

Minimize `--`, parentheses, and bullet lists as a substitute for properly
structured prose. Work out the logical structure of the reasoning and
write it as full sentences instead.

In Markdown, use reference-style links:

```markdown
Read [examples] for more information.

[examples]: https://example.com
```
