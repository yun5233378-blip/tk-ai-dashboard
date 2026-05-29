# SuperSpider Absorption Notes

Source project: `Lyx3314844-03/superspider`

License: MIT

This project does not vendor the full SuperSpider framework. Instead, it absorbs
several useful crawler-engine ideas into the TK-AI dashboard:

- Site preset shape: platform family, allowed domains, browser viewport,
  wait/scroll actions, capture list, and stop conditions.
- Crawler selection contract: runner order, recommended runner, capabilities,
  strategy hints, fallback plan, and confidence.
- Access friction playbook: classify login, CAPTCHA, risk control, rate limit,
  script shell, WAF, and blocked responses.
- Authorized-session replay: reuse only operator-provided browser login state,
  never force-bypass CAPTCHA or private access gates.
- Artifact capture: save HTML, screenshot, network summary, and friction report
  when a platform page cannot produce usable comments.

Implementation files:

- `crawler_engine.py`
- `scrape_multi_source_comments.py`
- `server.py` endpoint: `POST /api/admin/crawler-preflight`

Operational boundary:

The crawler should make failures explainable and recoverable. It should not
promise automatic CAPTCHA solving, stealth bypass, or access to non-public data
without an authorized operator session.
