from datetime import timedelta

from impact.metrics.plugins.authored.pr_body_quality import PRBodyQualityScore
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_context,
    make_pr,
    make_repo,
    make_user,
)


def test_pr_body_quality_score():
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    # PR with good body: long, sections, issue ref, PR ref
    good_body = """### Summary
Fixes issue #123 by updating the widget.

## Details
See PR #456 for related changes.
This is a detailed description with multiple sections."""
    pr_good = make_pr(1, user, repo, base_time=start, body=good_body, created_delta_hours=0)

    # PR with minimal body
    pr_minimal = make_pr(2, user, repo, base_time=start, body="small fix", created_delta_hours=10)

    # PR with no body
    pr_none = make_pr(3, user, repo, base_time=start, body=None, created_delta_hours=20)

    # PR with medium body, one section, issue ref
    med_body = "### Summary\nCloses #789. Some text here."
    pr_med = make_pr(4, user, repo, base_time=start, body=med_body, created_delta_hours=30)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr_good, pr_minimal, pr_none, pr_med],
    )
    end = start + timedelta(days=10)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = PRBodyQualityScore()
    res = metric.run(context)

    assert res.metric_slug == "pr_body_quality_score"
    assert res.details["pr_count"] == 4
    assert res.details["average_score"] > 0
    per_pr = res.details["per_pr"]
    # good should be high (>=70)
    good_score = next(p["score"] for p in per_pr if p["number"] == 1)
    assert good_score >= 70
    # minimal low
    min_score = next(p["score"] for p in per_pr if p["number"] == 2)
    assert min_score <= 20
    # none =0
    none_score = next(p["score"] for p in per_pr if p["number"] == 3)
    assert none_score == 0
    # check summary mentions score
    assert "Avg body quality score" in res.summary
