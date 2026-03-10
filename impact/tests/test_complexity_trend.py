from datetime import timedelta

from impact.metrics.plugins.authored.complexity_trend import ComplexityTrend
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_context,
    make_file,
    make_pr,
    make_repo,
    make_user,
)


def test_complexity_trend_tracks_stats_per_file():
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    pr2 = make_pr(2, user, repo, base_time=start, created_delta_hours=24)

    patch_shallow = """@@
+def foo():
+    return 1
"""
    patch_deep = """@@
+def foo():
+    if True:
+        return 2
"""

    files = [
        make_file("sha1", "src/app.py", pr_number=1, patch=patch_shallow, changes=10),
        make_file("sha2", "src/app.py", pr_number=2, patch=patch_deep, changes=12),
        make_file("sha3", "src/utils.py", pr_number=1, patch=patch_shallow, changes=5),
    ]

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2],
        files=files,
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = ComplexityTrend()
    res = metric.run(context)

    assert res.metric_slug == "complexity_trend"
    assert res.details["file_count"] == 2
    assert res.details["sample_count"] == 3
    assert res.details["max_std_dev"] > 0
    assert res.details["max_complexity"] >= res.details["min_complexity"]

    app_trend = next(item for item in res.details["file_trends"] if item["filename"] == "src/app.py")
    assert app_trend["min_complexity"] == 0.5
    assert app_trend["max_complexity"] == 1.0
    assert app_trend["std_dev"] > 0


def test_complexity_trend_excludes_generated_files():
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    patch = """@@
+def foo():
+    return 1
"""

    files = [
        make_file("sha1", "src/main.py", pr_number=1, patch=patch, changes=10),
        make_file("sha2", "package-lock.json", pr_number=1, patch=patch, changes=5000),
    ]

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=files,
    )
    context = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=30))

    metric = ComplexityTrend()
    res = metric.run(context)

    assert res.details["file_count"] == 1
    assert res.details["sample_count"] == 1


def test_complexity_trend_no_data():
    user = make_user(login="alice")
    bundle = make_bundle(users=[user], pull_requests=[], files=[])
    context = make_context(bundle, user_login="alice")

    metric = ComplexityTrend()
    res = metric.run(context)

    assert res.details["no_data"] is True
    assert res.details["file_count"] == 0
    assert res.details["sample_count"] == 0
