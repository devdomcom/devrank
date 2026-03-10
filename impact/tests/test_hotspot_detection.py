from datetime import timedelta
from impact.metrics.plugins.authored.hotspot_detection import HotspotDetection
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_context,
    make_file,
    make_pr,
    make_repo,
    make_user,
)


def test_hotspot_detection():
    """Test hotspot detection with multiple revisions and varying changes."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    # PR1: modifies fileA (100 changes) and fileB (20 changes)
    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    f1 = make_file("sha1", "src/fileA.py", changes=100, pr_number=1)
    f2 = make_file("sha2", "src/fileB.py", changes=20, pr_number=1)

    # PR2: modifies fileA (50 changes) and fileC (10 changes)
    pr2 = make_pr(2, user, repo, base_time=start, created_delta_hours=24)
    f3 = make_file("sha3", "src/fileA.py", changes=50, pr_number=2)
    f4 = make_file("sha4", "src/fileC.py", changes=10, pr_number=2)

    # PR3: modifies fileA (30 changes) and fileB (80 changes)
    pr3 = make_pr(3, user, repo, base_time=start, created_delta_hours=48)
    f5 = make_file("sha5", "src/fileA.py", changes=30, pr_number=3)
    f6 = make_file("sha6", "src/fileB.py", changes=80, pr_number=3)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2, pr3],
        files=[f1, f2, f3, f4, f5, f6],
    )
    end = start + timedelta(days=30)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = HotspotDetection()
    res = metric.run(context)

    assert res.metric_slug == "hotspot_detection"
    
    # Stats for fileA:
    # frequency = 3
    # total_changes = 100 + 50 + 30 = 180
    # avg_changes = 180 / 3 = 60
    # hotspot_score = 3 * 60 = 180
    
    # Stats for fileB:
    # frequency = 2
    # total_changes = 20 + 80 = 100
    # avg_changes = 100 / 2 = 50
    # hotspot_score = 2 * 50 = 100
    
    # Stats for fileC:
    # frequency = 1
    # total_changes = 10
    # avg_changes = 10
    # hotspot_score = 1 * 10 = 10

    hotspots = res.details["top_hotspots"]
    assert len(hotspots) == 3
    
    assert hotspots[0]["filename"] == "src/fileA.py"
    assert hotspots[0]["hotspot_score"] == 180.0
    assert hotspots[0]["frequency"] == 3
    assert hotspots[0]["avg_changes"] == 60.0
    
    assert hotspots[1]["filename"] == "src/fileB.py"
    assert hotspots[1]["hotspot_score"] == 100.0
    
    assert hotspots[2]["filename"] == "src/fileC.py"
    assert hotspots[2]["hotspot_score"] == 10.0
    
    assert res.details["max_hotspot_score"] == 180.0
    assert "Top hotspot: src/fileA.py" in res.summary


def test_hotspot_detection_excludes_generated_files():
    """Generated files should not be detected as hotspots."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)
    start = DEFAULT_START

    # PR1: real file + lockfile (many changes)
    pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
    f1 = make_file("sha1", "src/main.py", changes=10, pr_number=1)
    f2 = make_file("sha2", "package-lock.json", changes=5000, pr_number=1)

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=[f1, f2],
    )
    context = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=1))

    metric = HotspotDetection()
    res = metric.run(context)

    # Only src/main.py should be in hotspots
    hotspots = res.details["top_hotspots"]
    assert len(hotspots) == 1
    assert hotspots[0]["filename"] == "src/main.py"
    assert hotspots[0]["hotspot_score"] == 10.0


def test_hotspot_detection_no_data():
    """Edge: no PRs -> no data."""
    user = make_user(login="alice")
    bundle = make_bundle(users=[user], pull_requests=[])
    context = make_context(bundle, user_login="alice")
    
    metric = HotspotDetection()
    res = metric.run(context)
    
    assert res.details["no_data"] is True
    assert res.details["max_hotspot_score"] == 0.0
    assert len(res.details["top_hotspots"]) == 0
