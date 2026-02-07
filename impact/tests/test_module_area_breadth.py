from datetime import timedelta

from impact.metrics.plugins.authored.module_area_breadth import (
    ModuleAreaBreadth,
    normalize_path_to_area,
)
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_context,
    make_file,
    make_pr,
    make_repo,
    make_user,
)


def test_normalize_path_to_area():
    assert normalize_path_to_area("frontend/components/Button.js") == "frontend"
    assert normalize_path_to_area("src/backend/api.py") == "src"
    assert normalize_path_to_area("main.py") == "main.py"
    assert normalize_path_to_area(".gitignore") == ".gitignore"
    assert normalize_path_to_area("") == "unknown"
    assert normalize_path_to_area("a/b/c", levels=2) == "a/b"


def test_module_area_breadth_with_various_areas():
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    pr1 = make_pr(1, user, repo, additions=10, deletions=5)
    pr2 = make_pr(2, user, repo, additions=20, deletions=10)
    pr3 = make_pr(3, user, repo, additions=30, deletions=15)

    files = [
        make_file("sha1", "frontend/components/Button.js", pr_number=1),
        make_file("sha2", "frontend/utils/helpers.py", pr_number=1),
        make_file("sha3", "backend/api/models.py", pr_number=2),
        make_file("sha4", "backend/api/views.py", pr_number=2),
        make_file("sha5", "infra/docker/Dockerfile", pr_number=3),
        make_file("sha6", "docs/README.md", pr_number=3),
    ]

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2, pr3],
        files=files,
    )
    end = start + timedelta(days=10)
    context = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    metric = ModuleAreaBreadth()
    res = metric.run(context)

    assert res.metric_slug == "module_area_breadth"
    assert res.details["total_pr_count"] == 3
    assert res.details["total_files_touched"] == 6
    assert res.details["distinct_areas_count"] == 4  # frontend, backend, infra, docs
    assert set(res.details["distinct_areas"]) == {"docs", "frontend", "backend", "infra"}
    assert res.details["areas_per_pr"] == 4 / 3  # 1.333...
    assert res.details["truncation_levels"] == 1
    assert res.summary == "Touched 4 distinct areas"


def test_module_area_breadth_no_prs():
    user = make_user(id=1, login="alice")
    bundle = make_bundle(users=[user], pull_requests=[], files=[])
    context = make_context(bundle, user_login="alice")

    metric = ModuleAreaBreadth()
    res = metric.run(context)

    assert res.details["total_pr_count"] == 0
    assert res.details["total_files_touched"] == 0
    assert res.details["distinct_areas_count"] == 0
    assert res.details["distinct_areas"] == []
    assert res.details["areas_per_pr"] == 0.0
    assert res.summary == "Touched 0 distinct areas"


def test_module_area_breadth_single_pr_multiple_areas():
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    pr = make_pr(1, user, repo, additions=10, deletions=5)
    files = [
        make_file("sha1", "frontend/Button.js", pr_number=1),
        make_file("sha2", "backend/api.py", pr_number=1),
        make_file("sha3", "frontend/utils.js", pr_number=1),  # same area
    ]

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr],
        files=files,
    )
    context = make_context(bundle, user_login="alice")

    metric = ModuleAreaBreadth()
    res = metric.run(context)

    assert res.details["total_pr_count"] == 1
    assert res.details["total_files_touched"] == 3
    assert res.details["distinct_areas_count"] == 2  # frontend, backend
    assert set(res.details["distinct_areas"]) == {"frontend", "backend"}
    assert res.details["areas_per_pr"] == 2.0
    assert res.summary == "Touched 2 distinct areas"


def test_module_area_breadth_overlapping_areas():
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    pr1 = make_pr(1, user, repo)
    pr2 = make_pr(2, user, repo)
    files = [
        make_file("sha1", "frontend/a.js", pr_number=1),
        make_file("sha2", "backend/b.py", pr_number=1),
        make_file("sha3", "frontend/c.js", pr_number=2),  # overlapping
        make_file("sha4", "docs/d.md", pr_number=2),
    ]

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2],
        files=files,
    )
    context = make_context(bundle, user_login="alice")

    metric = ModuleAreaBreadth()
    res = metric.run(context)

    assert res.details["distinct_areas_count"] == 3  # frontend, backend, docs
    assert res.details["areas_per_pr"] == 2.0  # avg per-PR uniques: (2 + 2) / 2
