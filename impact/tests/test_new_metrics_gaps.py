"""
Tests covering gaps identified in the REVIEW.md audit:
- temporal_logical_coupling 50-file cap
- change_inducing_review_rate file-overlap heuristic
- pr_merge_effectiveness merge_effectiveness threshold key
- review_quality.py re-export shim
- domain models (ReleaseRecord, DeploymentRecord, WorkflowRunRecord)
- ai_bots.yaml config loading
- conventional_commit_rate direct import (renamed file)
"""

from datetime import UTC, datetime, timedelta

from impact.domain.models import (
    CommentType,
    DeploymentRecord,
    ReleaseRecord,
    ReviewState,
    WorkflowRunRecord,
)
from impact.metrics.plugins.authored.temporal_logical_coupling import TemporalLogicalCoupling
from impact.metrics.plugins.influence.change_inducing_review_rate import ChangeInducingReviewRate
from impact.metrics.plugins.authored.pr_merge_effectiveness import PRMergeEffectiveness
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_comment,
    make_commit,
    make_context,
    make_file,
    make_pr,
    make_repo,
    make_review,
    make_user,
)


# ── temporal_logical_coupling: 50-file cap ─────────────────────────

def test_coupling_skips_mega_prs():
    """PRs with > 50 files should be skipped in coupling analysis."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    # PR1: a normal PR with 2 files
    pr1 = make_pr(1, user, repo, base_time=start)
    f1 = make_file("s1", "src/a.py", pr_number=1)
    f2 = make_file("s2", "src/b.py", pr_number=1)

    # PR2: a mega-PR with 60 files → should be skipped
    pr2 = make_pr(2, user, repo, base_time=start, created_delta_hours=24)
    mega_files = [
        make_file(f"sha{i}", f"src/file{i}.py", pr_number=2)
        for i in range(60)
    ]

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1, pr2],
        files=[f1, f2] + mega_files,
    )
    end = start + timedelta(days=30)
    ctx = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    result = TemporalLogicalCoupling().run(ctx)

    # Only PR1's pair should appear (PR2 skipped due to > 50 files)
    assert result.details["pair_count"] == 1
    top = result.details["top_coupled_pairs"][0]
    assert top["file_a"] == "src/a.py"
    assert top["file_b"] == "src/b.py"


def test_coupling_includes_49_file_prs():
    """PRs with exactly 49 files should NOT be skipped."""
    user = make_user(id=1, login="alice")
    owner = make_user(id=2, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    pr1 = make_pr(1, user, repo, base_time=start)
    files_49 = [
        make_file(f"sha{i}", f"src/file{i}.py", pr_number=1)
        for i in range(49)
    ]

    bundle = make_bundle(
        users=[user, owner],
        repositories=[repo],
        pull_requests=[pr1],
        files=files_49,
    )
    end = start + timedelta(days=30)
    ctx = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    result = TemporalLogicalCoupling().run(ctx)

    # 49 files produces C(49,2) = 1176 pairs, but all have coupling 100
    # since there's only one PR
    assert result.details["pair_count"] > 0


# ── change_inducing_review_rate: file-overlap heuristic ─────────

def test_cir_file_overlap_filters_false_positives():
    """Review on file X, but post-review commit touches file Y → not induced."""
    reviewer = make_user(id=1, login="alice")
    author = make_user(id=2, login="bob")
    owner = make_user(id=3, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    pr1 = make_pr(1, author, repo, base_time=start)

    review = make_review(10, 1, reviewer, start + timedelta(hours=1), state=ReviewState.COMMENTED)
    # Review comment on src/x.py
    comment = make_comment(
        100, 1, reviewer, start + timedelta(hours=1),
        type=CommentType.REVIEW, review_id=10, path="src/x.py",
    )
    # Commit after review on src/y.py (different file)
    commit = make_commit("sha1", author, start + timedelta(hours=3), 1)
    file_y = make_file("sha_y", "src/y.py", pr_number=1)

    bundle = make_bundle(
        users=[reviewer, author, owner],
        repositories=[repo],
        pull_requests=[pr1],
        commits=[commit],
        reviews=[review],
        comments=[comment],
        files=[file_y],
    )
    end = start + timedelta(days=30)
    ctx = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    result = ChangeInducingReviewRate().run(ctx)

    # File overlap is empty → should NOT count as induced
    assert result.details["inducing_count"] == 0


def test_cir_file_overlap_allows_true_positives():
    """Review on file X, post-review commit touches file X → induced."""
    reviewer = make_user(id=1, login="alice")
    author = make_user(id=2, login="bob")
    owner = make_user(id=3, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    pr1 = make_pr(1, author, repo, base_time=start)

    review = make_review(10, 1, reviewer, start + timedelta(hours=1), state=ReviewState.COMMENTED)
    # Review comment on src/x.py
    comment = make_comment(
        100, 1, reviewer, start + timedelta(hours=1),
        type=CommentType.REVIEW, review_id=10, path="src/x.py",
    )
    # Commit after review also touches src/x.py
    commit = make_commit("sha1", author, start + timedelta(hours=3), 1)
    file_x = make_file("sha_x", "src/x.py", pr_number=1)

    bundle = make_bundle(
        users=[reviewer, author, owner],
        repositories=[repo],
        pull_requests=[pr1],
        commits=[commit],
        reviews=[review],
        comments=[comment],
        files=[file_x],
    )
    end = start + timedelta(days=30)
    ctx = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    result = ChangeInducingReviewRate().run(ctx)

    assert result.details["inducing_count"] == 1


# ── pr_merge_effectiveness: merge_effectiveness threshold key ───

def test_pr_merge_effectiveness_has_threshold_key():
    """Result details should contain 'merge_effectiveness' key for thresholds."""
    user = make_user(id=1, login="alice")
    reviewer = make_user(id=2, login="bob")
    owner = make_user(id=3, login="org")
    repo = make_repo(id=1, name="repo", owner=owner)

    start = DEFAULT_START
    pr1 = make_pr(1, user, repo, created_at=start, merged_at=start + timedelta(hours=5))
    review = make_review(101, 1, reviewer, start + timedelta(hours=2), ReviewState.APPROVED)

    bundle = make_bundle(
        users=[user, reviewer, owner],
        repositories=[repo],
        pull_requests=[pr1],
        reviews=[review],
    )
    end = start + timedelta(days=30)
    ctx = make_context(bundle, user_login="alice", start_date=start, end_date=end)

    result = PRMergeEffectiveness().run(ctx)

    # Must have merge_effectiveness key (what thresholds.py keys on)
    assert "merge_effectiveness" in result.details
    assert isinstance(result.details["merge_effectiveness"], (int, float))
    # Verify it equals the average_back_and_forth rounded
    assert result.details["merge_effectiveness"] == round(
        result.details["average_back_and_forth"], 2
    )


# ── review_quality.py re-export shim ──────────────────────────────

def test_review_quality_reexport_shim():
    """Importing from review_quality.py shim should work for all 3 classes."""
    from impact.metrics.plugins.authored.review_quality import (
        ReviewIterations,
        SlowReviewResponse,
        TimeToFirstReview,
    )
    # Also verify direct imports work
    from impact.metrics.plugins.authored.review_iterations import (
        ReviewIterations as RI,
    )
    from impact.metrics.plugins.authored.time_to_first_review import (
        TimeToFirstReview as TTFR,
    )
    from impact.metrics.plugins.authored.slow_review_response import (
        SlowReviewResponse as SRR,
    )

    assert ReviewIterations is RI
    assert TimeToFirstReview is TTFR
    assert SlowReviewResponse is SRR

    # Verify slug properties
    assert ReviewIterations().slug == "review_iterations"
    assert TimeToFirstReview().slug == "time_to_first_review"
    assert SlowReviewResponse().slug == "slow_review_response"


# ── Domain models: new data pipeline types ────────────────────────

def test_release_record_model():
    """ReleaseRecord should be constructible with expected fields."""
    now = datetime(2026, 3, 1, tzinfo=UTC)
    user = make_user(id=1, login="releaser")
    release = ReleaseRecord(
        id=1,
        tag_name="v1.0.0",
        name="Release 1.0",
        created_at=now,
        published_at=now + timedelta(hours=1),
        draft=False,
        prerelease=False,
        author=user,
        target_commitish="main",
    )
    assert release.tag_name == "v1.0.0"
    assert release.author.login == "releaser"
    assert release.draft is False


def test_deployment_record_model():
    """DeploymentRecord should be constructible with expected fields."""
    now = datetime(2026, 3, 1, tzinfo=UTC)
    deploy = DeploymentRecord(
        id=1,
        sha="abc123",
        ref="main",
        environment="production",
        created_at=now,
    )
    assert deploy.environment == "production"
    assert deploy.sha == "abc123"


def test_workflow_run_record_model():
    """WorkflowRunRecord should be constructible with expected fields."""
    now = datetime(2026, 3, 1, tzinfo=UTC)
    run = WorkflowRunRecord(
        id=1,
        head_sha="def456",
        created_at=now,
        status="completed",
        conclusion="success",
    )
    assert run.conclusion == "success"
    assert run.status == "completed"


def test_canonical_bundle_new_fields_default_empty():
    """CanonicalBundle should have empty lists for new data sources by default."""
    bundle = make_bundle()
    assert bundle.releases == []
    assert bundle.deployments == []
    assert bundle.workflow_runs == []


# ── AI bots YAML config ──────────────────────────────────────────

def test_ai_bot_yaml_loading():
    """_load_ai_bot_logins should load from ai_bots.yaml."""
    from impact.metrics.plugins.authored.ai_suggestion_acceptance import (
        AI_BOT_LOGINS,
        _is_ai_bot,
        _load_ai_bot_logins,
    )

    # Should have loaded something
    logins = _load_ai_bot_logins()
    assert len(logins) > 0
    assert "copilot" in logins or "copilot-pull-request-reviewer[bot]" in logins

    # Module-level constant should also be populated
    assert len(AI_BOT_LOGINS) > 0

    # Known bots should match
    assert _is_ai_bot("copilot-pull-request-reviewer[bot]")
    assert _is_ai_bot("bito-code-review[bot]")

    # Any [bot] suffix should match (catch-all rule)
    assert _is_ai_bot("random-new-bot[bot]")

    # Regular users should not match
    assert not _is_ai_bot("alice")
    assert not _is_ai_bot("")


def test_ai_bot_yaml_case_insensitive():
    """Bot matching should be case-insensitive."""
    from impact.metrics.plugins.authored.ai_suggestion_acceptance import _is_ai_bot

    assert _is_ai_bot("Copilot-Pull-Request-Reviewer[bot]")


# ── conventional_commit_rate direct import (renamed file) ─────────

def test_conventional_commit_rate_direct_import():
    """Importing directly from the renamed file should work."""
    from impact.metrics.plugins.authored.conventional_commit_rate import (
        ConventionalCommitRate,
    )

    metric = ConventionalCommitRate()
    assert metric.slug == "conventional_commit_rate"
    assert metric.category == "process_discipline"
