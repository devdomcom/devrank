"""
Tests for Review Coverage metric.

Covers:
  1. Metric properties (slug, name, category, frameworks)
  2. No PRs → no_data
  3. Single PR with full coverage (all files reviewed)
  4. Single PR with partial coverage
  5. Single PR with zero coverage (no review comments)
  6. Self-comments excluded (PR author commenting on own files)
  7. Bot reviewers excluded
  8. Generated files excluded from denominator
  9. Issue comments excluded (only REVIEW type counts)
 10. Comments without path excluded (non-inline)
 11. Multiple PRs → aggregate coverage
 12. Fully-reviewed and unreviewed PR counters
 13. No-data guard for short period + low count
 14. Sufficient data with longer period
 15. Per-PR detail structure
 16. PR with only generated files skipped entirely
 17. Summary text formatting
 18. Threshold + scoring integration
"""

from datetime import timedelta

import pytest

from impact.metrics.plugins.authored.review_coverage import ReviewCoverage
from impact.domain.models import CommentType
from impact.thresholds import METRIC_THRESHOLDS, score_metric
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_comment,
    make_context,
    make_file,
    make_pr,
    make_repo,
    make_user,
)


# ── Fixtures ──────────────────────────────────────────────────────────────


@pytest.fixture
def metric():
    return ReviewCoverage()


@pytest.fixture
def alice():
    return make_user(id=1, login="alice")


@pytest.fixture
def bob():
    return make_user(id=2, login="bob")


@pytest.fixture
def bot_user():
    return make_user(id=3, login="dependabot[bot]", is_bot=True)


@pytest.fixture
def repo(alice):
    owner = make_user(id=99, login="org")
    return make_repo(id=1, name="repo", owner=owner)


# ── 1. Metric Properties ─────────────────────────────────────────────────


class TestMetricProperties:
    def test_slug(self, metric):
        assert metric.slug == "review_coverage"

    def test_name(self, metric):
        assert metric.name == "Review Coverage"

    def test_category(self, metric):
        assert metric.category == "code_quality"

    def test_frameworks(self, metric):
        assert "DevRank" in metric.frameworks

    def test_description(self, metric):
        assert "review comment" in metric.description.lower()


# ── 2. No PRs → no_data ──────────────────────────────────────────────────


class TestNoPRs:
    def test_no_prs_returns_no_data(self, metric, alice, repo):
        bundle = make_bundle(
            users=[alice],
            repositories=[repo],
            pull_requests=[],
        )
        ctx = make_context(bundle, "alice", DEFAULT_START, DEFAULT_START + timedelta(days=30))
        result = metric.run(ctx)

        assert result.metric_slug == "review_coverage"
        assert result.details["no_data"] is True
        assert result.details["coverage_pct"] == 0.0
        assert result.details["total_files"] == 0
        assert "No PRs found" in result.summary


# ── 3. Full Coverage ─────────────────────────────────────────────────────


class TestFullCoverage:
    def test_all_files_reviewed(self, metric, alice, bob, repo):
        start = DEFAULT_START
        pr = make_pr(1, alice, repo, base_time=start, created_delta_hours=0, merged_delta_hours=24)

        files = [
            make_file("s1", "src/main.py", pr_number=1),
            make_file("s2", "src/utils.py", pr_number=1),
        ]
        comments = [
            make_comment(1, 1, bob, start + timedelta(hours=2),
                         type=CommentType.REVIEW, path="src/main.py"),
            make_comment(2, 1, bob, start + timedelta(hours=3),
                         type=CommentType.REVIEW, path="src/utils.py"),
        ]

        bundle = make_bundle(
            users=[alice, bob],
            repositories=[repo],
            pull_requests=[pr],
            files=files,
            comments=comments,
        )
        ctx = make_context(bundle, "alice", start, start + timedelta(days=30))
        result = metric.run(ctx)

        assert result.details["coverage_pct"] == 100.0
        assert result.details["total_files"] == 2
        assert result.details["reviewed_files"] == 2
        assert result.details["unreviewed_files"] == 0
        assert result.details["fully_reviewed_prs"] == 1
        assert result.details["unreviewed_prs"] == 0


# ── 4. Partial Coverage ──────────────────────────────────────────────────


class TestPartialCoverage:
    def test_some_files_reviewed(self, metric, alice, bob, repo):
        start = DEFAULT_START
        pr = make_pr(1, alice, repo, base_time=start, created_delta_hours=0, merged_delta_hours=24)

        files = [
            make_file("s1", "src/main.py", pr_number=1),
            make_file("s2", "src/utils.py", pr_number=1),
            make_file("s3", "src/config.py", pr_number=1),
        ]
        # Only review one file
        comments = [
            make_comment(1, 1, bob, start + timedelta(hours=2),
                         type=CommentType.REVIEW, path="src/main.py"),
        ]

        bundle = make_bundle(
            users=[alice, bob],
            repositories=[repo],
            pull_requests=[pr],
            files=files,
            comments=comments,
        )
        ctx = make_context(bundle, "alice", start, start + timedelta(days=30))
        result = metric.run(ctx)

        assert result.details["coverage_pct"] == pytest.approx(33.3, abs=0.1)
        assert result.details["total_files"] == 3
        assert result.details["reviewed_files"] == 1
        assert result.details["unreviewed_files"] == 2


# ── 5. Zero Coverage ─────────────────────────────────────────────────────


class TestZeroCoverage:
    def test_no_review_comments(self, metric, alice, repo):
        start = DEFAULT_START
        pr = make_pr(1, alice, repo, base_time=start, created_delta_hours=0, merged_delta_hours=24)

        files = [
            make_file("s1", "src/main.py", pr_number=1),
            make_file("s2", "src/utils.py", pr_number=1),
        ]

        bundle = make_bundle(
            users=[alice],
            repositories=[repo],
            pull_requests=[pr],
            files=files,
            comments=[],
        )
        ctx = make_context(bundle, "alice", start, start + timedelta(days=30))
        result = metric.run(ctx)

        assert result.details["coverage_pct"] == 0.0
        assert result.details["reviewed_files"] == 0
        assert result.details["unreviewed_prs"] == 1
        assert "no file-level comments" in result.summary


# ── 6. Self-Comments Excluded ────────────────────────────────────────────


class TestSelfCommentsExcluded:
    def test_author_comments_on_own_pr_not_counted(self, metric, alice, repo):
        start = DEFAULT_START
        pr = make_pr(1, alice, repo, base_time=start, created_delta_hours=0, merged_delta_hours=24)

        files = [make_file("s1", "src/main.py", pr_number=1)]
        # Alice comments on her own PR → should not count as review coverage
        comments = [
            make_comment(1, 1, alice, start + timedelta(hours=2),
                         type=CommentType.REVIEW, path="src/main.py"),
        ]

        bundle = make_bundle(
            users=[alice],
            repositories=[repo],
            pull_requests=[pr],
            files=files,
            comments=comments,
        )
        ctx = make_context(bundle, "alice", start, start + timedelta(days=30))
        result = metric.run(ctx)

        assert result.details["coverage_pct"] == 0.0
        assert result.details["reviewed_files"] == 0


# ── 7. Bot Reviewers Excluded ────────────────────────────────────────────


class TestBotReviewersExcluded:
    def test_bot_comments_not_counted(self, metric, alice, bot_user, repo):
        start = DEFAULT_START
        pr = make_pr(1, alice, repo, base_time=start, created_delta_hours=0, merged_delta_hours=24)

        files = [make_file("s1", "src/main.py", pr_number=1)]
        comments = [
            make_comment(1, 1, bot_user, start + timedelta(hours=2),
                         type=CommentType.REVIEW, path="src/main.py"),
        ]

        bundle = make_bundle(
            users=[alice, bot_user],
            repositories=[repo],
            pull_requests=[pr],
            files=files,
            comments=comments,
        )
        ctx = make_context(bundle, "alice", start, start + timedelta(days=30))
        result = metric.run(ctx)

        assert result.details["coverage_pct"] == 0.0
        assert result.details["reviewed_files"] == 0


# ── 8. Generated Files Excluded ──────────────────────────────────────────


class TestGeneratedFilesExcluded:
    def test_lockfiles_excluded_from_denominator(self, metric, alice, bob, repo):
        start = DEFAULT_START
        pr = make_pr(1, alice, repo, base_time=start, created_delta_hours=0, merged_delta_hours=24)

        files = [
            make_file("s1", "src/main.py", pr_number=1),
            make_file("s2", "package-lock.json", pr_number=1),  # generated
        ]
        # Review comment only on the real file
        comments = [
            make_comment(1, 1, bob, start + timedelta(hours=2),
                         type=CommentType.REVIEW, path="src/main.py"),
        ]

        bundle = make_bundle(
            users=[alice, bob],
            repositories=[repo],
            pull_requests=[pr],
            files=files,
            comments=comments,
        )
        ctx = make_context(bundle, "alice", start, start + timedelta(days=30))
        result = metric.run(ctx)

        # Only 1 non-generated file, and it was reviewed → 100%
        assert result.details["coverage_pct"] == 100.0
        assert result.details["total_files"] == 1
        assert result.details["generated_files_excluded"] == 1


# ── 9. Issue Comments Excluded ───────────────────────────────────────────


class TestIssueCommentsExcluded:
    def test_issue_type_comments_not_counted(self, metric, alice, bob, repo):
        start = DEFAULT_START
        pr = make_pr(1, alice, repo, base_time=start, created_delta_hours=0, merged_delta_hours=24)

        files = [make_file("s1", "src/main.py", pr_number=1)]
        # Issue comment (type=ISSUE), not a review comment
        comments = [
            make_comment(1, 1, bob, start + timedelta(hours=2),
                         type=CommentType.ISSUE, path="src/main.py"),
        ]

        bundle = make_bundle(
            users=[alice, bob],
            repositories=[repo],
            pull_requests=[pr],
            files=files,
            comments=comments,
        )
        ctx = make_context(bundle, "alice", start, start + timedelta(days=30))
        result = metric.run(ctx)

        assert result.details["coverage_pct"] == 0.0
        assert result.details["reviewed_files"] == 0


# ── 10. Comments Without Path Excluded ───────────────────────────────────


class TestNoPathCommentsExcluded:
    def test_review_comment_without_path_not_counted(self, metric, alice, bob, repo):
        start = DEFAULT_START
        pr = make_pr(1, alice, repo, base_time=start, created_delta_hours=0, merged_delta_hours=24)

        files = [make_file("s1", "src/main.py", pr_number=1)]
        # Review comment with no path (general review body, not inline)
        comments = [
            make_comment(1, 1, bob, start + timedelta(hours=2),
                         type=CommentType.REVIEW, path=None),
        ]

        bundle = make_bundle(
            users=[alice, bob],
            repositories=[repo],
            pull_requests=[pr],
            files=files,
            comments=comments,
        )
        ctx = make_context(bundle, "alice", start, start + timedelta(days=30))
        result = metric.run(ctx)

        assert result.details["coverage_pct"] == 0.0
        assert result.details["reviewed_files"] == 0


# ── 11. Multiple PRs Aggregate ───────────────────────────────────────────


class TestMultiplePRsAggregate:
    def test_aggregate_coverage_across_prs(self, metric, alice, bob, repo):
        start = DEFAULT_START
        # PR 1: 2 files, 1 reviewed → 50%
        pr1 = make_pr(1, alice, repo, base_time=start, created_delta_hours=0, merged_delta_hours=24)
        # PR 2: 1 file, 1 reviewed → 100%
        pr2 = make_pr(2, alice, repo, base_time=start, created_delta_hours=48, merged_delta_hours=72)
        # PR 3: 1 file, 0 reviewed → 0%
        pr3 = make_pr(3, alice, repo, base_time=start, created_delta_hours=96, merged_delta_hours=120)

        files = [
            make_file("s1", "src/a.py", pr_number=1),
            make_file("s2", "src/b.py", pr_number=1),
            make_file("s3", "src/c.py", pr_number=2),
            make_file("s4", "src/d.py", pr_number=3),
        ]
        comments = [
            make_comment(1, 1, bob, start + timedelta(hours=2),
                         type=CommentType.REVIEW, path="src/a.py"),
            make_comment(2, 2, bob, start + timedelta(hours=50),
                         type=CommentType.REVIEW, path="src/c.py"),
        ]

        bundle = make_bundle(
            users=[alice, bob],
            repositories=[repo],
            pull_requests=[pr1, pr2, pr3],
            files=files,
            comments=comments,
        )
        ctx = make_context(bundle, "alice", start, start + timedelta(days=30))
        result = metric.run(ctx)

        # Total: 4 files, 2 reviewed → 50%
        assert result.details["total_files"] == 4
        assert result.details["reviewed_files"] == 2
        assert result.details["coverage_pct"] == 50.0
        assert result.details["total_pr_count"] == 3


# ── 12. Fully-Reviewed and Unreviewed Counters ───────────────────────────


class TestPRLevelCounters:
    def test_fully_reviewed_and_unreviewed_counts(self, metric, alice, bob, repo):
        start = DEFAULT_START
        # PR 1: fully reviewed
        pr1 = make_pr(1, alice, repo, base_time=start, created_delta_hours=0, merged_delta_hours=24)
        # PR 2: partially reviewed (neither fully nor unreviewed)
        pr2 = make_pr(2, alice, repo, base_time=start, created_delta_hours=48, merged_delta_hours=72)
        # PR 3: unreviewed
        pr3 = make_pr(3, alice, repo, base_time=start, created_delta_hours=96, merged_delta_hours=120)

        files = [
            make_file("s1", "src/a.py", pr_number=1),
            make_file("s2", "src/b.py", pr_number=2),
            make_file("s3", "src/c.py", pr_number=2),
            make_file("s4", "src/d.py", pr_number=3),
        ]
        comments = [
            # PR 1: all files reviewed
            make_comment(1, 1, bob, start + timedelta(hours=2),
                         type=CommentType.REVIEW, path="src/a.py"),
            # PR 2: only 1 of 2 files reviewed
            make_comment(2, 2, bob, start + timedelta(hours=50),
                         type=CommentType.REVIEW, path="src/b.py"),
            # PR 3: no comments
        ]

        bundle = make_bundle(
            users=[alice, bob],
            repositories=[repo],
            pull_requests=[pr1, pr2, pr3],
            files=files,
            comments=comments,
        )
        ctx = make_context(bundle, "alice", start, start + timedelta(days=30))
        result = metric.run(ctx)

        assert result.details["fully_reviewed_prs"] == 1
        assert result.details["unreviewed_prs"] == 1
        # PR 2 is partial → neither fully reviewed nor unreviewed


# ── 13. No-Data Guard ────────────────────────────────────────────────────


class TestNoDataGuard:
    def test_short_period_low_count_triggers_no_data(self, metric, alice, bob, repo):
        """period_days < 14 AND per_pr count < 3 → no_data."""
        start = DEFAULT_START
        # Only 5 days, 2 PRs
        pr1 = make_pr(1, alice, repo, base_time=start, created_delta_hours=0, merged_delta_hours=24)
        pr2 = make_pr(2, alice, repo, base_time=start, created_delta_hours=48, merged_delta_hours=72)

        files = [
            make_file("s1", "src/a.py", pr_number=1),
            make_file("s2", "src/b.py", pr_number=2),
        ]
        comments = [
            make_comment(1, 1, bob, start + timedelta(hours=2),
                         type=CommentType.REVIEW, path="src/a.py"),
        ]

        bundle = make_bundle(
            users=[alice, bob],
            repositories=[repo],
            pull_requests=[pr1, pr2],
            files=files,
            comments=comments,
        )
        ctx = make_context(bundle, "alice", start, start + timedelta(days=5))
        result = metric.run(ctx)

        assert result.details["no_data"] is True
        # Coverage is still computed even when no_data
        assert result.details["coverage_pct"] == 50.0


# ── 14. Sufficient Data With Longer Period ────────────────────────────────


class TestSufficientData:
    def test_long_period_with_few_prs_is_valid(self, metric, alice, bob, repo):
        """period_days >= 14 with 2 PRs → no no_data flag."""
        start = DEFAULT_START
        pr1 = make_pr(1, alice, repo, base_time=start, created_delta_hours=0, merged_delta_hours=24)
        pr2 = make_pr(2, alice, repo, base_time=start, created_delta_hours=48, merged_delta_hours=72)

        files = [
            make_file("s1", "src/a.py", pr_number=1),
            make_file("s2", "src/b.py", pr_number=2),
        ]
        comments = [
            make_comment(1, 1, bob, start + timedelta(hours=2),
                         type=CommentType.REVIEW, path="src/a.py"),
        ]

        bundle = make_bundle(
            users=[alice, bob],
            repositories=[repo],
            pull_requests=[pr1, pr2],
            files=files,
            comments=comments,
        )
        ctx = make_context(bundle, "alice", start, start + timedelta(days=20))
        result = metric.run(ctx)

        assert result.details.get("no_data") is not True

    def test_short_period_with_enough_prs(self, metric, alice, bob, repo):
        """period_days < 14 but >= 3 PRs → no no_data flag."""
        start = DEFAULT_START
        prs = [
            make_pr(i, alice, repo, base_time=start,
                    created_delta_hours=i * 24, merged_delta_hours=i * 24 + 12)
            for i in range(1, 4)
        ]
        files = [make_file(f"s{i}", f"src/f{i}.py", pr_number=i) for i in range(1, 4)]
        comments = [
            make_comment(1, 1, bob, start + timedelta(hours=2),
                         type=CommentType.REVIEW, path="src/f1.py"),
        ]

        bundle = make_bundle(
            users=[alice, bob],
            repositories=[repo],
            pull_requests=prs,
            files=files,
            comments=comments,
        )
        ctx = make_context(bundle, "alice", start, start + timedelta(days=5))
        result = metric.run(ctx)

        assert result.details.get("no_data") is not True


# ── 15. Per-PR Detail Structure ──────────────────────────────────────────


class TestPerPRDetail:
    def test_per_pr_structure(self, metric, alice, bob, repo):
        start = DEFAULT_START
        pr = make_pr(1, alice, repo, base_time=start, created_delta_hours=0, merged_delta_hours=24)

        files = [
            make_file("s1", "src/main.py", pr_number=1),
            make_file("s2", "src/utils.py", pr_number=1),
        ]
        comments = [
            make_comment(1, 1, bob, start + timedelta(hours=2),
                         type=CommentType.REVIEW, path="src/main.py"),
        ]

        bundle = make_bundle(
            users=[alice, bob],
            repositories=[repo],
            pull_requests=[pr],
            files=files,
            comments=comments,
        )
        ctx = make_context(bundle, "alice", start, start + timedelta(days=30))
        result = metric.run(ctx)

        per_pr = result.details["per_pr"]
        assert len(per_pr) == 1

        entry = per_pr[0]
        assert entry["number"] == 1
        assert entry["total_files"] == 2
        assert entry["reviewed_files"] == 1
        assert entry["coverage_pct"] == 50.0
        assert "unreviewed" in entry
        assert "src/utils.py" in entry["unreviewed"]

    def test_unreviewed_list_capped_at_5(self, metric, alice, repo):
        """At most 5 unreviewed files listed per PR."""
        start = DEFAULT_START
        pr = make_pr(1, alice, repo, base_time=start, created_delta_hours=0, merged_delta_hours=24)

        # 8 files, none reviewed → 8 unreviewed, but list capped at 5
        files = [make_file(f"s{i}", f"src/f{i}.py", pr_number=1) for i in range(8)]

        bundle = make_bundle(
            users=[alice],
            repositories=[repo],
            pull_requests=[pr],
            files=files,
            comments=[],
        )
        ctx = make_context(bundle, "alice", start, start + timedelta(days=30))
        result = metric.run(ctx)

        assert len(result.details["per_pr"][0]["unreviewed"]) == 5


# ── 16. All Generated Files Skips PR ─────────────────────────────────────


class TestAllGeneratedFilesSkipsPR:
    def test_pr_with_only_generated_files_skipped(self, metric, alice, repo):
        start = DEFAULT_START
        pr = make_pr(1, alice, repo, base_time=start, created_delta_hours=0, merged_delta_hours=24)

        files = [
            make_file("s1", "package-lock.json", pr_number=1),
            make_file("s2", "yarn.lock", pr_number=1),
        ]

        bundle = make_bundle(
            users=[alice],
            repositories=[repo],
            pull_requests=[pr],
            files=files,
            comments=[],
        )
        ctx = make_context(bundle, "alice", start, start + timedelta(days=30))
        result = metric.run(ctx)

        # PR is skipped (no non-generated files) so total_pr_count = 0
        assert result.details["total_pr_count"] == 0
        assert result.details["total_files"] == 0
        assert result.details["generated_files_excluded"] == 2


# ── 17. Summary Text ─────────────────────────────────────────────────────


class TestSummaryText:
    def test_summary_with_coverage(self, metric, alice, bob, repo):
        start = DEFAULT_START
        pr = make_pr(1, alice, repo, base_time=start, created_delta_hours=0, merged_delta_hours=24)

        files = [make_file("s1", "src/main.py", pr_number=1)]
        comments = [
            make_comment(1, 1, bob, start + timedelta(hours=2),
                         type=CommentType.REVIEW, path="src/main.py"),
        ]

        bundle = make_bundle(
            users=[alice, bob],
            repositories=[repo],
            pull_requests=[pr],
            files=files,
            comments=comments,
        )
        ctx = make_context(bundle, "alice", start, start + timedelta(days=30))
        result = metric.run(ctx)

        assert "100.0%" in result.summary
        assert "1/1 files" in result.summary
        assert "1 PRs" in result.summary
        assert "fully reviewed" in result.summary

    def test_summary_with_no_generated_files(self, metric, alice, repo):
        """When only generated files exist, summary mentions it."""
        start = DEFAULT_START
        pr = make_pr(1, alice, repo, base_time=start, created_delta_hours=0, merged_delta_hours=24)

        files = [make_file("s1", "package-lock.json", pr_number=1)]

        bundle = make_bundle(
            users=[alice],
            repositories=[repo],
            pull_requests=[pr],
            files=files,
            comments=[],
        )
        ctx = make_context(bundle, "alice", start, start + timedelta(days=30))
        result = metric.run(ctx)

        assert "No non-generated files" in result.summary


# ── 18. Threshold + Scoring Integration ──────────────────────────────────


class TestThresholdIntegration:
    def test_threshold_entry_exists(self):
        assert "review_coverage" in METRIC_THRESHOLDS
        thresh = METRIC_THRESHOLDS["review_coverage"]
        assert thresh["key"] == "coverage_pct"

    def test_excellent_at_80(self):
        thresh = METRIC_THRESHOLDS["review_coverage"]
        assert thresh["excellent"](80) is True
        assert thresh["excellent"](90) is True
        assert thresh["excellent"](79.9) is False

    def test_good_at_60_to_80(self):
        thresh = METRIC_THRESHOLDS["review_coverage"]
        assert thresh["good"](60) is True
        assert thresh["good"](70) is True
        assert thresh["good"](80) is False
        assert thresh["good"](59) is False

    def test_neutral_at_40_to_60(self):
        thresh = METRIC_THRESHOLDS["review_coverage"]
        assert thresh["neutral"](40) is True
        assert thresh["neutral"](50) is True
        assert thresh["neutral"](60) is False

    def test_bad_below_40(self):
        thresh = METRIC_THRESHOLDS["review_coverage"]
        assert thresh["bad"](0) is True
        assert thresh["bad"](39) is True
        assert thresh["bad"](40) is False

    def test_continuous_scoring(self):
        score_0 = score_metric("review_coverage", 0)
        score_40 = score_metric("review_coverage", 40)
        score_60 = score_metric("review_coverage", 60)
        score_80 = score_metric("review_coverage", 80)
        score_100 = score_metric("review_coverage", 100)

        assert score_0 == 0
        assert score_40 == 25
        assert score_60 == 50
        assert score_80 == 75
        assert score_100 == 100


# ── 19. Multiple Reviewers on Same File ──────────────────────────────────


class TestMultipleReviewers:
    def test_multiple_comments_same_file_counted_once(self, metric, alice, bob, repo):
        """Multiple review comments on the same file still counts as 1 reviewed file."""
        start = DEFAULT_START
        carol = make_user(id=4, login="carol")
        pr = make_pr(1, alice, repo, base_time=start, created_delta_hours=0, merged_delta_hours=24)

        files = [make_file("s1", "src/main.py", pr_number=1)]
        comments = [
            make_comment(1, 1, bob, start + timedelta(hours=2),
                         type=CommentType.REVIEW, path="src/main.py"),
            make_comment(2, 1, carol, start + timedelta(hours=3),
                         type=CommentType.REVIEW, path="src/main.py"),
        ]

        bundle = make_bundle(
            users=[alice, bob, carol],
            repositories=[repo],
            pull_requests=[pr],
            files=files,
            comments=comments,
        )
        ctx = make_context(bundle, "alice", start, start + timedelta(days=30))
        result = metric.run(ctx)

        assert result.details["total_files"] == 1
        assert result.details["reviewed_files"] == 1
        assert result.details["coverage_pct"] == 100.0


# ── 20. Review Comment on Untracked Path ─────────────────────────────────


class TestCommentOnUnknownFile:
    def test_comment_path_not_in_files_list(self, metric, alice, bob, repo):
        """A review comment on a path not in the PR's file list doesn't inflate coverage."""
        start = DEFAULT_START
        pr = make_pr(1, alice, repo, base_time=start, created_delta_hours=0, merged_delta_hours=24)

        files = [make_file("s1", "src/main.py", pr_number=1)]
        comments = [
            # Comment on a different path than the file list
            make_comment(1, 1, bob, start + timedelta(hours=2),
                         type=CommentType.REVIEW, path="src/other.py"),
        ]

        bundle = make_bundle(
            users=[alice, bob],
            repositories=[repo],
            pull_requests=[pr],
            files=files,
            comments=comments,
        )
        ctx = make_context(bundle, "alice", start, start + timedelta(days=30))
        result = metric.run(ctx)

        # The intersection is empty → 0 coverage
        assert result.details["coverage_pct"] == 0.0
        assert result.details["reviewed_files"] == 0


# ── 21. Period Days in Details ────────────────────────────────────────────


class TestPeriodDays:
    def test_period_days_computed(self, metric, alice, repo):
        start = DEFAULT_START
        pr = make_pr(1, alice, repo, base_time=start, created_delta_hours=0, merged_delta_hours=24)
        files = [make_file("s1", "src/a.py", pr_number=1)]

        bundle = make_bundle(
            users=[alice],
            repositories=[repo],
            pull_requests=[pr],
            files=files,
        )
        ctx = make_context(bundle, "alice", start, start + timedelta(days=30))
        result = metric.run(ctx)

        assert result.details["period_days"] == 30.0


# ── 22. Mixed Valid and Bot/Self Comments ─────────────────────────────────


class TestMixedCommentFilters:
    def test_only_valid_human_comments_counted(self, metric, alice, bob, bot_user, repo):
        """Self-comments, bot comments, issue comments, pathless comments all excluded;
        only valid human REVIEW comments with paths count."""
        start = DEFAULT_START
        pr = make_pr(1, alice, repo, base_time=start, created_delta_hours=0, merged_delta_hours=24)

        files = [
            make_file("s1", "src/a.py", pr_number=1),
            make_file("s2", "src/b.py", pr_number=1),
        ]
        comments = [
            # Self-comment → excluded
            make_comment(1, 1, alice, start + timedelta(hours=1),
                         type=CommentType.REVIEW, path="src/a.py"),
            # Bot comment → excluded
            make_comment(2, 1, bot_user, start + timedelta(hours=2),
                         type=CommentType.REVIEW, path="src/a.py"),
            # Issue comment → excluded
            make_comment(3, 1, bob, start + timedelta(hours=3),
                         type=CommentType.ISSUE, path="src/a.py"),
            # Pathless review → excluded
            make_comment(4, 1, bob, start + timedelta(hours=4),
                         type=CommentType.REVIEW, path=None),
            # Valid human review → COUNTED
            make_comment(5, 1, bob, start + timedelta(hours=5),
                         type=CommentType.REVIEW, path="src/b.py"),
        ]

        bundle = make_bundle(
            users=[alice, bob, bot_user],
            repositories=[repo],
            pull_requests=[pr],
            files=files,
            comments=comments,
        )
        ctx = make_context(bundle, "alice", start, start + timedelta(days=30))
        result = metric.run(ctx)

        assert result.details["reviewed_files"] == 1  # only src/b.py
        assert result.details["total_files"] == 2
        assert result.details["coverage_pct"] == 50.0
