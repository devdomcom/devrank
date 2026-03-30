"""
Tests for AI Phantom Ownership metric and the is_bot_user() utility.

Covers:
  - is_bot_user() with all three detection layers
  - _compute_human_review_depth() bot exclusion
  - _classify_files() phantom vs reviewed classification
  - AIPhantomOwnership.run() end-to-end scenarios:
      * No PRs / no AI PRs -> no_data
      * AI PR with zero human reviews -> 100% phantom
      * AI PR with full human inline review -> 0% phantom
      * Mixed: some files reviewed, some not
      * Bot-only reviews correctly excluded
      * Generated files correctly excluded
      * Copilot timeline event detection
"""

from datetime import UTC, datetime, timedelta

import pytest

from impact.domain.models import (
    CanonicalBundle,
    CommentRecord,
    CommentType,
    FileRecord,
    MetricContext,
    ReviewRecord,
    ReviewState,
    TimelineEvent,
    User,
    UserType,
)
from impact.metrics.plugins.authored.ai_phantom_ownership import (
    AIPhantomOwnership,
    _classify_files,
    _compute_human_review_depth,
    _has_copilot_timeline,
)
from impact.metrics.utils import is_bot_user
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_commit,
    make_context,
    make_file,
    make_pr,
    make_repo,
    make_review,
    make_user,
)


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

def _bot_user(login: str = "copilot-reviewer[bot]", node_id: str = "BOT_abc123") -> User:
    return User(id=900, login=login, type=UserType.BOT, node_id=node_id, is_bot=True)


def _copilot_user() -> User:
    """Copilot inline commenter: type=Bot but NO [bot] suffix."""
    return User(id=901, login="Copilot", type=UserType.BOT, node_id="BOT_kgDOCnlnWA", is_bot=True)


def _human_reviewer(login: str = "reviewer1", uid: int = 200) -> User:
    return User(id=uid, login=login, type=UserType.USER)


def _make_timeline_event(
    pr_number: int, event: str, actor: User, hours_offset: float = 0,
) -> TimelineEvent:
    return TimelineEvent(
        id=hash((pr_number, event, hours_offset)) % 10**9,
        event=event,
        actor=actor,
        created_at=DEFAULT_START + timedelta(hours=hours_offset),
        pull_request_number=pr_number,
    )


def _make_inline_comment(
    cid: int, pr_number: int, user: User, path: str, hours_offset: float = 1,
) -> CommentRecord:
    return CommentRecord(
        id=cid,
        user=user,
        body="Review comment",
        created_at=DEFAULT_START + timedelta(hours=hours_offset),
        type=CommentType.REVIEW,
        pull_request_number=pr_number,
        path=path,
        position=1,
    )


# ===========================================================================
# is_bot_user() tests
# ===========================================================================

class TestIsBotUser:
    """Test canonical ``is_bot`` flag read by ``is_bot_user()``.

    The three-layer GitHub-specific detection logic (type/suffix/node_id)
    now lives in ``GitHubAdapter._is_github_bot()`` which sets the
    canonical ``is_bot`` field.  ``is_bot_user()`` simply reads it (§7.1).
    """

    def test_bot_flag_true(self):
        """Canonical is_bot=True → detected as bot."""
        u = User(id=1, login="somebot", type=UserType.BOT, is_bot=True)
        assert is_bot_user(u) is True

    def test_bot_flag_false(self):
        """Canonical is_bot=False → not a bot."""
        u = User(id=2, login="alice", type=UserType.USER, is_bot=False)
        assert is_bot_user(u) is False

    def test_default_flag_is_false(self):
        """is_bot defaults to False (backward-compatible for old data)."""
        u = User(id=3, login="alice", type=UserType.USER)
        assert is_bot_user(u) is False

    def test_suffix_bot_with_flag(self):
        """Bot with [bot] suffix — adapter sets is_bot=True."""
        u = User(id=4, login="dependabot[bot]", type=UserType.USER, is_bot=True)
        assert is_bot_user(u) is True

    def test_copilot_no_suffix(self):
        """Copilot: type=Bot, no [bot] suffix — adapter sets is_bot=True."""
        u = _copilot_user()
        assert is_bot_user(u) is True

    def test_human_with_bot_substring(self):
        """gabotorresruiz contains 'bot' but is_bot=False."""
        u = User(id=5, login="gabotorresruiz", type=UserType.USER)
        assert is_bot_user(u) is False

    def test_none_attributes(self):
        """Gracefully handles objects missing the is_bot attribute."""
        class Bare:
            pass
        assert is_bot_user(Bare()) is False


# ===========================================================================
# _compute_human_review_depth() tests
# ===========================================================================

class TestHumanReviewDepth:
    def _build(self, reviews=None, comments=None, files=None, timeline=None):
        alice = make_user(id=1, login="alice")
        repo = make_repo()
        pr = make_pr(1, alice, repo, merged_at=DEFAULT_START + timedelta(hours=5))
        commit = make_commit("aaa", alice, DEFAULT_START + timedelta(hours=1), 1,
                             message="feat: add feature (copilot)")
        bundle = make_bundle(
            users=[alice],
            repositories=[repo],
            pull_requests=[pr],
            commits=[commit],
            reviews=reviews or [],
            comments=comments or [],
            files=files or [],
            timeline=timeline or [],
        )
        from impact.ledger.ledger import Ledger
        return Ledger(bundle)

    def test_excludes_bots(self):
        bot = _bot_user()
        human = _human_reviewer()
        reviews = [
            make_review(10, 1, bot, DEFAULT_START + timedelta(hours=2), ReviewState.COMMENTED),
            make_review(11, 1, human, DEFAULT_START + timedelta(hours=3), ReviewState.APPROVED),
        ]
        ledger = self._build(reviews=reviews)
        depth = _compute_human_review_depth(ledger, 1, "alice")
        assert depth["human_reviews"] == 1
        assert depth["human_approvals"] == 1
        assert depth["bot_reviews"] == 1

    def test_excludes_copilot_no_suffix(self):
        """Copilot (no [bot] suffix) must be excluded."""
        copilot = _copilot_user()
        human = _human_reviewer()
        comments = [
            _make_inline_comment(100, 1, copilot, "src/main.py"),
            _make_inline_comment(101, 1, human, "src/main.py", hours_offset=2),
        ]
        ledger = self._build(comments=comments)
        depth = _compute_human_review_depth(ledger, 1, "alice")
        assert depth["human_inline_comments"] == 1
        assert "src/main.py" in depth["human_inline_files"]

    def test_excludes_author_reviews(self):
        alice = make_user(id=1, login="alice")
        reviews = [
            make_review(10, 1, alice, DEFAULT_START + timedelta(hours=2), ReviewState.APPROVED),
        ]
        ledger = self._build(reviews=reviews)
        depth = _compute_human_review_depth(ledger, 1, "alice")
        assert depth["human_reviews"] == 0

    def test_zero_depth_when_only_bots(self):
        bot1 = _bot_user("bito-code-review[bot]")
        bot2 = _bot_user("codeant-ai[bot]", node_id="BOT_xyz")
        reviews = [
            make_review(10, 1, bot1, DEFAULT_START + timedelta(hours=2)),
            make_review(11, 1, bot2, DEFAULT_START + timedelta(hours=3)),
        ]
        ledger = self._build(reviews=reviews)
        depth = _compute_human_review_depth(ledger, 1, "alice")
        assert depth["human_reviews"] == 0
        assert depth["depth_score"] == 0
        assert depth["bot_reviews"] == 2


# ===========================================================================
# _classify_files() tests
# ===========================================================================

class TestClassifyFiles:
    def test_all_phantom_when_no_human_comments(self):
        files = [
            make_file("sha1", "src/app.py", additions=50, deletions=10, pr_number=1),
            make_file("sha2", "src/util.py", additions=30, deletions=5, pr_number=1),
        ]
        phantom, reviewed = _classify_files(files, ai_confidence=80.0, human_inline_files=set())
        assert len(phantom) == 2
        assert len(reviewed) == 0

    def test_reviewed_when_human_comments_present(self):
        files = [
            make_file("sha1", "src/app.py", additions=50, pr_number=1),
            make_file("sha2", "src/util.py", additions=30, pr_number=1),
        ]
        phantom, reviewed = _classify_files(
            files, ai_confidence=80.0, human_inline_files={"src/app.py"},
        )
        assert len(phantom) == 1
        assert phantom[0]["file"] == "src/util.py"
        assert len(reviewed) == 1
        assert reviewed[0]["file"] == "src/app.py"

    def test_low_confidence_means_no_phantom(self):
        files = [make_file("sha1", "src/app.py", additions=50, pr_number=1)]
        phantom, reviewed = _classify_files(files, ai_confidence=30.0, human_inline_files=set())
        assert len(phantom) == 0
        assert len(reviewed) == 1

    def test_generated_files_excluded(self):
        files = [
            make_file("sha1", "package-lock.json", additions=1000, pr_number=1),
            make_file("sha2", "src/real.py", additions=50, pr_number=1),
        ]
        phantom, reviewed = _classify_files(files, ai_confidence=80.0, human_inline_files=set())
        # package-lock.json is a generated file, should be excluded entirely
        assert len(phantom) + len(reviewed) <= 2
        assert all(f["file"] != "package-lock.json" for f in phantom + reviewed)


# ===========================================================================
# _has_copilot_timeline() tests
# ===========================================================================

class TestCopilotTimeline:
    def test_detects_copilot_events(self):
        alice = make_user()
        repo = make_repo()
        pr = make_pr(1, alice, repo)
        timeline = [
            _make_timeline_event(1, "copilot_work_started", alice, 0),
            _make_timeline_event(1, "copilot_work_finished", alice, 0.5),
        ]
        bundle = make_bundle(
            pull_requests=[pr], commits=[], timeline=timeline,
            users=[alice], repositories=[repo],
        )
        from impact.ledger.ledger import Ledger
        ledger = Ledger(bundle)
        assert _has_copilot_timeline(ledger, 1) is True

    def test_no_copilot_events(self):
        alice = make_user()
        repo = make_repo()
        pr = make_pr(1, alice, repo)
        timeline = [
            _make_timeline_event(1, "labeled", alice, 0),
        ]
        bundle = make_bundle(
            pull_requests=[pr], commits=[], timeline=timeline,
            users=[alice], repositories=[repo],
        )
        from impact.ledger.ledger import Ledger
        ledger = Ledger(bundle)
        assert _has_copilot_timeline(ledger, 1) is False


# ===========================================================================
# AIPhantomOwnership.run() end-to-end tests
# ===========================================================================

class TestAIPhantomOwnershipMetric:
    def _run_metric(self, bundle, user_login="alice", days=30):
        ctx = make_context(
            bundle, user_login,
            start_date=DEFAULT_START,
            end_date=DEFAULT_START + timedelta(days=days),
        )
        metric = AIPhantomOwnership()
        return metric.run(ctx)

    def test_no_prs_returns_no_data(self):
        bundle = make_bundle()
        result = self._run_metric(bundle)
        assert result.details["no_data"] is True
        assert result.details["phantom_rate"] == 0.0

    def test_no_ai_prs_returns_no_data(self):
        """PRs exist but none are AI-assisted."""
        alice = make_user()
        repo = make_repo()
        pr = make_pr(1, alice, repo, merged_at=DEFAULT_START + timedelta(hours=5))
        commit = make_commit("aaa", alice, DEFAULT_START + timedelta(hours=1), 1,
                             message="feat: regular human commit")
        files = [make_file("sha1", "src/app.py", additions=50, pr_number=1)]
        bundle = make_bundle(
            users=[alice], repositories=[repo],
            pull_requests=[pr], commits=[commit], files=files,
        )
        result = self._run_metric(bundle)
        assert result.details["no_data"] is True
        assert result.details["ai_pr_count"] == 0

    def test_ai_pr_zero_human_review_100pct_phantom(self):
        """AI PR with only bot reviews -> 100% phantom ownership."""
        alice = make_user()
        repo = make_repo()
        bot = _bot_user()
        pr = make_pr(1, alice, repo, merged_at=DEFAULT_START + timedelta(hours=5),
                     additions=100, deletions=10)
        # AI signal: commit message mentions copilot
        commit = make_commit("aaa", alice, DEFAULT_START + timedelta(hours=1), 1,
                             message="feat: add feature\n\nCo-authored-by: GitHub Copilot")
        # Only bot review, no human review
        reviews = [
            make_review(10, 1, bot, DEFAULT_START + timedelta(hours=2), ReviewState.COMMENTED),
        ]
        files = [
            make_file("sha1", "src/app.py", additions=50, deletions=5, pr_number=1),
            make_file("sha2", "src/util.py", additions=30, deletions=3, pr_number=1),
        ]
        bundle = make_bundle(
            users=[alice, bot], repositories=[repo],
            pull_requests=[pr], commits=[commit],
            reviews=reviews, files=files,
        )
        result = self._run_metric(bundle)
        assert result.details["phantom_rate"] == 100.0
        assert result.details["phantom_file_count"] == 2
        assert result.details["total_ai_files"] == 2
        assert result.details["ai_pr_count"] == 1

    def test_ai_pr_full_human_review_0pct_phantom(self):
        """AI PR with human inline review on all files -> 0% phantom."""
        alice = make_user()
        repo = make_repo()
        reviewer = _human_reviewer()
        pr = make_pr(1, alice, repo, merged_at=DEFAULT_START + timedelta(hours=5))
        commit = make_commit("aaa", alice, DEFAULT_START + timedelta(hours=1), 1,
                             message="feat: add feature\n\nCo-authored-by: GitHub Copilot")
        reviews = [
            make_review(10, 1, reviewer, DEFAULT_START + timedelta(hours=2), ReviewState.APPROVED),
        ]
        # Human inline comments on both files
        comments = [
            _make_inline_comment(100, 1, reviewer, "src/app.py", hours_offset=2),
            _make_inline_comment(101, 1, reviewer, "src/util.py", hours_offset=2.5),
        ]
        files = [
            make_file("sha1", "src/app.py", additions=50, pr_number=1),
            make_file("sha2", "src/util.py", additions=30, pr_number=1),
        ]
        bundle = make_bundle(
            users=[alice, reviewer], repositories=[repo],
            pull_requests=[pr], commits=[commit],
            reviews=reviews, comments=comments, files=files,
        )
        result = self._run_metric(bundle)
        assert result.details["phantom_rate"] == 0.0
        assert result.details["phantom_file_count"] == 0

    def test_mixed_review_partial_phantom(self):
        """AI PR: 1 file reviewed by human, 2 files not -> 66.7% phantom."""
        alice = make_user()
        repo = make_repo()
        reviewer = _human_reviewer()
        bot = _bot_user()
        pr = make_pr(1, alice, repo, merged_at=DEFAULT_START + timedelta(hours=5))
        commit = make_commit("aaa", alice, DEFAULT_START + timedelta(hours=1), 1,
                             message="feat: add feature\n\nGenerated-with: cursor")
        reviews = [
            make_review(10, 1, reviewer, DEFAULT_START + timedelta(hours=2), ReviewState.APPROVED),
            make_review(11, 1, bot, DEFAULT_START + timedelta(hours=2), ReviewState.COMMENTED),
        ]
        # Human reviewed only app.py
        comments = [
            _make_inline_comment(100, 1, reviewer, "src/app.py", hours_offset=2),
            # Bot comments don't count
            _make_inline_comment(101, 1, bot, "src/util.py", hours_offset=2),
            _make_inline_comment(102, 1, bot, "src/config.py", hours_offset=2),
        ]
        files = [
            make_file("sha1", "src/app.py", additions=50, pr_number=1),
            make_file("sha2", "src/util.py", additions=30, pr_number=1),
            make_file("sha3", "src/config.py", additions=20, pr_number=1),
        ]
        bundle = make_bundle(
            users=[alice, reviewer, bot], repositories=[repo],
            pull_requests=[pr], commits=[commit],
            reviews=reviews, comments=comments, files=files,
        )
        result = self._run_metric(bundle)
        # 2 of 3 files are phantom (util.py and config.py)
        assert result.details["phantom_file_count"] == 2
        assert result.details["total_ai_files"] == 3
        assert abs(result.details["phantom_rate"] - 66.7) < 0.1

    def test_copilot_timeline_boosts_detection(self):
        """PR with copilot_work_started event is detected as AI even without keywords."""
        alice = make_user()
        repo = make_repo()
        pr = make_pr(1, alice, repo, merged_at=DEFAULT_START + timedelta(hours=5))
        commit = make_commit("aaa", alice, DEFAULT_START + timedelta(hours=1), 1,
                             message="feat: add dashboard widget")  # no AI keyword
        timeline = [
            _make_timeline_event(1, "copilot_work_started", alice, 0),
            _make_timeline_event(1, "copilot_work_finished", alice, 0.5),
        ]
        files = [
            make_file("sha1", "src/widget.py", additions=80, pr_number=1),
        ]
        bundle = make_bundle(
            users=[alice], repositories=[repo],
            pull_requests=[pr], commits=[commit],
            files=files, timeline=timeline,
        )
        result = self._run_metric(bundle)
        assert result.details["ai_pr_count"] == 1
        assert result.details["phantom_file_count"] == 1
        # Per-PR confidence should be >= 70 due to timeline boost
        assert result.details["per_pr"][0]["ai_confidence"] >= 70.0

    def test_per_pr_details_populated(self):
        """per_pr entries contain expected keys."""
        alice = make_user()
        repo = make_repo()
        reviewer = _human_reviewer()
        pr = make_pr(1, alice, repo, merged_at=DEFAULT_START + timedelta(hours=5))
        commit = make_commit("aaa", alice, DEFAULT_START + timedelta(hours=1), 1,
                             message="feat: copilot generated code")
        reviews = [
            make_review(10, 1, reviewer, DEFAULT_START + timedelta(hours=2), ReviewState.APPROVED),
        ]
        files = [make_file("sha1", "src/app.py", additions=50, pr_number=1)]
        bundle = make_bundle(
            users=[alice, reviewer], repositories=[repo],
            pull_requests=[pr], commits=[commit],
            reviews=reviews, files=files,
        )
        result = self._run_metric(bundle)
        assert len(result.details["per_pr"]) == 1
        entry = result.details["per_pr"][0]
        assert "number" in entry
        assert "ai_confidence" in entry
        assert "human_reviews" in entry
        assert "depth_score" in entry
        assert "phantom_files" in entry

    def test_metric_properties(self):
        m = AIPhantomOwnership()
        assert m.slug == "ai_phantom_ownership"
        assert m.name == "AI Phantom Ownership"
        assert m.category == "contextual"
        assert "DevRank" in m.frameworks
        assert m.signal_type == "authored"
