from datetime import timedelta

from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric

# Events within this window by the same side are collapsed into one turn.
# This prevents interleaved thread replies from inflating the count.
_TURN_GAP = timedelta(hours=1)


class DiscussionCycles(Metric):
    """
    Count alternating-person comment exchanges on the user's merged PRs.

    A "cycle" is a change of speaker between author and reviewer(s) in the
    comment stream (reviews + review comments + issue comments).

    To handle GitHub's nested review-comment threads — where interleaved
    replies across multiple threads would inflate a naive alternation count —
    events are collapsed into **turns**: a contiguous block of same-side
    activity with no gap longer than ``_TURN_GAP``.  Each turn switch
    (author-turn → reviewer-turn or vice versa) counts as one cycle.
    """

    @property
    def slug(self) -> str:
        return "discussion_cycles"

    @property
    def name(self) -> str:
        return "Discussion Cycles"

    @property
    def description(self) -> str:
        return "Avg alternating-person comment exchanges per merged PR."

    @property
    def category(self) -> str:
        return "process_discipline"

    @property
    def frameworks(self) -> list[str]:
        return ["DevRank"]

    def run(self, context: MetricContext) -> MetricResult:
        period_days = (
            (context.end_date - context.start_date).total_seconds() / 86400
            if context.start_date and context.end_date
            else 0
        )

        prs = context.ledger.get_prs_for_user(
            context.user_login, context.start_date, context.end_date,
        )
        merged = [pr for pr in prs if pr.merged]

        per_pr: list[dict] = []
        counts: list[int] = []

        for pr in merged:
            cycles = self._count_cycles(pr, context)
            per_pr.append({"number": pr.number, "cycles": cycles})
            counts.append(cycles)

        avg = sum(counts) / len(counts) if counts else 0.0
        summary = f"{len(merged)} merged PRs; avg discussion cycles: {avg:.2f}"
        details: dict[str, object] = {
            "merged_prs": len(merged),
            "average_cycles": round(avg, 2),
            "per_pr": per_pr,
            "period_days": period_days,
        }
        if not counts or (period_days < 14 and len(counts) < 3):
            details["no_data"] = True
        return MetricResult(metric_slug=self.slug, summary=summary, details=details)

    @staticmethod
    def _count_cycles(pr, context) -> int:
        """Count turn-based speaker alternations in the comment timeline.

        Events are first sorted chronologically, then collapsed into turns:
        consecutive same-side (author vs reviewer) events within ``_TURN_GAP``
        form a single turn.  Only transitions between turns count as cycles.

        This handles GitHub's nested review-comment threads where a reviewer
        posts N inline comments in one review and the author replies to each
        one individually — that's 1 review turn + 1 author turn = 1 cycle,
        not 2N alternations.
        """
        author = pr.user.login

        # Build a time-sorted stream of (timestamp, is_author) tuples
        events: list[tuple] = []

        for r in context.ledger.get_reviews_for_pr(pr.number):
            if r.user.is_bot:
                continue
            events.append((r.submitted_at, r.user.login == author))

        for c in context.ledger.get_comments_for_pr(pr.number):
            if c.user.is_bot:
                continue
            events.append((c.created_at, c.user.login == author))

        if len(events) < 2:
            return 0

        events.sort(key=lambda e: e[0])

        # Collapse into turns: a turn is a contiguous block of same-side
        # events where no gap exceeds _TURN_GAP.
        cycles = 0
        turn_is_author = events[0][1]
        turn_end = events[0][0]

        for ts, is_author in events[1:]:
            if is_author == turn_is_author and (ts - turn_end) <= _TURN_GAP:
                # Same side, within gap — extend the current turn
                turn_end = ts
            elif is_author == turn_is_author:
                # Same side but gap too large — new turn by same side (no cycle)
                turn_end = ts
            else:
                # Side changed — new turn, count a cycle
                cycles += 1
                turn_is_author = is_author
                turn_end = ts

        return cycles
