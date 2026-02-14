from datetime import datetime
from zoneinfo import ZoneInfo

from impact.domain.models import MetricContext, MetricResult
from impact.metrics.base import Metric


class OffHoursActivityRate(Metric):
    @property
    def slug(self) -> str:
        return "off_hours_activity_rate"

    @property
    def name(self) -> str:
        return "Off-Hours Activity Rate"

    @property
    def description(self) -> str:
        return "% activity (commits/PRs) in weekends/late nights (sustainability/burnout signal; user TZ aware)."

    @property
    def category(self) -> str:
        return "risk_sustainability"

    def run(self, context: MetricContext) -> MetricResult:
        user = context.user_login
        # User TZ from bundle (manifest); data UTC
        tz_str = getattr(context.ledger.bundle, "user_timezone", None)
        if not tz_str:
            # Without timezone data, off-hours classification is unreliable
            # (e.g., a UTC+8 engineer's morning commits would be flagged as night activity)
            period_days = (context.end_date - context.start_date).total_seconds() / 86400 if context.start_date and context.end_date else 0
            return MetricResult(
                metric_slug=self.slug,
                summary="Off-hours rate: N/A (timezone data not available; cannot classify off-hours without user timezone).",
                details={
                    "off_hours_rate": 0.0,
                    "off_count": 0,
                    "total_activities": 0,
                    "weekend_count": 0,
                    "night_count": 0,
                    "user_timezone": None,
                    "off_activities": [],
                    "period_days": period_days,
                    "no_data": True,
                    "no_data_reason": "timezone data not available in bundle",
                },
            )
        user_tz = ZoneInfo(tz_str)
        # Off-hours def (local): weekend or 22:00-06:00
        off_count = 0
        total = 0
        off_activities = []
        weekend_count = 0
        night_count = 0
        # Commits (primary activity)
        commits = context.ledger.get_commits_for_user(user, context.start_date, context.end_date)
        for c in commits:
            total += 1
            local = c.date.astimezone(user_tz)
            is_weekend = local.weekday() >= 5
            is_night = local.hour >= 22 or local.hour < 6
            if is_weekend or is_night:
                off_count += 1
                if is_weekend:
                    weekend_count += 1
                if is_night:
                    night_count += 1
                off_activities.append({
                    "type": "commit",
                    "sha": c.sha,
                    "local_time": local.isoformat(),
                    "weekend": is_weekend,
                    "night": is_night,
                })
        # PRs created (add if no commits)
        prs = context.ledger.get_prs_for_user(user, context.start_date, context.end_date)
        for pr in prs:
            if pr.number not in {c.pull_request_number for c in commits if c.pull_request_number}:  # avoid dup
                total += 1
                local = pr.created_at.astimezone(user_tz)
                is_weekend = local.weekday() >= 5
                is_night = local.hour >= 22 or local.hour < 6
                if is_weekend or is_night:
                    off_count += 1
                    if is_weekend:
                        weekend_count += 1
                    if is_night:
                        night_count += 1
                    off_activities.append({
                        "type": "pr_create",
                        "number": pr.number,
                        "local_time": local.isoformat(),
                        "weekend": is_weekend,
                        "night": is_night,
                    })
        rate = (off_count / total * 100) if total else 0.0
        summary = f"Off-hours rate: {rate:.1f}% ({off_count}/{total} activities; {weekend_count} weekend, {night_count} night; TZ: {tz_str})."
        details = {
            "off_hours_rate": rate,
            "off_count": off_count,
            "total_activities": total,
            "weekend_count": weekend_count,
            "night_count": night_count,
            "user_timezone": tz_str,
            "off_activities": off_activities[:10],  # cap for report
        }
        period_days = (context.end_date - context.start_date).total_seconds() / 86400 if context.start_date and context.end_date else 0
        details["period_days"] = period_days
        if total == 0 or (period_days < 14 and total < 5):
            details["no_data"] = True
        return MetricResult(
            metric_slug=self.slug,
            summary=summary,
            details=details,
        )
