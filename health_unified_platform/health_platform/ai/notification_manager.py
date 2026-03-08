"""Notification manager using ntfy.sh for push notifications.

Sends alerts when anomalies are detected, with priority levels
based on anomaly severity.
"""

from __future__ import annotations

import re
import urllib.request

from health_platform.ai.anomaly_detector import AnomalyReport
from health_platform.utils.logging_config import get_logger

logger = get_logger("notification_manager")

# ntfy.sh priority mapping
PRIORITY_MAP = {
    "anomaly": "urgent",  # z > 2.0
    "unusual": "high",  # z > 1.5
    "warning": "default",  # z > 1.0
    "constellation": "urgent",
    "degradation": "high",
}


class NotificationManager:
    """Send push notifications via ntfy.sh."""

    def __init__(self, topic: str | None = None):
        """Initialize with ntfy.sh topic.

        Args:
            topic: ntfy.sh topic name. If None, notifications are logged but not sent.

        Raises:
            ValueError: If topic contains invalid characters or exceeds 64 chars.
        """
        if topic is not None and not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", topic):
            raise ValueError(
                f"Invalid ntfy.sh topic: {topic!r}. "
                "Must be 1-64 characters of [a-zA-Z0-9_-]."
            )
        self.topic = topic
        self.base_url = "https://ntfy.sh"

    def notify_anomaly_report(self, report: AnomalyReport) -> int:
        """Send notifications for anomalies in a report.

        Returns number of notifications sent.
        """
        if not report.has_critical and not report.degradations:
            logger.debug("No critical anomalies -- skipping notifications")
            return 0

        count = 0

        # Send constellation alerts (highest priority)
        for c in report.constellations:
            self._send(
                title=f"Health Alert: {c.pattern_type.replace('_', ' ').title()}",
                message=c.description,
                priority=PRIORITY_MAP["constellation"],
                tags=["warning", "health"],
            )
            count += 1

        # Send critical anomalies
        critical = [a for a in report.anomalies if a.severity == "anomaly"]
        if critical:
            descriptions = [a.description for a in critical[:5]]
            self._send(
                title=f"Health: {len(critical)} Anomalies Detected",
                message="\n".join(descriptions),
                priority=PRIORITY_MAP["anomaly"],
                tags=["rotating_light", "health"],
            )
            count += 1

        # Send degradation warnings
        for d in report.degradations:
            self._send(
                title=f"Health Trend: {d.metric}",
                message=d.description,
                priority=PRIORITY_MAP["degradation"],
                tags=["chart_with_downwards_trend"],
            )
            count += 1

        logger.info("Sent %d notifications", count)
        return count

    def _send(
        self,
        title: str,
        message: str,
        priority: str = "default",
        tags: list[str] | None = None,
    ) -> bool:
        """Send a single notification via ntfy.sh."""
        if not self.topic:
            logger.info("Notification (dry run): %s -- %s", title, message)
            return False

        url = f"{self.base_url}/{self.topic}"
        headers = {
            "Title": title,
            "Priority": priority,
        }
        if tags:
            headers["Tags"] = ",".join(tags)

        try:
            req = urllib.request.Request(
                url,
                data=message.encode("utf-8"),
                headers=headers,
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status == 200:
                    logger.debug("Notification sent: %s", title)
                    return True
                logger.warning("ntfy.sh returned status %d", resp.status)
                return False
        except Exception as exc:
            logger.error("Failed to send notification: %s", exc)
            return False
