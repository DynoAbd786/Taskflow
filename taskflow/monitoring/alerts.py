"""
Alerting and notification system for TaskFlow.
Handles alert rules, notifications, and escalation policies.
"""

import asyncio
import smtplib
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import httpx
import redis.asyncio as redis

from taskflow.utils.config import get_config


class AlertSeverity(str, Enum):
    """Alert severity levels."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AlertStatus(str, Enum):
    """Alert status."""

    FIRING = "firing"
    RESOLVED = "resolved"
    ACKNOWLEDGED = "acknowledged"
    SILENCED = "silenced"


@dataclass
class AlertRule:
    """Alert rule definition."""

    name: str
    description: str
    condition: str
    severity: AlertSeverity
    threshold: float
    duration: int = 60  # seconds
    labels: Dict[str, str] = field(default_factory=dict)
    annotations: Dict[str, str] = field(default_factory=dict)
    enabled: bool = True

    # Evaluation state
    last_evaluation: Optional[datetime] = None
    consecutive_violations: int = 0
    is_firing: bool = False


@dataclass
class Alert:
    """Active alert instance."""

    id: str
    rule_name: str
    status: AlertStatus
    severity: AlertSeverity
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    labels: Dict[str, str] = field(default_factory=dict)
    annotations: Dict[str, str] = field(default_factory=dict)

    fired_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    resolved_at: Optional[datetime] = None
    acknowledged_at: Optional[datetime] = None
    acknowledged_by: Optional[str] = None

    notification_count: int = 0
    last_notification: Optional[datetime] = None


class NotificationChannel:
    """Base notification channel."""

    async def send_notification(self, alert: Alert) -> bool:
        """Send notification for alert."""
        raise NotImplementedError


class EmailNotificationChannel(NotificationChannel):
    """Email notification channel."""

    def __init__(
        self,
        smtp_host: str,
        smtp_port: int,
        username: str,
        password: str,
        from_email: str,
        to_emails: List[str],
        use_tls: bool = True,
    ):
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.from_email = from_email
        self.to_emails = to_emails
        self.use_tls = use_tls

    async def send_notification(self, alert: Alert) -> bool:
        """Send email notification."""
        try:
            msg = MIMEMultipart()
            msg["From"] = self.from_email
            msg["To"] = ", ".join(self.to_emails)
            msg[
                "Subject"
            ] = f"TaskFlow Alert: {alert.rule_name} ({alert.severity.value.upper()})"

            body = self._format_email_body(alert)
            msg.attach(MIMEText(body, "html"))

            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                if self.use_tls:
                    server.starttls()
                server.login(self.username, self.password)
                server.send_message(msg)

            return True

        except Exception:
            return False

    def _format_email_body(self, alert: Alert) -> str:
        """Format email body."""
        return f"""
        <html>
        <body>
        <h2>TaskFlow Alert</h2>
        <p><strong>Alert:</strong> {alert.rule_name}</p>
        <p><strong>Severity:</strong> {alert.severity.value.upper()}</p>
        <p><strong>Status:</strong> {alert.status.value.upper()}</p>
        <p><strong>Message:</strong> {alert.message}</p>
        <p><strong>Fired At:</strong> {alert.fired_at.isoformat()}</p>
        
        <h3>Labels:</h3>
        <ul>
        {''.join(f'<li>{k}: {v}</li>' for k, v in alert.labels.items())}
        </ul>
        
        <h3>Details:</h3>
        <ul>
        {''.join(f'<li>{k}: {v}</li>' for k, v in alert.details.items())}
        </ul>
        </body>
        </html>
        """


class WebhookNotificationChannel(NotificationChannel):
    """Webhook notification channel."""

    def __init__(self, webhook_url: str, headers: Optional[Dict[str, str]] = None):
        self.webhook_url = webhook_url
        self.headers = headers or {}

    async def send_notification(self, alert: Alert) -> bool:
        """Send webhook notification."""
        try:
            payload = {
                "alert_id": alert.id,
                "rule_name": alert.rule_name,
                "status": alert.status.value,
                "severity": alert.severity.value,
                "message": alert.message,
                "fired_at": alert.fired_at.isoformat(),
                "labels": alert.labels,
                "details": alert.details,
            }

            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self.webhook_url, json=payload, headers=self.headers, timeout=30.0
                )
                return response.status_code < 400

        except Exception:
            return False


class SlackNotificationChannel(NotificationChannel):
    """Slack notification channel."""

    def __init__(self, webhook_url: str, channel: Optional[str] = None):
        self.webhook_url = webhook_url
        self.channel = channel

    async def send_notification(self, alert: Alert) -> bool:
        """Send Slack notification."""
        try:
            color_map = {
                AlertSeverity.LOW: "good",
                AlertSeverity.MEDIUM: "warning",
                AlertSeverity.HIGH: "danger",
                AlertSeverity.CRITICAL: "danger",
            }

            payload = {
                "username": "TaskFlow",
                "icon_emoji": ":warning:",
                "attachments": [
                    {
                        "color": color_map.get(alert.severity, "warning"),
                        "title": f"TaskFlow Alert: {alert.rule_name}",
                        "text": alert.message,
                        "fields": [
                            {
                                "title": "Severity",
                                "value": alert.severity.value.upper(),
                                "short": True,
                            },
                            {
                                "title": "Status",
                                "value": alert.status.value.upper(),
                                "short": True,
                            },
                            {
                                "title": "Fired At",
                                "value": alert.fired_at.isoformat(),
                                "short": False,
                            },
                        ],
                    }
                ],
            }

            if self.channel:
                payload["channel"] = self.channel

            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self.webhook_url, json=payload, timeout=30.0
                )
                return response.status_code < 400

        except Exception:
            return False


class AlertManager:
    """Central alert management system."""

    def __init__(self):
        self.config = get_config()
        self.rules: Dict[str, AlertRule] = {}
        self.active_alerts: Dict[str, Alert] = {}
        self.notification_channels: List[NotificationChannel] = []

        self._running = False
        self._shutdown_event = asyncio.Event()
        self._redis = None

        self._evaluation_lock = asyncio.Lock()

    async def start(self) -> None:
        """Start alert manager."""
        self._running = True

        try:
            self._redis = redis.from_url(self.config.redis.url)
        except Exception:
            pass

        await self._load_default_rules()

        evaluation_task = asyncio.create_task(self._evaluation_loop())
        notification_task = asyncio.create_task(self._notification_loop())

        await asyncio.gather(evaluation_task, notification_task, return_exceptions=True)

    async def stop(self) -> None:
        """Stop alert manager."""
        self._running = False
        self._shutdown_event.set()

        if self._redis:
            await self._redis.close()

    def add_notification_channel(self, channel: NotificationChannel) -> None:
        """Add notification channel."""
        self.notification_channels.append(channel)

    def add_rule(self, rule: AlertRule) -> None:
        """Add alert rule."""
        self.rules[rule.name] = rule

    def remove_rule(self, rule_name: str) -> None:
        """Remove alert rule."""
        self.rules.pop(rule_name, None)

    def enable_rule(self, rule_name: str) -> None:
        """Enable alert rule."""
        if rule_name in self.rules:
            self.rules[rule_name].enabled = True

    def disable_rule(self, rule_name: str) -> None:
        """Disable alert rule."""
        if rule_name in self.rules:
            self.rules[rule_name].enabled = False

    async def evaluate_metric(
        self, metric_name: str, value: float, labels: Dict[str, str] = None
    ) -> None:
        """Evaluate metric against alert rules."""
        async with self._evaluation_lock:
            for rule in self.rules.values():
                if not rule.enabled:
                    continue

                if await self._evaluate_rule(rule, metric_name, value, labels or {}):
                    await self._fire_alert(rule, metric_name, value, labels or {})
                else:
                    await self._resolve_alert(rule)

    async def _evaluate_rule(
        self, rule: AlertRule, metric_name: str, value: float, labels: Dict[str, str]
    ) -> bool:
        """Evaluate a single rule."""
        if metric_name not in rule.condition:
            return False

        threshold_exceeded = False

        if ">" in rule.condition:
            threshold_exceeded = value > rule.threshold
        elif "<" in rule.condition:
            threshold_exceeded = value < rule.threshold
        elif "=" in rule.condition:
            threshold_exceeded = abs(value - rule.threshold) < 0.001

        rule.last_evaluation = datetime.now(timezone.utc)

        if threshold_exceeded:
            rule.consecutive_violations += 1
        else:
            rule.consecutive_violations = 0

        required_violations = rule.duration // 60
        return rule.consecutive_violations >= required_violations

    async def _fire_alert(
        self, rule: AlertRule, metric_name: str, value: float, labels: Dict[str, str]
    ) -> None:
        """Fire an alert."""
        if rule.is_firing:
            return

        rule.is_firing = True

        alert_id = f"{rule.name}_{int(datetime.now().timestamp())}"

        alert = Alert(
            id=alert_id,
            rule_name=rule.name,
            status=AlertStatus.FIRING,
            severity=rule.severity,
            message=f"{rule.description} (current: {value}, threshold: {rule.threshold})",
            details={
                "metric_name": metric_name,
                "current_value": value,
                "threshold": rule.threshold,
                "consecutive_violations": rule.consecutive_violations,
            },
            labels={**rule.labels, **labels},
            annotations=rule.annotations,
        )

        self.active_alerts[alert_id] = alert

        await self._persist_alert(alert)
        await self._send_notifications(alert)

    async def _resolve_alert(self, rule: AlertRule) -> None:
        """Resolve alerts for a rule."""
        if not rule.is_firing:
            return

        rule.is_firing = False

        for alert in list(self.active_alerts.values()):
            if alert.rule_name == rule.name and alert.status == AlertStatus.FIRING:
                alert.status = AlertStatus.RESOLVED
                alert.resolved_at = datetime.now(timezone.utc)

                await self._persist_alert(alert)
                await self._send_notifications(alert)

    async def acknowledge_alert(self, alert_id: str, acknowledged_by: str) -> bool:
        """Acknowledge an alert."""
        if alert_id not in self.active_alerts:
            return False

        alert = self.active_alerts[alert_id]
        alert.status = AlertStatus.ACKNOWLEDGED
        alert.acknowledged_at = datetime.now(timezone.utc)
        alert.acknowledged_by = acknowledged_by

        await self._persist_alert(alert)
        return True

    async def silence_alert(self, alert_id: str, duration_minutes: int = 60) -> bool:
        """Silence an alert."""
        if alert_id not in self.active_alerts:
            return False

        alert = self.active_alerts[alert_id]
        alert.status = AlertStatus.SILENCED

        await self._persist_alert(alert)

        asyncio.create_task(
            self._unsilence_alert_after_delay(alert_id, duration_minutes)
        )
        return True

    async def _unsilence_alert_after_delay(
        self, alert_id: str, duration_minutes: int
    ) -> None:
        """Unsilence alert after delay."""
        await asyncio.sleep(duration_minutes * 60)

        if alert_id in self.active_alerts:
            alert = self.active_alerts[alert_id]
            if alert.status == AlertStatus.SILENCED:
                alert.status = AlertStatus.FIRING
                await self._persist_alert(alert)

    async def _evaluation_loop(self) -> None:
        """Alert evaluation loop."""
        while self._running and not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break

    async def _notification_loop(self) -> None:
        """Notification sending loop."""
        while self._running and not self._shutdown_event.is_set():
            try:
                await self._process_notifications()
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break

    async def _process_notifications(self) -> None:
        """Process pending notifications."""
        for alert in self.active_alerts.values():
            if alert.status == AlertStatus.FIRING and self._should_send_notification(
                alert
            ):
                await self._send_notifications(alert)

    def _should_send_notification(self, alert: Alert) -> bool:
        """Check if notification should be sent."""
        if alert.notification_count == 0:
            return True

        if not alert.last_notification:
            return True

        time_since_last = datetime.now(timezone.utc) - alert.last_notification

        escalation_intervals = [5, 15, 30, 60]  # minutes
        interval_index = min(
            alert.notification_count - 1, len(escalation_intervals) - 1
        )
        required_interval = timedelta(minutes=escalation_intervals[interval_index])

        return time_since_last >= required_interval

    async def _send_notifications(self, alert: Alert) -> None:
        """Send notifications for alert."""
        for channel in self.notification_channels:
            try:
                success = await channel.send_notification(alert)
                if success:
                    alert.notification_count += 1
                    alert.last_notification = datetime.now(timezone.utc)
            except Exception:
                pass

    async def _persist_alert(self, alert: Alert) -> None:
        """Persist alert to storage."""
        if self._redis:
            try:
                alert_data = {
                    "id": alert.id,
                    "rule_name": alert.rule_name,
                    "status": alert.status.value,
                    "severity": alert.severity.value,
                    "message": alert.message,
                    "fired_at": alert.fired_at.isoformat(),
                    "resolved_at": alert.resolved_at.isoformat()
                    if alert.resolved_at
                    else None,
                    "acknowledged_at": alert.acknowledged_at.isoformat()
                    if alert.acknowledged_at
                    else None,
                    "acknowledged_by": alert.acknowledged_by,
                    "labels": alert.labels,
                    "details": alert.details,
                }

                await self._redis.hset(
                    f"taskflow:alerts:{alert.id}", mapping=alert_data
                )
                await self._redis.expire(
                    f"taskflow:alerts:{alert.id}", 7 * 24 * 3600
                )  # 7 days
            except Exception:
                pass

    async def _load_default_rules(self) -> None:
        """Load default alert rules."""
        default_rules = [
            AlertRule(
                name="high_queue_depth",
                description="Queue depth is too high",
                condition="queue_size > threshold",
                severity=AlertSeverity.HIGH,
                threshold=1000,
                duration=300,
                labels={"component": "queue"},
            ),
            AlertRule(
                name="worker_failure_rate",
                description="Worker failure rate is high",
                condition="worker_failures > threshold",
                severity=AlertSeverity.MEDIUM,
                threshold=5,
                duration=300,
                labels={"component": "worker"},
            ),
            AlertRule(
                name="task_failure_rate",
                description="Task failure rate is high",
                condition="task_failures > threshold",
                severity=AlertSeverity.HIGH,
                threshold=0.1,  # 10%
                duration=300,
                labels={"component": "task"},
            ),
            AlertRule(
                name="system_cpu_high",
                description="System CPU usage is critically high",
                condition="cpu_percent > threshold",
                severity=AlertSeverity.CRITICAL,
                threshold=90.0,
                duration=300,
                labels={"component": "system"},
            ),
            AlertRule(
                name="system_memory_high",
                description="System memory usage is critically high",
                condition="memory_percent > threshold",
                severity=AlertSeverity.CRITICAL,
                threshold=90.0,
                duration=300,
                labels={"component": "system"},
            ),
        ]

        for rule in default_rules:
            self.add_rule(rule)

    def get_active_alerts(self) -> List[Alert]:
        """Get all active alerts."""
        return list(self.active_alerts.values())

    def get_alert_summary(self) -> Dict[str, Any]:
        """Get alert summary."""
        alerts = self.get_active_alerts()

        return {
            "total_alerts": len(alerts),
            "firing": len([a for a in alerts if a.status == AlertStatus.FIRING]),
            "acknowledged": len(
                [a for a in alerts if a.status == AlertStatus.ACKNOWLEDGED]
            ),
            "silenced": len([a for a in alerts if a.status == AlertStatus.SILENCED]),
            "by_severity": {
                severity.value: len([a for a in alerts if a.severity == severity])
                for severity in AlertSeverity
            },
        }


_alert_manager: Optional[AlertManager] = None


def get_alert_manager() -> AlertManager:
    """Get the global alert manager instance."""
    global _alert_manager
    if _alert_manager is None:
        _alert_manager = AlertManager()
    return _alert_manager
