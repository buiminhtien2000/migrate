# notification_service.py
from typing import Dict, List, Union
from abc import ABC, abstractmethod
from datetime import datetime
import aiohttp # type: ignore
import json

class SendNotification(ABC):
    @abstractmethod
    async def send(self, notification: Dict) -> bool:
        """Send notification abstract method."""
        pass

class Notification(SendNotification):
    def __init__(self, config: 'Config', logger: 'CustomLogger'): # type: ignore
        self.config = config
        self.logger = logger
        self.notifications_queue: List[Dict] = []
        self.session = None

    async def _ensure_session(self):
        """Ensure aiohttp session exists"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
            
    async def send(self, notification: Union[str, Dict, List]) -> bool:
        """
        Send notification through configured channels.
        
        Args:
            notification: Content to send (string, dict or list)
                    
        Returns:
            bool: True if sent successfully
        """
        try:
            # Convert to dict first
            enriched_notification = self._enrich_notification(notification)
            
            # Convert to json string before sending
            try:
                message = json.dumps(enriched_notification, ensure_ascii=False, indent=3)
            except Exception as e:
                self.logger.error(f"Failed to serialize notification: {str(e)}")
                message = str(enriched_notification)

            # Send through configured channels
            await self._send_to_configured_channels(message)
            return True
                
        except Exception as e:
            self.logger.error(f"Failed to send notification: {str(e)}")
            self.notifications_queue.append(notification)
            return False

    def _enrich_notification(self, notification: Union[str, Dict, List]) -> Dict:
        """
        Add additional context to notification.
        
        Args:
            notification: Original notification (string, dict or list)
        Returns:
            Dict: Enriched notification with standard fields
        """
        if isinstance(notification, str):
            notification = {'message': notification}
            
        return {
            **notification,
            'timestamp': datetime.now().isoformat(),
            'severity': self._determine_severity(notification)
        }

    def _determine_severity(self, notification: Dict) -> str:
        """Determine notification severity based on error type."""
        error_type = notification.get('error_type', '')
        severity_mapping = {
            'CriticalError': 'high',
            'ProcessingError': 'medium',
            'ValidationError': 'low'
        }
        return severity_mapping.get(error_type, 'medium')

    async def _send_to_configured_channels(self, notification: str) -> None:
        """Send to all configured notification channels."""
        channels = self.config.NOTIFICATION_CHANNELS
        for channel in channels:
            await self._send_to_channel(channel, notification)

    async def _send_to_channel(self, channel: str, notification: str) -> None:
        """Send to specific notification channel."""
        channel_handlers = {
            'email': self._send_email_notification,
            'slack': self._send_slack_notification,
            'teams': self._send_teams_notification
        }
        handler = channel_handlers.get(channel)
        if handler:
            await handler(notification)

    async def _send_email_notification(self, notification: str) -> None:
        # Implement email notification logic
        pass

    async def _send_slack_notification(self, notification: str) -> None:

        await self._ensure_session()
        
        try:
            payload = {"text": notification}
            slack_url = self.config.from_file.get('SLACK_URL')

            if slack_url is None:
                return False
            
            async with self.session.post(slack_url, json=payload) as response:
                return response.status == 200
        except Exception as e:
            print(f"Error: {str(e)}")
            return False

    async def _send_teams_notification(self, notification: str) -> None:
        # Implement Teams notification logic
        pass

    async def process_queued_notifications(self) -> None:
        """Process any queued notifications that failed to send."""
        while self.notifications_queue:
            notification = self.notifications_queue.pop(0)
            await self.send(notification)

    async def close(self):
        """Close aiohttp session"""
        if self.session and not self.session.closed:
            await self.session.close()        