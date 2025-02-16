import os
import re
import schedule
import time
import json
from datetime import datetime, timedelta
from slack_sdk import WebClient

from config import SLACK_BOT_TOKEN

# Constants
RESTART_KEYWORDS = re.compile(r"\b(reboot|restart)\b", re.IGNORECASE)
SERVICE_KEYWORDS = re.compile(r"\b(\*?ecn\*?/mm\*?|ecn|mm|market maker|price-aggregator|aggregator|market driver|md|risk manager|manager|MDDRIVER)\b", re.IGNORECASE)

class SlackClient:
    """Handles communication with Slack API."""
    def __init__(self, token: str):
        self.client = WebClient(token=token)

    def fetch_messages(self, channel_id: str, date: str):
        """Fetches messages for a specific day."""
        messages = []
        try:
            start_time = datetime.strptime(date, "%Y-%m-%d").replace(hour=0, minute=0, second=0)
            end_time = start_time + timedelta(days=1)
            response = self.client.conversations_history(
                channel=channel_id,
                oldest=start_time.timestamp(),
                latest=end_time.timestamp()
            )
            messages = response.get("messages", [])
        except Exception as e:
            print(f"Error fetching messages: {e}")
        return messages

    def send_message(self, channel_id: str, text: str, blocks=None):
        """Sends a message to Slack."""
        try:
            response = self.client.chat_postMessage(channel=channel_id, text=text, blocks=blocks)
            print(f"Message sent: {response['ts']}")
        except Exception as e:
            print(f"Error sending message: {e}")

    def send_alert(self, channel_id: str, count: int):
        """Sends an alert if restart limit is exceeded."""
        alert_message = f" :red_circle: Alert! Restart requests exceeded limit: {count} :red_circle:"
        self.send_message(channel_id, alert_message)


class RestartAnalyzer:
    """Extracts and analyzes restart requests."""
    def __init__(self, slack_client: SlackClient):
        self.slack_client = slack_client

    def extract_restart_requests(self, messages):
        """Extracts messages related to restarts."""
        return [msg['text'] for msg in messages if RESTART_KEYWORDS.search(msg.get('text', '')) and SERVICE_KEYWORDS.search(msg.get('text', ''))]

    def extract_services(self, restart_requests):
        """Extracts service names from restart requests."""
        service_dict = {}
        for request in restart_requests:
            match = SERVICE_KEYWORDS.search(request)
            if not match:
                continue
            service_name = match.group()
            details_pattern = re.compile(fr"{re.escape(service_name)}:\s*(.+?)(?:\n|$)", re.IGNORECASE)
            result = details_pattern.search(request)
            if result:
                details = result.group(1).replace('*', '').replace('and', ',')
                service_dict[service_name] = service_dict.get(service_name, "") + details + ","

        # Cleaning up results
        for key in service_dict:
            service_dict[key] = [item.strip() for item in service_dict[key].split(',') if item]
        return service_dict

    def count_restarts(self, channel_id, date):
        """Counts restart requests and triggers an alert if necessary."""
        messages = self.slack_client.fetch_messages(channel_id, date)
        restart_requests = self.extract_restart_requests(messages)
        services_names = self.extract_services(restart_requests)
        restart_num = sum(len(services) * (2 if key == 'ecn/mm' else 1) + sum(1 for s in services if 'REF' in s) for key, services in services_names.items())

        # Send alert if restart count exceeds limit
        if restart_num > 20:
            self.slack_client.send_alert(ALERT_CHANNEL_ID, restart_num)

        return restart_num


class RestartScheduler:
    """Handles scheduled tasks."""
    def __init__(self, slack_client: SlackClient, restart_analyzer: RestartAnalyzer):
        self.slack_client = slack_client
        self.restart_analyzer = restart_analyzer

    def daily_check(self):
        """Performs a daily restart analysis and sends a summary."""
        date = datetime.now().strftime("%Y-%m-%d")
        messages = self.slack_client.fetch_messages(CHANNEL_ID, date)
        restart_requests = self.restart_analyzer.extract_restart_requests(messages)
        services_names = self.restart_analyzer.extract_services(restart_requests)
        restarts_count = self.restart_analyzer.count_restarts(CHANNEL_ID, date)

        daily_message = f"Total restart requests on {date}: {restarts_count} :alien:"
        self.slack_client.send_message(NOTIFICATION_CHANNEL_ID, daily_message)

        message_about_services = json.dumps(services_names, indent=4)
        code_block_res = [{
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"```{message_about_services}```"
            }
        }]
        self.slack_client.send_message(NOTIFICATION_CHANNEL_ID, "Restart is coming soon :alien:", blocks=code_block_res)

    def start_scheduler(self):
        """Starts the scheduled tasks."""
        schedule.every(10).seconds.do(lambda: self.restart_analyzer.count_restarts(CHANNEL_ID, datetime.now().strftime("%Y-%m-%d")))
        schedule.every().day.at("00:40").do(self.daily_check)

        print("Scheduler is running. Press CMD+C to exit.")
        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nScheduler stopped. Goodbye!")


# === MAIN EXECUTION ===
if __name__ == "__main__":
    # Slack API Tokens
    ALERT_CHANNEL_ID = 'C08DFU192MT'
    CHANNEL_ID = "C07UM0ETK5L"
    NOTIFICATION_CHANNEL_ID = 'C088AHY4UAE'

    # Initialize components
    slack_client = SlackClient(SLACK_BOT_TOKEN)
    restart_analyzer = RestartAnalyzer(slack_client)
    scheduler = RestartScheduler(slack_client, restart_analyzer)

    # Start scheduler
    scheduler.start_scheduler()