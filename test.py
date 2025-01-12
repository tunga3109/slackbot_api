import os
import re
import schedule
import time
from datetime import datetime, timedelta
from slack_sdk import WebClient
from dotenv import load_dotenv

load_dotenv()

class SlackRestartMonitor:
    def __init__(self, bot_token, channel_id, alert_channel_id):
        self.client = WebClient(token=bot_token)
        self.channel_id = channel_id
        self.alert_channel_id = alert_channel_id
        self.restart_keywords = re.compile(r"\b(reboot|restart)\b", re.IGNORECASE)

    def extract_restart_requests(self, messages):
        """Extracts messages related to restarts."""
        restart_requests = []
        for message in messages:
            text = message.get('text', '')
            if self.restart_keywords.search(text) and text.startswith(('##', '###')):
                restart_requests.append(text)
        return restart_requests

    def fetch_messages_for_day(self, date):
        """Fetches all messages for a specific day (midnight to midnight)."""
        messages = []
        try:
            # Calculate time range
            start_time = datetime.strptime(date, "%Y-%m-%d").replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            end_time = start_time + timedelta(days=1)

            # Convert to UNIX timestamps
            oldest = start_time.timestamp()
            latest = end_time.timestamp()

            # Fetch messages
            response = self.client.conversations_history(
                channel=self.channel_id, oldest=oldest, latest=latest, limit=self.max_limit
            )
            messages = response.get("messages", [])
        except Exception as e:
            print(f"Error fetching messages: {e}")
        return messages

    def count_restarts(self, date):
        """Counts the number of restart-related messages for a given date."""
        messages = self.fetch_messages_for_day(date)
        restart_requests = self.extract_restart_requests(messages)
        return len(restart_requests)

    def send_alert(self, date, count):
        """Sends an alert if the number of restart requests exceeds the threshold."""
        try:
            alert_message = f"Alert: On {date}, the number of restart requests has reached {count}."
            response = self.client.chat_postMessage(channel=self.alert_channel_id, text=alert_message)
            print(f"Alert sent: {response['ts']}")
        except Exception as e:
            print(f"Error sending alert: {e}")

    def daily_check(self):
        """Performs the daily check for restart requests."""
        date = datetime.now().strftime("%Y-%m-%d")
        restarts_count = self.count_restarts(date)

        # Send daily report
        daily_message = f"Total restart requests on {date}: {restarts_count}"
        try:
            response = self.client.chat_postMessage(channel=self.alert_channel_id, text=daily_message)
            print(f"Daily message sent: {response['ts']}")
        except Exception as e:
            print(f"Error sending daily message: {e}")

        # Send alert if necessary
        if restarts_count > 5:
            self.send_alert(date, restarts_count)


if __name__ == "__main__":
    # Environment variables
    BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
    CHANNEL_ID = "C07UM0ETK5L"  # Replace with your Slack channel ID
    ALERT_CHANNEL_ID = "C088AHY4UAE"  # Replace with your alert channel ID

    # Create instance of SlackRestartMonitor
    monitor = SlackRestartMonitor(BOT_TOKEN, CHANNEL_ID, ALERT_CHANNEL_ID)

    # Schedule daily check
    schedule.every().day.at("19:05").do(monitor.daily_check)
    print("Scheduler is running. Press Ctrl+C to exit.")

    # Run the scheduler
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nScheduler stopped. Goodbye!")