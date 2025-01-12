import os
import re
import schedule
import time
import logging
from datetime import datetime, timedelta, timezone
from slack_sdk import WebClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class UTCFormatter(logging.Formatter):
    converter = time.gmtime  # Use UTC time for log timestamps
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat()

# Configure logging
logging.basicConfig(
    filename="slack_monitor.log",  # Log file name
    level=logging.INFO,  # Log level: DEBUG, INFO, WARNING, ERROR, CRITICAL
    format="%(asctime)s - %(levelname)s - %(message)s",  # Log format
    datefmt="%Y-%m-%d %H:%M:%S UTC"  # UTC timestamp format
)

# Update logger to use UTCFormatter
for handler in logging.getLogger().handlers:
    handler.setFormatter(UTCFormatter("%(asctime)s - %(levelname)s - %(message)s"))

class SlackRestartMonitor:
    def __init__(self, bot_token, channel_id, alert_channel_id):
        self.client = WebClient(token=bot_token)
        self.channel_id = channel_id
        self.alert_channel_id = alert_channel_id
        self.restart_keywords = re.compile(r"\b(reboot|restart)\b", re.IGNORECASE)
        logging.info("SlackRestartMonitor initialized.")

    def extract_restart_requests(self, messages):
        """Extracts messages related to restarts."""
        restart_requests = []
        for message in messages:
            text = message.get('text', '')
            if self.restart_keywords.search(text) and text.startswith(('##', '###')):
                restart_requests.append(text)
        logging.info(f"Extracted {len(restart_requests)} restart requests.")
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
                channel=self.channel_id, oldest=oldest, latest=latest
            )
            messages = response.get("messages", [])
            logging.info(f"Fetched {len(messages)} messages for {date}.")
        except Exception as e:
            logging.error(f"Error fetching messages: {e}")
        return messages

    def count_restarts(self, date):
        """Counts the number of restart-related messages for a given date."""
        messages = self.fetch_messages_for_day(date)
        restart_requests = self.extract_restart_requests(messages)
        logging.info(f"Counted {len(restart_requests)} restart requests for {date}.")
        return len(restart_requests)

    def send_alert(self, date, count):
        """Sends an alert if the number of restart requests exceeds the threshold."""
        try:
            alert_message = f"Alert: On {date}, the number of restart requests has reached {count}."
            response = self.client.chat_postMessage(channel=self.alert_channel_id, text=alert_message)
            logging.info(f"Alert sent: {response['ts']}")
        except Exception as e:
            logging.error(f"Error sending alert: {e}")

    def daily_check(self):
        """Performs the daily check for restart requests."""
        date = datetime.now().strftime("%Y-%m-%d")
        restarts_count = self.count_restarts(date)

        # Send daily report
        daily_message = f"Total restart requests on {date}: {restarts_count}"
        try:
            response = self.client.chat_postMessage(channel=self.alert_channel_id, text=daily_message)
            logging.info(f"Daily message sent: {response['ts']}")
        except Exception as e:
            logging.error(f"Error sending daily message: {e}")

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
    schedule.every().day.at("20:20").do(monitor.daily_check)
    logging.info("Scheduler started. Press Ctrl+C to exit.")

    # Run the scheduler
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Scheduler stopped. Goodbye!")