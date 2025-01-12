import os
import re

import schedule
import time
from datetime import datetime, timedelta

from slack_sdk import WebClient
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")

client = WebClient(token=BOT_TOKEN)

restart_keywords = re.compile(r"\b(reboot|restart)\b", re.IGNORECASE)
MAX_LIMIT = 5

def extract_restart_requests(messages):
    """Извлекает сообщения о рестартах из списка сообщений."""
    restart_requests = []
    for message in messages:
        text = message.get('text', '')  
        if restart_keywords.search(text) and text.startswith(('##', '###')):
            restart_requests.append(text)
    return restart_requests


def fetch_messages_for_day(channel_id, date):
    """
    Fetches all messages from Slack for a specific day (midnight to midnight).

    Args:
        channel_id (str): Slack channel ID.
        date (str): Date in "YYYY-MM-DD" format.

    Returns:
        list: List of messages within the time range.
    """
    messages = []
    try:
        # Calculate time range
        start_time = datetime.strptime(date, "%Y-%m-%d")
        start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = start_time + timedelta(days=1)

        # Convert to UNIX timestamps
        oldest = start_time.timestamp()
        latest = end_time.timestamp()

        # Fetch messages
        response = client.conversations_history(
            channel=channel_id,
            oldest=oldest,
            latest=latest,
            limit=MAX_LIMIT
        )
        messages = response.get("messages", [])
    except Exception as e:
        print(f"Error fetching messages: {e}")
    
    return messages

def count_restarts(channel_id, date):
    messages = fetch_messages_for_day(channel_id, date)
    restart_requests = extract_restart_requests(messages)
    return len(restart_requests)

def send_alert(channel_id, date, count):
    try:
        alert_message = f"Alert: On {date}, the number of restart requests has reached {count}."
        response = client.chat_postMessage(channel=channel_id, text=alert_message)
        print(f"Alert sent: {response['ts']}")
    except Exception as e:
        print(f"Error sending alert: {e}")

def daily_check():
    CHANNEL_ID = "C07UM0ETK5L"  # Replace with your Slack channel ID
    DATE = datetime.now().strftime("%Y-%m-%d")  # Current date
    restarts_count = count_restarts(CHANNEL_ID, DATE)
    daily_message = f"Total restart requests on {DATE}: {restarts_count}"
    ALERT_CHANNEL_ID = 'C07V5MFH319'
    response = client.chat_postMessage(channel=CHANNEL_ID, text=daily_message)
    print(f"Alert sent: {response['ts']}")
    if restarts_count > 5:
        send_alert(ALERT_CHANNEL_ID, DATE, restarts_count)


a = ''
if __name__ == "__main__":
    schedule.every().day.at("00:01").do(daily_check)
    print("Scheduler is running. Press Ctrl+C to exit.")
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nScheduler stopped. Goodbye!")

