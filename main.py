import os
import re

import schedule
import time
from datetime import datetime, timedelta

from slack_sdk import WebClient
from config import SLACK_BOT_TOKEN

client = WebClient(token=SLACK_BOT_TOKEN)

restart_keywords = re.compile(r"\b(reboot|restart)\b", re.IGNORECASE)
service_keywords = re.compile(r"\b(\*?ecn\*?/mm\*?|\*?ecn\*?/mm|ecn/mm|ECN/MM|ecn|mm|market maker|price-aggregator|aggregator|market driver|md|risk manager|manager|MDDRIVER)\b", re.IGNORECASE)

def extract_restart_requests(messages):
    """Извлекает сообщения о рестартах из списка сообщений."""
    restart_requests = []
    for message in messages:
        text = message.get('text', '')  
        if restart_keywords.search(text) and text.startswith(('*##', '*###', '##', '###')) and service_keywords.search(text):
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
        )
        messages = response.get("messages", [])
    except Exception as e:
        print(f"Error fetching messages: {e}")
     
    return messages

def extract_services_names(restart_requests_list):
    service_dict = {}

    for request in restart_requests_list:
        match = service_keywords.search(request)
        if not match:
            continue

        service_name = match.group().lower()
        details_pattern = re.compile(fr"{re.escape(service_name)}:\s*(.+?)(?:\n|$)", re.IGNORECASE)
        result = details_pattern.search(request)

        if result:
            details = result.group(1).replace('*', '').replace('and', ',')  

            if service_name not in service_dict:
                service_dict[service_name] = set()
                
            service_dict[service_name].update(map(str.strip, details.split(',')))

    return {key: list(value) for key, value in service_dict.items()}

def count_restarts(channel_id, date):
    restart_num = 0
    messages = fetch_messages_for_day(channel_id, date)
    restart_requests = extract_restart_requests(messages)
    services_names = extract_services_names(restart_requests)
    restart_num = sum(len(services) * (2 if key == 'ecn/mm' else 1) + sum(1 for s in services if 'REF' in s) for key, services in services_names.items())
    
    return restart_num

alert_sent = False

def send_alert(channel_id, count):
    global alert_sent  

    if not alert_sent:
        try:
            alert_message = f" :red_circle: Alert! Restart requests exceeded limit: {count} :red_circle:\n <@U08ECFZBYNL> FYI"
            response = client.chat_postMessage(channel=channel_id, text=alert_message)
            print(f"Alert sent: {response['ts']}")
            alert_sent = True  
        except Exception as e:
            print(f"Error sending alert: {e}")

def normal(count):  
    global alert_sent
    if count <= 20 and alert_sent:
        print("Restart count back to normal. Resetting alert flag.")
        alert_sent = False

def daily_check():
    import json
    CHANNEL_ID = "C07UM0ETK5L"  
    NOTIFICATION_CHANNEL_ID = 'C088AHY4UAE'
    DATE = datetime.now().strftime("%Y-%m-%d") 

    messages = fetch_messages_for_day(CHANNEL_ID, DATE)
    restart_requests = extract_restart_requests(messages)
    services_names = extract_services_names(restart_requests)
    restarts_count = count_restarts(CHANNEL_ID, DATE)

    daily_message = f"Total restart requests on {DATE}: {restarts_count} :alien:"
    response = client.chat_postMessage(channel=NOTIFICATION_CHANNEL_ID, text=daily_message)
    message_about_services = json.dumps(services_names, indent=4)
    code_block_res = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"```{message_about_services}```"
                }
            }
        ]
    client.chat_postMessage(channel=NOTIFICATION_CHANNEL_ID, blocks=code_block_res, text='Restart is coming soon :alien:')
    print(f"Alert sent: {response['ts']}")



if __name__ == "__main__":
    CHANNEL_ID = "C07UM0ETK5L"
    DATE = datetime.now().strftime("%Y-%m-%d")
    schedule.every().day.at("23:30").do(daily_check)
    print("Scheduler is running. Press CMD+C to exit.")
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nScheduler stopped. Goodbye!")

