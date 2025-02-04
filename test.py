import os
import re
import schedule
import time
import json
from datetime import datetime, timedelta
from slack_sdk import WebClient
from dotenv import load_dotenv
from abc import ABC, abstractmethod

# Load environment variables
load_dotenv()
BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")

# Slack Client
class SlackService(ABC):
    @abstractmethod
    def fetch_messages(self, channel_id: str, date: str):
        pass
    
    @abstractmethod
    def send_message(self, channel_id: str, text: str):
        pass

class SlackAPI(SlackService):
    def __init__(self, bot_token: str):
        self.client = WebClient(token=bot_token)
    
    def fetch_messages(self, channel_id: str, date: str):
        messages = []
        try:
            start_time = datetime.strptime(date, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0)
            end_time = start_time + timedelta(days=1)
            response = self.client.conversations_history(
                channel=channel_id, oldest=start_time.timestamp(), latest=end_time.timestamp()
            )
            messages = response.get("messages", [])
        except Exception as e:
            print(f"Error fetching messages: {e}")
        return messages
    
    def send_message(self, channel_id: str, text: str):
        try:
            response = self.client.chat_postMessage(channel=channel_id, text=text)
            print(f"Message sent: {response['ts']}")
        except Exception as e:
            print(f"Error sending message: {e}")

# Restart Monitor Class
class RestartMonitor:
    restart_keywords = re.compile(r"\b(reboot|restart)\b", re.IGNORECASE)
    service_keywords = re.compile(r"\b(\*?ecn\*?/mm\*?|ecn|mm|market maker|price-aggregator|aggregator|market driver|md|risk manager|manager)\b", re.IGNORECASE)
    
    def __init__(self, slack_service: SlackService, channel_id: str, alert_channel_id: str):
        self.slack_service = slack_service
        self.channel_id = channel_id
        self.alert_channel_id = alert_channel_id

    def extract_restart_requests(self, messages):
        return [msg.get('text', '') for msg in messages if 
                self.restart_keywords.search(msg.get('text', '')) and
                msg.get('text', '').startswith(('*##', '*###', '##', '###')) and
                self.service_keywords.search(msg.get('text', ''))]

    def extract_services_names(self, restart_requests_list):
        service_dict = {}
        for request in restart_requests_list:
            match = self.service_keywords.search(request)
            if not match:
                continue
            service_name = match.group().replace("*", " ")
            details_pattern = re.compile(fr"{re.escape(match.group())}:\s*(.+?)\n", re.IGNORECASE)
            result = details_pattern.search(request)
            if result:
                details = result.group(1).replace("*", "").replace("and", "").strip()
                service_dict[service_name] = service_dict.get(service_name, "") + details
        return service_dict

    def count_restarts(self, date):
        messages = self.slack_service.fetch_messages(self.channel_id, date)
        restart_requests = self.extract_restart_requests(messages)
        return len(restart_requests)

    def send_alert(self, date, count):
        alert_message = f"Alert: On {date}, the number of restart requests has reached {count}."
        self.slack_service.send_message(self.alert_channel_id, alert_message)
    
    def daily_check(self):
        date = datetime.now().strftime("%Y-%m-%d")
        messages = self.slack_service.fetch_messages(self.channel_id, date)
        restart_requests = self.extract_restart_requests(messages)
        services_names = self.extract_services_names(restart_requests)
        restarts_count = self.count_restarts(date)
        self.slack_service.send_message(self.alert_channel_id, f"Total restart requests on {date}: {restarts_count}")
        self.slack_service.send_message(self.alert_channel_id, json.dumps(services_names, indent=4))
        if restarts_count > 5:
            self.send_alert(date, restarts_count)

# Scheduler Class
class MonitorScheduler:
    def __init__(self, monitor: RestartMonitor, check_time: str):
        self.monitor = monitor
        self.check_time = check_time
        schedule.every().day.at(self.check_time).do(self.monitor.daily_check)
        print("Scheduler initialized.")

    def start(self):
        print("Scheduler is running. Press CMD+C to exit.")
        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nScheduler stopped. Goodbye!")

a = ''

if __name__ == "__main__":
    CHANNEL_ID = "C07UM0ETK5L"
    ALERT_CHANNEL_ID = "C088AHY4UAE"
    slack_service = SlackAPI(BOT_TOKEN)
    monitor = RestartMonitor(slack_service, CHANNEL_ID, ALERT_CHANNEL_ID)
    scheduler = MonitorScheduler(monitor, "17:00")
    scheduler.start()

