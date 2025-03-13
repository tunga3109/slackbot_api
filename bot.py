import os
import re
import time
import json
import schedule
import threading

from datetime import datetime, timedelta
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk import WebClient

from config import RESTART_BOT_TOKEN, SOCKET_BOT_TOKEN

# Constants
RESTART_KEYWORDS = re.compile(r"\b(reboot|restart)\b", re.IGNORECASE)
SERVICE_KEYWORDS = re.compile(r"\b(ecn/mm|ECN/MM|enc and mm|ECN and MM|ecn|mm|market maker|price-aggregator|price_aggregator|aggregator|market driver|md|risk manager|manager|MDDRIVER|drivers|driver|RiskManager)\b", re.IGNORECASE)

class SlackClient:
    """Handles communication with Slack API."""
    def __init__(self, token: str):
        self.client = WebClient(token=token)
        self.alert_sent = False

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
        """Sends an alert message if the restart request count exceeds the limit."""
        if not self.alert_sent:
            try:
                alert_message = f" :red_circle: Alert! Restart requests exceeded limit: {count} :red_circle:\n <@U08ECFZBYNL> FYI"
                response = self.client.chat_postMessage(channel=channel_id, text=alert_message)
                print(f"Alert sent: {response['ts']}")
                self.alert_sent = True  
            except Exception as e:
                print(f"Error sending alert: {e}")

    def reset_alert(self, count):
        """Resets the alert flag when the restart request count returns to normal."""
        if count <= 20 and self.alert_sent:
            print("Restart count back to normal. Resetting alert flag.")
            self.alert_sent = False


class RestartAnalyzer:
    """Extracts and analyzes restart requests."""
    def __init__(self, slack_client: SlackClient):
        self.slack_client = slack_client

    def extract_restart_requests(self, messages):
        """Extracts messages related to restarts."""
        return [msg['text'].replace("*", "") for msg in messages if RESTART_KEYWORDS.search(msg.get('text', '')) and SERVICE_KEYWORDS.search(msg.get('text', ''))]

    def extract_services(self, restart_requests_list):
        service_dict = {}

        for request in restart_requests_list:
            print(f"Processing request:\n{request}\n")  # Лог запроса

            matches = SERVICE_KEYWORDS.findall(request)
            if not matches:
                print("No service matches found.")
                continue

            print(f"Found services: {matches}")

            for match in matches:
                service_name = match.lower()

                if service_name in ("riskmanager", "risk-manager"):  # Добавляем сразу
                    service_dict["risk-manager"] = '1'
                    continue

                # Регулярное выражение для поиска деталей сервиса
                details_pattern = re.compile(
                    fr"{re.escape(match)}\s*[-\:]\s*(.+?)(?:\n|$)",  
                    re.IGNORECASE
                )
                result = details_pattern.search(request)

                if result:
                    details = result.group(1).strip()
                    if service_name in ("price-aggregator", "price_aggregator"):
                        details = re.split(r"\s*[,/|]\s*", details)  # Store as a single list element
                        service_name = 'price-aggregator'
                    elif service_name in ('ecn/mm', 'ecn and mm'):
                        details = re.split(r"\s*[,/|]\s*", details)
                        service_name = 'ecn/mm'
                    elif service_name in ('driver', 'mddriver', 'drivers', 'md'):
                        details = re.split(r"\s*[,/|]\s*", details)
                        service_name = 'market-driver'  
                    else:
                        details = re.split(r"\s*\|\s*|\s*,\s*|\s*\+\s*", details)  # Разделяем по `|`, `,`, `+`
                    
                    print(f"Extracted details for {service_name}: {details}")

                    if service_name not in service_dict:
                        service_dict[service_name] = set()

                    service_dict[service_name].update(map(str.strip, details))
                else:
                    print(f"No details found for {service_name}")

        print(f"Final extracted services: {service_dict}")
        return {key: list(sorted(value)) for key, value in service_dict.items()}

    def count_restarts(self, channel_id, date):
        """Counts restart requests and triggers an alert if necessary."""
        messages = self.slack_client.fetch_messages(channel_id, date)
        restart_requests = self.extract_restart_requests(messages)
        services_names = self.extract_services(restart_requests)
        restart_num = sum(len(services) * (2 if (key == 'ecn/mm') or (key == 'ecn and mm') else 1) + sum(1 for s in services if '+ REF' in s) for key, services in services_names.items())

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
        schedule.every().day.at("23:25", "Africa/Bissau").do(lambda: self.daily_check())

        print("Scheduler is running. Press CMD+C to exit.")
        try:
            while True:
                schedule.run_pending()
                time.sleep(10)
        except KeyboardInterrupt:
            print("\nScheduler stopped. Goodbye!")


class SlackBot:
    """Handles real-time Slack events."""
    def __init__(self, bot_token, app_token):
        self.app = App(token=bot_token)
        self.app_token = app_token
        self.slack_client = SlackClient(bot_token)
        self.restart_analyzer = RestartAnalyzer(self.slack_client)
        self.register_events()

    def register_events(self):
        """Registers event listeners for Slack messages."""
        @self.app.event("message")
        def handle_messages(event, ack):
            ack()
            date = datetime.now().strftime("%Y-%m-%d")
            restarts_count = self.restart_analyzer.count_restarts(CHANNEL_ID, date)
            text = event.get("text", "").lower()

            if "user" not in event or "text" not in event:
                self.slack_client.reset_alert(restarts_count)
                return

            if RESTART_KEYWORDS.search(text):
                if restarts_count > 20:
                    self.slack_client.send_alert(ALERT_CHANNEL_ID, restarts_count)
                    date = datetime.now().strftime("%Y-%m-%d")
                    messages = self.slack_client.fetch_messages(CHANNEL_ID, date)
                    restart_requests = self.restart_analyzer.extract_restart_requests(messages)
                    services_names = self.restart_analyzer.extract_services(restart_requests)
                    message_about_services = json.dumps(services_names, indent=4)

                    code_block_res = [{
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"```{message_about_services}```"
                        }
                    }]
                    self.slack_client.send_message(ALERT_CHANNEL_ID, 'Restart list', blocks=code_block_res)


    def run(self):
        """Starts the Slack bot using Socket Mode."""
        handler = SocketModeHandler(self.app, self.app_token)
        handler.start()


# === MAIN EXECUTION ===
if __name__ == "__main__":
    ALERT_CHANNEL_ID = 'C08DFU192MT'
    CHANNEL_ID = "C07UM0ETK5L"
    NOTIFICATION_CHANNEL_ID = 'C088AHY4UAE'

    slack_bot = SlackBot(bot_token=RESTART_BOT_TOKEN, app_token=SOCKET_BOT_TOKEN)
    scheduler = RestartScheduler(slack_bot.slack_client, slack_bot.restart_analyzer)

    bot_thread = threading.Thread(target=slack_bot.run, daemon=True)
    bot_thread.start()

    # Start scheduler
    scheduler.start_scheduler()