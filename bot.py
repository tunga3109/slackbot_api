import os
import re
import time
import json
import schedule
import threading
import logging
from dotenv import load_dotenv

from datetime import datetime, timedelta, timezone
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk import WebClient

load_dotenv()  # Загружает переменные из .env-файла

RESTART_BOT_TOKEN = os.environ["RESTART_BOT_TOKEN"]
SOCKET_BOT_TOKEN = os.environ["SOCKET_BOT_TOKEN"]

# === Logger Class ===
class Logger:
    def __init__(self, log_file="restart_bot.log"):
        logging.Formatter.converter = time.gmtime  # Use UTC
        self.logger = logging.getLogger("RestartBotLogger")
        self.logger.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S UTC')

        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        if not self.logger.handlers:
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)

    def info(self, msg): self.logger.info(msg)
    def error(self, msg): self.logger.error(msg)
    def warning(self, msg): self.logger.warning(msg)


logger = Logger()

# === Regex ===
RESTART_KEYWORDS = re.compile(r"\b(reboot|restart)\b", re.IGNORECASE)
SERVICE_KEYWORDS = re.compile(
    r"\b(ecn/mm|ECN/MM|ecn and mm|ECN and MM|ecn|mm|market maker|price-aggregator|price_aggregator|pa|market driver|md|risk manager|manager|MDDRIVER|drivers|driver|RiskManager)\b",
    re.IGNORECASE
)


class SlackClient:
    def __init__(self, token: str):
        self.client = WebClient(token=token)
        self.alert_sent = False

    def fetch_messages(self, channel_id: str, date: str):
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
            logger.error(f"Error fetching messages: {e}")
        return messages

    def send_message(self, channel_id: str, text: str, blocks=None):
        try:
            response = self.client.chat_postMessage(channel=channel_id, text=text, blocks=blocks)
            logger.info(f"Message sent: {response['ts']}")
        except Exception as e:
            logger.error(f"Error sending message: {e}")

    def send_alert(self, channel_id: str, count: int):
        if not self.alert_sent:
            try:
                alert_message = f" :red_circle: Alert! Restart requests exceeded limit: {count} :red_circle:\n <@U08ECFZBYNL> FYI"
                response = self.client.chat_postMessage(channel=channel_id, text=alert_message)
                logger.info(f"Alert sent: {response['ts']}")
                self.alert_sent = True
            except Exception as e:
                logger.error(f"Error sending alert: {e}")

    def reset_alert(self, count):
        if count <= 20 and self.alert_sent:
            logger.info("Restart count back to normal. Resetting alert flag.")
            logger.info(f"Restart count: {count}")
            self.alert_sent = False
        else:
            logger.info(f"Restart count: {count}")


class RestartAnalyzer:
    def __init__(self, slack_client: SlackClient):
        self.slack_client = slack_client

    def extract_restart_requests(self, messages):
        return [
            msg['text'].replace("*", "")
            for msg in messages
            if RESTART_KEYWORDS.search(msg.get('text', '')) and SERVICE_KEYWORDS.search(msg.get('text', ''))
        ]

    def extract_services(self, restart_requests_list):
        service_dict = {}

        for request in restart_requests_list:
            logger.info(f"Processing request:\n{request}\n")

            matches = SERVICE_KEYWORDS.findall(request)
            if not matches:
                logger.info("No service matches found.")
                continue

            logger.info(f"Found services: {matches}")

            for match in matches:
                service_name = match.lower()

                details_pattern = re.compile(fr"{re.escape(match)}\s*[-\:]\s*(.+?)(?:\n|$)", re.IGNORECASE)
                result = details_pattern.search(request)

                if service_name in ("riskmanager", "risk-manager"):
                    service_dict["risk-manager"] = {'1'}
                    continue

                if result:
                    details = result.group(1).strip()
                    if service_name in ("price-aggregator", "price_aggregator", 'pa'):
                        details = re.split(r"\s*[,/|]\s*", details)
                        service_name = 'price-aggregator'
                    elif service_name in ('ecn/mm', 'ecn and mm'):
                        details = re.split(r"\s*[,/|]\s*", details)
                        service_name = 'ecn/mm'
                    elif service_name in ('driver', 'mddriver', 'drivers', 'md'):
                        details = re.split(r"\s*[,/|]\s*", details)
                        service_name = 'market-driver'
                    elif service_name in ('mm', 'MM'):
                        details = re.split(r"\s*[,/|]\s*", details)
                        service_name = 'mm'
                    elif service_name in ('ecn', 'ECN'):
                        details = re.split(r"\s*[,/|]\s*", details)
                        service_name = 'ecn'
                    else:
                        details = re.split(r"\s*\|\s*|\s*,\s*|\s*\+\s*", details)

                    logger.info(f"Extracted details for {service_name}: {details}")

                    if service_name not in service_dict:
                        service_dict[service_name] = set()

                    service_dict[service_name].update(map(str.strip, details))
                else:
                    logger.info(f"No details found for {service_name}")

        logger.info(f"Final extracted services: {service_dict}")
        return {key: list(sorted(value, key=len)) for key, value in service_dict.items()}

    def count_restarts(self, channel_id, date):
        messages = self.slack_client.fetch_messages(channel_id, date)
        restart_requests = self.extract_restart_requests(messages)
        services_names = self.extract_services(restart_requests)
        restart_num = sum(
            len(services) * (2 if key in ('ecn/mm', 'ecn and mm') else 1)
            + sum(1 for s in services if '+ REF' in s)
            for key, services in services_names.items()
        )
        return restart_num


class RestartScheduler:
    def __init__(self, slack_client: SlackClient, restart_analyzer: RestartAnalyzer):
        self.slack_client = slack_client
        self.restart_analyzer = restart_analyzer

    def daily_check(self):
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        string_date = datetime.now(timezone.utc).strftime("%d-%m-%Y")
        messages = self.slack_client.fetch_messages(CHANNEL_ID, date)
        restart_requests = self.restart_analyzer.extract_restart_requests(messages)
        services_names = self.restart_analyzer.extract_services(restart_requests)
        restarts_count = self.restart_analyzer.count_restarts(CHANNEL_ID, date)

        daily_message = f"Total restart requests on {string_date}: {restarts_count} :alien:"
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
        schedule.every().day.at("20:30", "Africa/Bissau").do(lambda: self.daily_check())
        schedule.every(1).minutes.do(self.send_ping)
        logger.info("Scheduler is running. Press CMD+C to exit.")
        try:
            while True:
                schedule.run_pending()
                time.sleep(10)
        except KeyboardInterrupt:
            logger.info("Scheduler stopped. Goodbye!")

    def send_ping(self):
        self.slack_client.send_message(PING_CHANNEL_ID, "✅ Bot is alive!")
        logger.info(f"Automated ping sent to {PING_CHANNEL_ID}.")


class SlackBot:
    def __init__(self, bot_token, app_token):
        self.app = App(token=bot_token)
        self.app_token = app_token
        self.slack_client = SlackClient(bot_token)
        self.restart_analyzer = RestartAnalyzer(self.slack_client)
        self.register_events()

    def register_events(self):
        @self.app.event("message")
        def handle_messages(event, ack):
            ack()
            date = datetime.utcnow().strftime("%Y-%m-%d")
            restarts_count = self.restart_analyzer.count_restarts(CHANNEL_ID, date)
            text = event.get("text", "").lower()

            if "user" not in event or "text" not in event:
                self.slack_client.reset_alert(restarts_count)
                return

            if RESTART_KEYWORDS.search(text):
                if restarts_count > 20:
                    self.slack_client.send_alert(ALERT_CHANNEL_ID, restarts_count)
                    messages = self.slack_client.fetch_messages(CHANNEL_ID, date)
                    restart_requests = self.restart_analyzer.extract_restart_requests(messages)
                    services_names = self.restart_analyzer.extract_services(restart_requests)
                    message_about_services = json.dumps(services_names, indent=4)
                    logger.info(f"Restart count: {restarts_count}")
                    code_block_res = [{
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"```{message_about_services}```"
                        }
                    }]
                    self.slack_client.send_message(ALERT_CHANNEL_ID, 'Restart list', blocks=code_block_res)
                else:
                    logger.info(f"Restart count: {restarts_count}")

    def run(self):
        handler = SocketModeHandler(self.app, self.app_token)
        handler.start()


# === MAIN EXECUTION ===
if __name__ == "__main__":
    ALERT_CHANNEL_ID = 'C08DFU192MT'
    CHANNEL_ID = "C07UM0ETK5L"
    NOTIFICATION_CHANNEL_ID = 'C088AHY4UAE'
    PING_CHANNEL_ID = 'C08KKABJM6Z'

    slack_bot = SlackBot(bot_token=RESTART_BOT_TOKEN, app_token=SOCKET_BOT_TOKEN)
    scheduler = RestartScheduler(slack_bot.slack_client, slack_bot.restart_analyzer)

    bot_thread = threading.Thread(target=slack_bot.run, daemon=True)
    bot_thread.start()

    scheduler.start_scheduler()