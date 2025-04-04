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
SERVICE_KEYWORDS = re.compile(r"\b(\*?ecn\*?/mm\*?|ecn|mm|market maker|price-aggregator|aggregator|market driver|md|risk manager|manager|MDDRIVER)\b", re.IGNORECASE)
MENTION_PATTERN = re.compile(r"<@U08ECFZBYNL>")


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

    def fetch_replies(self, channel_id: str, thread_ts: str):
        """Fetches replies from a specific thread."""
        try:
            response = self.client.conversations_replies(channel=channel_id, ts=thread_ts)
            return response.get("messages", [])
        except Exception as e:
            print(f"Error fetching replies: {e}")
            return []

    def send_message(self, channel_id: str, text: str, blocks=None):
        """Sends a message to Slack."""
        try:
            response = self.client.chat_postMessage(channel=channel_id, text=text, blocks=blocks)
            print(f"Message sent: {response['ts']}")
        except Exception as e:
            print(f"Error sending message: {e}")

    def send_alert(self, channel_id: str, count: int):
        """Sends an alert if restart requests exceed the limit."""
        if not self.alert_sent:
            try:
                alert_message = f" :red_circle: Alert! Restart requests exceeded limit: {count} :red_circle:\n <@U08ECFZBYNL> FYI"
                response = self.client.chat_postMessage(channel=channel_id, text=alert_message)
                print(f"Alert sent: {response['ts']}")
                self.alert_sent = True  
            except Exception as e:
                print(f"Error sending alert: {e}")

    def reset_alert(self, count):
        """Resets the alert flag when restart requests normalize."""
        if count <= 20 and self.alert_sent:
            print(f"Restart count back to normal: {count}. Resetting alert flag.")
            self.alert_sent = False


class RestartAnalyzer:
    """Extracts and analyzes restart requests."""
    def __init__(self, slack_client: SlackClient):
        self.slack_client = slack_client

    def extract_restart_requests(self, channel_id, messages):
        """Extracts restart requests where the bot is mentioned in replies."""
        restart_requests = []
        for message in messages:
            # if "thread_ts" not in message or "ts" not in message:
            #     continue  

            text = message.get("text", "")
            if RESTART_KEYWORDS.search(text) and text.startswith(("*##", "*###", "##", "###")) and SERVICE_KEYWORDS.search(text):
                restart_requests.append(text)
            # replies = self.slack_client.fetch_replies(channel_id, message["ts"])

            # if any(MENTION_PATTERN.search(reply.get("text", "")) for reply in replies):
            #     if RESTART_KEYWORDS.search(text) and text.startswith(("*##", "*###", "##", "###")) and SERVICE_KEYWORDS.search(text):
            #         restart_requests.append(text)

        return restart_requests

    def extract_services(self, restart_requests):
        """Extracts service names from restart requests."""
        service_dict = {}

        for request in restart_requests:
            match = SERVICE_KEYWORDS.search(request)
            if not match:
                continue

            service_name = match.group().lower()
            details_pattern = re.compile(fr"{re.escape(service_name)}:\s*(.+?)(?:\n|$)", re.IGNORECASE)
            result = details_pattern.search(request)

            if result:
                details = result.group(1).replace("*", "").replace("and", ",")
                service_dict.setdefault(service_name, set()).update(map(str.strip, details.split(",")))

        return {key: sorted(value) for key, value in service_dict.items()}

    def count_restarts(self, channel_id, date):
        """Counts restart requests and triggers an alert if necessary."""
        messages = self.slack_client.fetch_messages(channel_id, date)
        restart_requests = self.extract_restart_requests(channel_id, messages)
        services_names = self.extract_services(restart_requests)

        restart_num = sum(
            len(services) * (2 if key == "ecn/mm" else 1) + sum(1 for s in services if "REF" in s)
            for key, services in services_names.items()
        )
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
        restart_requests = self.restart_analyzer.extract_restart_requests(CHANNEL_ID, messages)
        services_names = self.restart_analyzer.extract_services(restart_requests)
        restarts_count = self.restart_analyzer.count_restarts(CHANNEL_ID, date)

        self.slack_client.send_message(NOTIFICATION_CHANNEL_ID, f"Total restart requests on {date}: {restarts_count} :alien:")

        message_about_services = json.dumps(services_names, indent=4)
        code_block_res = [{
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"```{message_about_services}```"}
        }]
        self.slack_client.send_message(NOTIFICATION_CHANNEL_ID, "Restart is coming soon :alien:", blocks=code_block_res)

    def start_scheduler(self):
        """Starts the scheduled tasks."""
        schedule.every().day.at("15:48", "Africa/Bissau").do(lambda: self.daily_check())
        schedule.every(5).minutes.do(self.send_ping)
        print("Scheduler is running. Press CMD+C to exit.")

        try:
            while True:
                schedule.run_pending()
                time.sleep(10)
        except KeyboardInterrupt:
            print("\nScheduler stopped. Goodbye!")

    def send_ping(self):
        """Отправляет автоматический пинг в канал."""
        self.slack_client.send_message(ALERT_CHANNEL_ID, "✅ Bot is alive!")
        print(f"Automated ping sent to {ALERT_CHANNEL_ID}.")


class SlackBot:
    """Handles real-time Slack events."""
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
            CHANNEL_ID = "C07UM0ETK5L"
            date = datetime.now().strftime("%Y-%m-%d")
            restarts_count = self.restart_analyzer.count_restarts(CHANNEL_ID, date)
            if "user" not in event or "text" not in event:
                print("[SKIP] No user or text in event")
                self.slack_client.reset_alert(restarts_count)
                return

            text = event["text"]
            ts = event["ts"]
            parent_ts = event.get("thread_ts", ts)
            channel = event["channel"]
            date = datetime.now().strftime("%Y-%m-%d")
            
            print(event)
            print(f"[EVENT] New message in {channel}: {text} (ts: {ts}, thread_ts: {parent_ts})")

            replies = self.slack_client.fetch_replies(channel, parent_ts)
            print(f"[DEBUG] Found {len(replies)} replies")
            print(f"[DEBUG] Replies content: {[r.get('text', '') for r in replies]}")

            mention_found = any(MENTION_PATTERN.search(reply.get("text", "")) for reply in replies)
            thread_with_tag = mention_found and (RESTART_KEYWORDS.search(text) and SERVICE_KEYWORDS.search(text))
            restart_threads = RESTART_KEYWORDS.search(text) and SERVICE_KEYWORDS.search(text) and text.startswith(("*##", "*###", "##", "###"))

            if thread_with_tag:
                print("[MATCH] Mention or restart detected in replies!")
                messages = self.slack_client.fetch_messages(channel_id=channel, date=date)
                restart_requests = text = self.restart_analyzer.extract_restart_requests(channel_id=channel, messages=messages)
                services_names = self.restart_analyzer.extract_services(restart_requests)
                restarts_count = self.restart_analyzer.count_restarts(CHANNEL_ID, date)
                print(f"[MATCH] Restart requests: {services_names}")

                if restarts_count > 20:
                    self.slack_client.send_alert(ALERT_CHANNEL_ID, restarts_count)
            
            if restart_threads:
                print("[MATCH] Mention detected in replies!")
                messages = self.slack_client.fetch_messages(channel_id=channel, date=date)
                restart_requests = text = self.restart_analyzer.extract_restart_requests(channel_id=channel, messages=messages)
                services_names = self.restart_analyzer.extract_services(restart_requests)
                restarts_count = self.restart_analyzer.count_restarts(CHANNEL_ID, date)
                print(f"[MATCH] Restart requests: {services_names}")

                if restarts_count > 20:
                    self.slack_client.send_alert(ALERT_CHANNEL_ID, restarts_count)
                

                    

                
                
        
                # if RESTART_KEYWORDS.search(text) and SERVICE_KEYWORDS.search(text):
                #     print(f"[MATCH] Restart request detected: {text}")
                #     replies = self.slack_client.fetch_replies(channel, parent_ts)
                #     print(f"[DEBUG] Found {len(replies)} replies")

                #     messages = self.slack_client.fetch_messages(CHANNEL_ID, date)
                #     restart_requests = text = self.restart_analyzer.extract_restart_requests(channel_id=CHANNEL_ID, messages=messages)
                #     services_names = self.restart_analyzer.extract_services(restart_requests)
                #     print(f"[MATCH] Restart requests: {services_names}")
                #     print("[MATCH] Mention detected in replies!")


                # replies = self.slack_client.fetch_replies(channel, parent_ts)
                # print(f"[DEBUG] Found {len(replies)} replies")
                # print(f"[DEBUG] Replies content: {[r.get('text', '') for r in replies]}")  

                # # Проверяем, есть ли упоминание в ЛЮБОМ ответе
                # mention_found = any(MENTION_PATTERN.search(reply.get("text", "")) for reply in replies)

                # if mention_found:
                #     print("[MATCH] Mention detected in replies!")

                #     date = datetime.now().strftime("%Y-%m-%d")
                #     restarts_count = self.restart_analyzer.count_restarts(channel, date)

                #     print(f"[COUNT] Total restarts: {restarts_count}")

                #     if restarts_count > 20:
                #         print("[ALERT] Sending alert!")
                #         self.slack_client.send_alert(ALERT_CHANNEL_ID, restarts_count)
                # else:
                #     print("[DEBUG] No mention found in replies")

    def run(self):
        """Starts the Slack bot using Socket Mode."""
        handler = SocketModeHandler(self.app, self.app_token)
        handler.start()


# === MAIN EXECUTION ===
if __name__ == "__main__":
    ALERT_CHANNEL_ID = "C08DFU192MT"
    CHANNEL_ID = "C07UM0ETK5L"
    NOTIFICATION_CHANNEL_ID = "C088AHY4UAE"

    slack_bot = SlackBot(bot_token=RESTART_BOT_TOKEN, app_token=SOCKET_BOT_TOKEN)
    scheduler = RestartScheduler(slack_bot.slack_client, slack_bot.restart_analyzer)

    threading.Thread(target=slack_bot.run, daemon=True).start()
    scheduler.start_scheduler()