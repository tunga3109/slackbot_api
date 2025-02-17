import re
import time
from datetime import datetime, timedelta

from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk import WebClient
from main import count_restarts, send_alert, normal

from config import RESTART_BOT_TOKEN, SOCKET_BOT_TOKEN

class SlackBot:
    def __init__(self, bot_token, app_token):
        self.app = App(token=bot_token)
        self.app_token = app_token
        self.register_events()  
        self.restart_keywords = re.compile(r"\b(reboot|restart)\b", re.IGNORECASE)
        self.service_keywords = re.compile(r"\b(\*?ecn\*?/mm\*?|\*?ecn\*?/mm|ecn/mm|ECN/MM|ecn|mm|market maker|price-aggregator|aggregator|market driver|md|risk manager|manager|MDDRIVER)\b", re.IGNORECASE)

    def register_events(self):
        @self.app.event("message")
        def handle_messages(event, ack):
            ack()
            date = datetime.now().strftime("%Y-%m-%d")
            CHANNEL_ID = "C07UM0ETK5L"
            ALERT_CHANNEL_ID = 'C08DFU192MT'
            restarts_count = count_restarts(CHANNEL_ID, date=date)
            text = event.get("text", "").lower()

            if "user" not in event or "text" not in event:
                normal(restarts_count)
                return

            if self.restart_keywords.search(text):
                if restarts_count > 20:
                    send_alert(ALERT_CHANNEL_ID, restarts_count)

    def run(self):
        handler = SocketModeHandler(self.app, self.app_token)
        handler.start()

if __name__ == "__main__":
    bot = SlackBot(bot_token=RESTART_BOT_TOKEN, app_token=SOCKET_BOT_TOKEN)
    bot.run()