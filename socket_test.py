from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from config import RESTART_BOT_TOKEN, SOCKET_BOT_TOKEN

class SlackBot:
    def __init__(self, bot_token, app_token):
        """Инициализация бота"""
        self.app = App(token=bot_token)
        self.app_token = app_token
        self.register_events()  # Регистрируем обработчики

    def register_events(self):
        """Обработчик для всех сообщений"""
        @self.app.event("message")
        def handle_messages(event, say):
            text = event.get("text", "").lower()
            user = event["user"]

            if "restart" in text:
                say(f"<@{user}>, detected 'restart'! Executing restart...")
            elif "status" in text:
                say("✅ All systems operational!")

        """Обработчик только для 'hello' """
        @self.app.message("hello")
        def handle_hello(message, say):
            user = message["user"]
            say(f"Hello, <@{user}>! How can I assist you today? 😊")

    def run(self):
        """Запуск бота"""
        handler = SocketModeHandler(self.app, self.app_token)
        handler.start()

# === Запуск бота ===
if __name__ == "__main__":
    bot = SlackBot(bot_token=RESTART_BOT_TOKEN, app_token=SOCKET_BOT_TOKEN)
    bot.run()