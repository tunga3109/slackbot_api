import os
import re
from slack_sdk import WebClient
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")

client = WebClient(token=BOT_TOKEN)

restart_keywords = re.compile(r"\b(рестарт|перезапуск|reboot|restart)\b", re.IGNORECASE)

def extract_restart_requests(messages):
    """Извлекает сообщения о рестартах из списка сообщений."""
    restart_requests = []
    for message in messages:
        text = message.get('text', '')  # Безопасно извлекаем текст сообщения
        if restart_keywords.search(text) and text.startswith(('##', '###')):
            restart_requests.append(text)
    return restart_requests


if __name__ == "__main__":
    response = client.conversations_history(channel='C07UM0ETK5L')
    if 'messages' in response:
        restart_messages = extract_restart_requests(response['messages'])
        for msg in restart_messages:
            print(msg)



