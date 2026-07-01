from prefect import flow, task
from prefect.context import get_run_context
import os, smtplib, json, logging
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)

WHATSAPP_LOG_FILE = "/tmp/whatsapp_alerts.log"

@task
def send_email(subject, body):
    smtp_host = os.getenv('SMTP_HOST', 'smtp.gmail.com')
    smtp_port = int(os.getenv('SMTP_PORT', '587'))
    smtp_user = os.getenv('SMTP_USER', '')
    smtp_pass = os.getenv('SMTP_PASSWORD', '')
    to = os.getenv('ALERT_EMAIL_TO', 'team@example.com')

    if not smtp_user or not smtp_pass:
        logger.warning("SMTP not configured")
        return

    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = smtp_user
    msg['To'] = to
    with smtplib.SMTP(smtp_host, smtp_port) as s:
        s.starttls()
        s.login(smtp_user, smtp_pass)
        s.send_message(msg)
    logger.info(f"Email sent to {to}")

@task
def send_telegram(message):
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN', '')
    chat_id = os.getenv('TELEGRAM_CHAT_ID', '')
    if not bot_token or not chat_id:
        logger.warning("Telegram not configured")
        return

    import requests
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    requests.post(url, json={'chat_id': chat_id, 'text': message, 'parse_mode': 'Markdown'}, timeout=10)
    logger.info("Telegram sent")

@task
def send_whatsapp(message):
    phone = os.getenv('WHATSAPP_PHONE', '')
    api_key = os.getenv('WHATSAPP_API_KEY', '')
    if not phone or not api_key:
        logger.warning(f"WhatsApp not configured. Logging alert to {WHATSAPP_LOG_FILE}")
        with open(WHATSAPP_LOG_FILE, 'a') as f:
            f.write(f"[INFO] WhatsApp Alert: {message}\n")
        return

    try:
        import requests
        url = f"https://api.callmebot.com/whatsapp.php?phone={phone}&text={requests.utils.quote(message)}&apikey={api_key}"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            logger.info(f"WhatsApp sent to {phone}")
        else:
            logger.warning(f"WhatsApp API returned {resp.status_code}")
    except Exception as e:
        logger.error(f"WhatsApp send failed: {e}")
        with open(WHATSAPP_LOG_FILE, 'a') as f:
            f.write(f"[ERROR] WhatsApp failed: {message}\n")

@flow(log_prints=True)
def alert_notify_flow(flow_name="unknown", error="unknown"):
    subject = f"[IPBD Alert] Pipeline Failed: {flow_name}"
    body = f"Flow: {flow_name}\nError: {error[:500]}\nTime: ..."

    send_email(subject, body)
    msg = f"🔴 Pipeline Alert\nFlow: {flow_name}\nError: {error[:200]}"
    send_telegram(msg)
    send_whatsapp(msg)

if __name__ == "__main__":
    alert_notify_flow(flow_name="test", error="test error")
