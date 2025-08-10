import smtplib
import logging
from email.mime.text import MIMEText
from datetime import datetime
from config import ALERT_EMAIL, EMAIL_APP_PASSWORD, MIN_GROWTH_RATE

logger = logging.getLogger("yt-monitor")

def send_alert(channel_id: str, channel_name: str, prev_subs: int, current_subs: int, growth_rate: float):
    """发送频道增长警报邮件"""
    if not (ALERT_EMAIL and EMAIL_APP_PASSWORD):
        logger.warning("未配置邮件通知，跳过告警")
        return
    
    growth = current_subs - prev_subs
    subject = f"⚠️ YouTube频道增长警报: {channel_name}"
    body = f"""
频道名称: {channel_name}
频道链接: https://www.youtube.com/channel/{channel_id}
增长率: {growth_rate:.2f}% (阈值: {MIN_GROWTH_RATE}%)
当前订阅: {current_subs} (增长量: {growth})
检测时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
    
    try:
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = ALERT_EMAIL
        msg["To"] = ALERT_EMAIL
        
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=10) as server:
            server.starttls()
            server.login(ALERT_EMAIL, EMAIL_APP_PASSWORD)
            server.send_message(msg)
        logger.info(f"已发送邮件告警: {channel_name}")
    except Exception as e:
        logger.exception(f"发送邮件失败: {e}")