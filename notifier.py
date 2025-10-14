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


def send_monthly_report(report_file: str, channel_count: int):
    """发送月度报告邮件"""
    if not (ALERT_EMAIL and EMAIL_APP_PASSWORD):
        logger.warning("未配置邮件通知，跳过月度报告")
        return
    
    subject = f"📈 YouTube频道月度增长报告 - {datetime.now().strftime('%Y年%m月')}"
    body = f"""
本月发现 {channel_count} 个稳定增长的优质频道。

报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
报告文件: {report_file}

主要筛选条件:
- 订阅数 > 1万
- 月增长率 > 5%
- 月增长量 > 1000

请查看附件获取详细频道列表。
"""
    # 添加附件发送逻辑（需要扩展邮件功能支持附件）
    # 这里简化为在正文中显示
    try:
        with open(report_file, 'r', encoding='utf-8-sig') as f:
            report_content = f.read()
        body += f"\n\n前10个频道:\n{report_content[:2000]}..."  # 限制长度
        
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = ALERT_EMAIL
        msg["To"] = ALERT_EMAIL
        
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=10) as server:
            server.starttls()
            server.login(ALERT_EMAIL, EMAIL_APP_PASSWORD)
            server.send_message(msg)
        logger.info(f"已发送月度报告: {channel_count}个频道")
    except Exception as e:
        logger.exception(f"发送月度报告失败: {e}")