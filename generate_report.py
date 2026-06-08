#!/usr/bin/env python3
"""通用增长报告生成器 - 支持不同时间范围"""
import os
import pandas as pd
import smtplib
import argparse
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from utils import logger, safe_read_csv, get_channel_history
from config import ALERT_EMAIL, EMAIL_APP_PASSWORD, MONITOR_FILE

def calculate_growth(channel_id: str, days: int = 7) -> tuple:
    """
    基于历史数据计算真实增长
    :return: (growth_amount, growth_rate, start_subs, end_subs, data_days)
    """
    try:
        history_df = get_channel_history(channel_id)
        if history_df.empty or len(history_df) < 2:
            return 0, 0, 0, 0, 0
        
        if 'date' in history_df.columns:
            try:
                history_df['date'] = pd.to_datetime(history_df['date'])
                history_df = history_df.sort_values('date')
            except Exception as e:
                logger.warning(f"频道 {channel_id} 历史数据日期格式异常: {e}")
                return 0, 0, 0, 0, 0
        else:
            logger.warning(f"频道 {channel_id} 历史数据缺少日期列")
            return 0, 0, 0, 0, 0
        
        end_date = history_df['date'].max()
        start_date = end_date - timedelta(days=days)
        
        start_period = history_df[history_df['date'] <= start_date]
        if start_period.empty:
            start_record = history_df.iloc[0]
            actual_days = (end_date - start_record['date']).days
            if actual_days < min(3, days):  # 最少需要3天数据
                return 0, 0, 0, 0, actual_days
        else:
            start_record = start_period.iloc[-1]
            actual_days = days
        
        end_record = history_df.iloc[-1]
        
        start_subs = int(start_record['subscribers'])
        end_subs = int(end_record['subscribers'])
        growth_amount = end_subs - start_subs
        
        if start_subs > 0:
            growth_rate = (growth_amount / start_subs) * 100
        else:
            growth_rate = 0
            
        return growth_amount, growth_rate, start_subs, end_subs, actual_days
        
    except Exception as e:
        logger.error(f"计算频道 {channel_id} 增长失败: {e}")
        return 0, 0, 0, 0, 0

def generate_report(days: int = 7, report_type: str = "weekly"):
    """生成指定时间范围的报告"""
    try:
        os.makedirs("reports", exist_ok=True)
        
        df = safe_read_csv(MONITOR_FILE)
        if df.empty:
            logger.warning("没有频道数据")
            send_no_data_email(report_type)
            return
        
        report_data = []
        total_processed = 0
        valid_channels = 0
        
        # 根据报告类型调整筛选条件
        if report_type == "weekly":
            min_growth_rate = 3.0   # 周增长率阈值
            min_growth_amount = 200  # 周增长量阈值
            min_data_days = 3       # 最少需要3天数据
            period_name = "周"
        elif report_type == "monthly":
            min_growth_rate = 5.0   # 月增长率阈值
            min_growth_amount = 500  # 月增长量阈值
            min_data_days = 7       # 最少需要7天数据
            period_name = "月"
        else:  # daily
            min_growth_rate = 1.0   # 日增长率阈值
            min_growth_amount = 50   # 日增长量阈值
            min_data_days = 1       # 最少需要1天数据
            period_name = "日"
        
        for _, row in df.iterrows():
            total_processed += 1
            channel_id = row.get('id')
            channel_name = row.get('name', '未知频道')
            
            if not channel_id or pd.isna(channel_id):
                continue
            
            # 跳过短视频频道
            if row.get('short_video', False):
                continue
                
            growth_amount, growth_rate, start_subs, end_subs, data_days = calculate_growth(channel_id, days)
            
            # 数据可靠性检查
            if data_days < min_data_days:
                continue
                
            # 通用筛选条件
            min_subs = 5000
            max_subs = 500000
            
            if (growth_rate >= min_growth_rate and 
                growth_amount >= min_growth_amount and 
                min_subs <= end_subs <= max_subs):
                
                valid_channels += 1
                
                # 计算质量评分
                quality_score = calculate_channel_quality(end_subs, growth_rate, growth_amount, report_type)
                
                # 分级筛选
                if growth_rate > min_growth_rate * 3 and growth_amount > min_growth_amount * 10 and quality_score >= 8:
                    priority = "🔥 高潜力"
                elif growth_rate > min_growth_rate * 2 and growth_amount > min_growth_amount * 5 and quality_score >= 6:
                    priority = "⭐ 优质增长"  
                elif growth_rate > min_growth_rate * 1.5 and growth_amount > min_growth_amount * 2:
                    priority = "📈 稳定增长"
                else:
                    priority = "✅ 一般增长"
                
                report_data.append({
                    '优先级': priority,
                    '频道名称': channel_name,
                    '频道链接': row.get('url', ''),
                    '起始订阅': start_subs,
                    '当前订阅': end_subs,
                    f'{period_name}增长量': growth_amount,
                    f'{period_name}增长率%': round(growth_rate, 2),
                    '质量评分': quality_score,
                    '数据天数': data_days,
                    '更新时间': row.get('update_time', ''),
                    '长视频平均播放数':row.get('long_video_avg_views',0),
                    '长视频互动率':row.get('long_video_avg_interaction_rate',0),
                    '更新频率(1/天)':row.get('update_frequency_days',0),
                    '平均时长':row.get('overall_avg_duration_seconds',0)
                })
        
        logger.info(f"{period_name}度报告处理完成: 总计{total_processed}个频道, 有效{valid_channels}个, 符合条件{len(report_data)}个")
        
        if report_data:
            report_df = pd.DataFrame(report_data)
            report_df = report_df.sort_values(['优先级', '质量评分'], ascending=[True, False])
            
            # 保存报告
            timestamp = datetime.now().strftime('%Y%m%d')
            report_file = f"reports/{report_type}_report_{timestamp}.csv"
            report_df.to_csv(report_file, index=False, encoding='utf-8-sig')
            
            generate_summary_report(report_df, report_file, total_processed, valid_channels, report_type, period_name)
            logger.info(f"{period_name}度报告已生成: {report_file}")
        else:
            logger.info(f"本期无符合条件的增长频道")
            send_no_growth_email(total_processed, valid_channels, report_type, period_name)
            
    except Exception as e:
        logger.error(f"生成{report_type}报告失败: {e}")
        send_error_email(str(e), report_type)

def calculate_channel_quality(current_subs: int, growth_rate: float, growth_amount: int, report_type: str) -> int:
    """基于增长数据计算质量评分 (1-10分)"""
    score = 0
    
    # 根据报告类型调整评分标准
    if report_type == "weekly":
        growth_rate_multiplier = 2
        growth_amount_multiplier = 0.2
    elif report_type == "monthly":
        growth_rate_multiplier = 1
        growth_amount_multiplier = 1
    else:  # daily
        growth_rate_multiplier = 5
        growth_amount_multiplier = 0.05
    
    # 订阅规模 (0-3分)
    if current_subs > 1000000:
        score += 3
    elif current_subs > 500000:
        score += 2  
    elif current_subs > 100000:
        score += 1
    
    # 增长率 (0-4分)
    adjusted_growth_rate = growth_rate * growth_rate_multiplier
    if adjusted_growth_rate > 20:
        score += 4
    elif adjusted_growth_rate > 15:
        score += 3
    elif adjusted_growth_rate > 10:
        score += 2
    elif adjusted_growth_rate > 5:
        score += 1
    
    # 增长量 (0-3分)
    adjusted_growth_amount = growth_amount * growth_amount_multiplier
    if adjusted_growth_amount > 10000:
        score += 3
    elif adjusted_growth_amount > 5000:
        score += 2
    elif adjusted_growth_amount > 1000:
        score += 1
    
    return min(score, 10)

def generate_summary_report(report_df, report_file, total_processed, valid_channels, report_type, period_name):
    """生成摘要报告并发送"""
    total_recommended = len(report_df)
    high_potential = len(report_df[report_df['优先级'] == "🔥 高潜力"])
    quality_growth = len(report_df[report_df['优先级'] == "⭐ 优质增长"])
    stable_growth = len(report_df[report_df['优先级'] == "📈 稳定增长"])
    general_growth = len(report_df[report_df['优先级'] == "✅ 一般增长"])
    
    growth_rate_col = f'{period_name}增长率%'
    avg_growth_rate = report_df[growth_rate_col].mean()
    avg_quality_score = report_df['质量评分'].mean()
    avg_data_days = report_df['数据天数'].mean()
    
    summary = f"""
📊 YouTube频道{period_name}度增长报告 - {datetime.now().strftime('%Y年%m月%d日')}

📈 数据处理统计:
• 总监控频道: {total_processed} 个
• 有效数据频道: {valid_channels} 个
• 推荐频道: {total_recommended} 个

🎯 增长分级统计:
• 🔥 高潜力频道: {high_potential} 个
• ⭐ 优质增长频道: {quality_growth} 个  
• 📈 稳定增长频道: {stable_growth} 个
• ✅ 一般增长频道: {general_growth} 个

📊 质量指标:
• 平均增长率: {avg_growth_rate:.1f}%
• 平均质量评分: {avg_quality_score:.1f}/10
• 平均数据天数: {avg_data_days:.1f} 天

🏆 本期重点推荐 (前5名):
{get_top_recommendations(report_df, period_name)}

详细报告已保存至: {report_file}
报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
    
    send_summary_email(summary, report_type, period_name)
    logger.info(f"{period_name}度摘要报告已发送")

def get_top_recommendations(report_df, period_name):
    """获取前5名推荐"""
    top5 = report_df.head(5)
    if top5.empty:
        return "   本期无推荐频道"
    
    growth_amount_col = f'{period_name}增长量'
    growth_rate_col = f'{period_name}增长率%'
    
    recommendations = []
    for idx, (_, row) in enumerate(top5.iterrows(), 1):
        rec = (f"{idx}. {row['频道名称']} - {row[growth_rate_col]}%增长 "
               f"(质量分: {row['质量评分']}, 增长量: {row[growth_amount_col]:,})")
        recommendations.append(rec)
    return "\n".join(recommendations)

def send_summary_email(summary, report_type, period_name):
    """发送摘要邮件"""
    if not (ALERT_EMAIL and EMAIL_APP_PASSWORD):
        logger.warning("未配置邮件通知，跳过报告发送")
        return
    
    try:
        msg = MIMEMultipart()
        msg["Subject"] = f"📈 YouTube频道{period_name}度增长报告 - {datetime.now().strftime('%Y年%m月%d日')}"
        msg["From"] = ALERT_EMAIL
        msg["To"] = ALERT_EMAIL
        
        msg.attach(MIMEText(summary, 'plain', 'utf-8'))
        
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=10) as server:
            server.starttls()
            server.login(ALERT_EMAIL, EMAIL_APP_PASSWORD)
            server.send_message(msg)
            
        logger.info(f"{period_name}度摘要报告邮件已发送")
    except Exception as e:
        logger.exception(f"发送{period_name}度报告邮件失败: {e}")

def send_no_growth_email(total_processed, valid_channels, report_type, period_name):
    """发送无增长频道通知"""
    if not (ALERT_EMAIL and EMAIL_APP_PASSWORD):
        return
    
    try:
        # 根据报告类型调整筛选条件描述
        if report_type == "weekly":
            min_growth_rate = 3.0
            min_growth_amount = 200
            min_data_days = 3
        elif report_type == "monthly":
            min_growth_rate = 5.0
            min_growth_amount = 500
            min_data_days = 7
        else:  # daily
            min_growth_rate = 1.0
            min_growth_amount = 50
            min_data_days = 1
        
        subject = f"📊 YouTube频道{period_name}度报告 - {datetime.now().strftime('%Y年%m月%d日')}"
        body = f"""
本期未发现符合条件的增长频道。

数据处理统计:
• 总监控频道: {total_processed} 个
• 有效数据频道: {valid_channels} 个 (历史数据≥{min_data_days}天)
• 推荐频道: 0 个

报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

可能原因:
• 本期频道增长普遍放缓
• 筛选条件过于严格 (当前: {period_name}增长率≥{min_growth_rate}%, {period_name}增长量≥{min_growth_amount}, 订阅数5K-500K)
• 历史数据不足 (需要至少{min_data_days}天历史记录)
• 监控频道数量较少

建议检查:
1. 频道监控列表是否正常更新
2. 历史数据是否完整积累
3. 筛选条件是否需要调整
"""
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = ALERT_EMAIL
        msg["To"] = ALERT_EMAIL
        
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=10) as server:
            server.starttls()
            server.login(ALERT_EMAIL, EMAIL_APP_PASSWORD)
            server.send_message(msg)
            
        logger.info(f"无增长{period_name}度通知已发送")
    except Exception as e:
        logger.exception(f"发送无增长通知失败: {e}")

def send_no_data_email(report_type):
    """发送无数据通知"""
    if not (ALERT_EMAIL and EMAIL_APP_PASSWORD):
        return
    
    try:
        period_name = "月" if report_type == "monthly" else "周" if report_type == "weekly" else "日"
        
        subject = f"❌ YouTube频道{period_name}度报告 - 数据异常"
        body = f"""
{period_name}度报告生成失败：未找到监控频道数据。

可能原因:
• 监控列表文件不存在或为空
• 文件路径配置错误
• 数据收集任务未正常运行

请检查:
1. {MONITOR_FILE} 文件是否存在
2. 数据收集任务是否正常执行
3. 配置文件路径是否正确

报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = ALERT_EMAIL
        msg["To"] = ALERT_EMAIL
        
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=10) as server:
            server.starttls()
            server.login(ALERT_EMAIL, EMAIL_APP_PASSWORD)
            server.send_message(msg)
            
        logger.info(f"无数据{period_name}度通知已发送")
    except Exception as e:
        logger.exception(f"发送无数据通知失败: {e}")

def send_error_email(error_msg, report_type):
    """发送错误通知"""
    if not (ALERT_EMAIL and EMAIL_APP_PASSWORD):
        return
    
    try:
        period_name = "月" if report_type == "monthly" else "周" if report_type == "weekly" else "日"
        
        subject = f"❌ YouTube频道{period_name}度报告 - 生成失败"
        body = f"""
{period_name}度报告生成过程中发生错误：

错误信息:
{error_msg}

报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

请检查系统日志获取详细错误信息。
"""
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = ALERT_EMAIL
        msg["To"] = ALERT_EMAIL
        
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=10) as server:
            server.starttls()
            server.login(ALERT_EMAIL, EMAIL_APP_PASSWORD)
            server.send_message(msg)
            
        logger.info(f"{period_name}度错误通知已发送")
    except Exception as e:
        logger.exception(f"发送错误通知失败: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='生成YouTube频道增长报告')
    parser.add_argument('--days', type=int, default=7, help='报告覆盖的天数')
    parser.add_argument('--type', choices=['daily', 'weekly', 'monthly'], default='weekly', help='报告类型')
    
    args = parser.parse_args()
    
    logger.info(f"开始生成{args.type}增长报告...")
    generate_report(days=args.days, report_type=args.type)
    logger.info(f"{args.type}报告生成流程完成")