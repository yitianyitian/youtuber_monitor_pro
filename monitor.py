import os
import time
import pandas as pd
from datetime import datetime
from googleapiclient.errors import HttpError
from config import HISTORY_DIR, MIN_GROWTH_RATE, MIN_INACTIVE_DAYS, GROWTH_THRESHOLD, FILTER_SHORT_VIDEOS,MONITOR_FILE
from utils import youtube, read_channel_list, write_atomic_csv, parse_channel_id, logger
from utils import retry, append_history, get_channel_history
from notifier import send_alert

# 获取频道订阅数
@retry(Exception, tries=4, delay=1, backoff=2)
def get_subs_via_api(channel_id: str) -> int:
    try:
        resp = youtube.channels().list(part="statistics", id=channel_id).execute()
        items = resp.get("items", [])
        if not items:
            raise ValueError("Channel not found")
        subs = int(items[0]["statistics"].get("subscriberCount", 0))
        return subs
    except HttpError as e:
        if e.resp.status == 403:
            logger.error("API配额耗尽或密钥无效")
        else:
            logger.warning(f"API 获取订阅数失败: {e}")
        raise
    except Exception as e:
        logger.warning(f"API 获取订阅数异常: {e}")
        raise

# 更新频道数据
def update_channel_data():
    try:
        df = read_channel_list(MONITOR_FILE)
        # 确保有short_video列
        if "short_video" not in df.columns:
            df["short_video"] = False
    except FileNotFoundError:
        logger.error("频道列表文件不存在，请先运行收集器")
        return
    
    updated = False
    for idx, row in df.iterrows():
        url = str(row["url"])
        name = row.get("name", url)
        channel_id = row.get("id")
        
        # 如果启用短视频过滤且该频道是短视频频道，跳过更新
        if FILTER_SHORT_VIDEOS and row.get("short_video", False):
            logger.info(f"跳过短视频频道: {name}")
            continue
        
        # 如果缺少channel_id，尝试解析
        if not channel_id or pd.isna(channel_id):
            try:
                channel_id = parse_channel_id(url)
                df.at[idx, "id"] = channel_id
            except Exception as e:
                logger.error(f"{name} 解析 channel id 失败: {e}")
                continue
        
        try:
            current_subs = get_subs_via_api(channel_id)
        except Exception as e:
            logger.warning(f"{name} 获取订阅数失败，跳过: {e}")
            continue
        
        prev_current = int(row.get("current_subs") if not pd.isna(row.get("current_subs")) else 0)
        prev_last = int(row.get("last_subs") if not pd.isna(row.get("last_subs")) else prev_current)
        
        # 计算增长率
        growth = current_subs - prev_current
        growth_rate = (growth / prev_current * 100) if prev_current > 0 else 0.0
        
        # 更新数据
        df["update_time"] = df["update_time"].astype(str)  # 确保列是字符串类型
        update_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        df.at[idx, "last_subs"] = prev_current
        df.at[idx, "current_subs"] = current_subs
        df.at[idx, "growth"] = growth
        df.at[idx, "growth_rate"] = round(growth_rate, 4)
        df.at[idx, "update_time"] = update_time
        
        # 保存历史记录
        append_history(channel_id, datetime.utcnow().isoformat(), current_subs)
        logger.info(f"{name} ({channel_id}): {prev_current} -> {current_subs} Δ={growth} ({growth_rate:.2f}%)")
        
        # 检查增长率是否超过阈值
        # if growth_rate >= MIN_GROWTH_RATE:
        #     send_alert(channel_id, name, prev_current, current_subs, growth_rate)
        
        time.sleep(1.2)  # 避免请求过快
        updated = True
    
    if updated:
        write_atomic_csv(MONITOR_FILE, df)
    logger.info("频道数据更新完成")

# 移除不活跃频道
def remove_inactive_channels():
    try:
        df = read_channel_list(MONITOR_FILE)
        # 确保有short_video列
        if "short_video" not in df.columns:
            df["short_video"] = False
    except FileNotFoundError:
        logger.error("频道列表文件不存在")
        return
    
    remove_ids = []
    for idx, row in df.iterrows():
        # 跳过短视频频道（如果启用过滤）
        if FILTER_SHORT_VIDEOS and row.get("short_video", False):
            continue
            
        cid = row.get("id")
        if not cid or pd.isna(cid):
            continue
        
        try:
            history = get_channel_history(cid, MIN_INACTIVE_DAYS + 1)
            if len(history) < MIN_INACTIVE_DAYS:
                continue
                
            # 计算最近N天的增长率
            start = history.iloc[-1]["subscribers"]
            end = history.iloc[0]["subscribers"]
            if start <= 0:
                continue
                
            gr = (end - start) / start
            if gr < GROWTH_THRESHOLD:
                remove_ids.append(cid)
                logger.info(f"标记为不活跃: {row.get('name')} ({cid}), growth={gr:.2%}")
        except Exception as e:
            logger.warning(f"检查频道活跃度失败: {cid} - {e}")
            continue
    
    if remove_ids:
        # 保存不活跃频道记录
        inactive_df = df[df["id"].isin(remove_ids)]
        inactive_file = "inactive_channels.csv"
        header = not os.path.exists(inactive_file)
        inactive_df.to_csv(inactive_file, mode="a", header=header, index=False)
        
        # 从主列表中移除
        df = df[~df["id"].isin(remove_ids)]
        write_atomic_csv(MONITOR_FILE, df)
        
        # 删除历史文件
        for cid in remove_ids:
            hist_file = os.path.join(HISTORY_DIR, f"{cid}.csv")
            if os.path.exists(hist_file):
                os.remove(hist_file)
                logger.info(f"已删除历史文件: {hist_file}")
        
        logger.info(f"已移除 {len(remove_ids)} 个不活跃频道")