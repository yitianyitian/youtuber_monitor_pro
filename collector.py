import os
import time
import json
import logging
import pandas as pd
import isodate
from googleapiclient.errors import HttpError
from datetime import datetime, timedelta
from config import (CHANNEL_FILE,HISTORY_DIR,SEARCH_KEYWORDS, COLLECT_DAYS, MIN_SUBS, MAX_SUBS, RESULT_LIMIT,
                   MIN_VIEW_SUB_RATIO, MIN_HOT_RATIO, CACHE_DAYS)
from utils import youtube, ensure_dirs, logger, parse_channel_id, read_channel_list, write_atomic_csv
from utils import retry, append_history, save_keyword_score, get_channel_history

# ==== 新增：API 配额统计 ====
API_QUOTA_LIMIT = 10000       # 每日总配额
API_QUOTA_SAFE_LIMIT = 9000   # 安全阈值，超出则停止
quota_used = 0

def add_quota(cost):
    """累加配额消耗并检测是否超限"""
    global quota_used
    quota_used += cost
    logger.info(f"当前已用配额: {quota_used}/{API_QUOTA_LIMIT}")
    if quota_used >= API_QUOTA_SAFE_LIMIT:
        logger.warning("API配额接近上限，停止后续请求")

# 获取视频统计数据（带缓存）
def get_video_stats_cached(channel_id: str):
    from datetime import datetime, timedelta
    
    cache_dir = os.path.join(HISTORY_DIR, "video_cache")
    os.makedirs(cache_dir, exist_ok=True)
    cache_file = os.path.join(cache_dir, f"{channel_id}.json")
    now = datetime.utcnow()

    # 尝试读取缓存
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            cache_time = datetime.fromisoformat(data["timestamp"])
            if now - cache_time < timedelta(days=CACHE_DAYS):
                return data["avg_views"], data["view_sub_ratio"], data["hot_ratio"]
        except Exception:
            pass

    # 缓存不存在或过期 → 重新查询
    try:
        # 获取频道信息
        add_quota(1)  # channels.list 每频道1
        resp = youtube.channels().list(
            part="contentDetails,statistics", 
            id=channel_id
        ).execute()
        
        if not resp.get("items"):
            return 0, 0, 0
            
        uploads = resp["items"][0]
        playlist_id = uploads["contentDetails"]["relatedPlaylists"]["uploads"]
        subs = int(uploads["statistics"].get("subscriberCount", 0))
        
        # 获取最近视频
        videos = []
        next_page_token = None
        while len(videos) < 20:  # 最多获取20个视频
            add_quota(1)  # playlistItems.list 每次调用固定1
            req = youtube.playlistItems().list(
                part="contentDetails",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            )
            resp = req.execute()
            for item in resp.get("items", []):
                videos.append(item["contentDetails"]["videoId"])
            next_page_token = resp.get("nextPageToken")
            if not next_page_token:
                break
        
        # 获取视频统计数据
        view_counts = []
        for i in range(0, len(videos), 50):
            batch_ids = videos[i:i+50]
            add_quota(len(batch_ids))  # videos.list 每视频1
            resp = youtube.videos().list(
                part="statistics",
                id=",".join(batch_ids)
            ).execute()
            for item in resp.get("items", []):
                view_counts.append(int(item["statistics"].get("viewCount", 0)))
        
        # 计算指标
        avg_views = sum(view_counts) / len(view_counts) if view_counts else 0
        hot_ratio = sum(1 for v in view_counts if v > avg_views * 2) / len(view_counts) if view_counts else 0
        view_sub_ratio = avg_views / subs if subs > 0 else 0
        
        # 写入缓存
        cache_data = {
            "timestamp": now.isoformat(),
            "avg_views": avg_views,
            "view_sub_ratio": view_sub_ratio,
            "hot_ratio": hot_ratio
        }
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(cache_data, f)
        
        return avg_views, view_sub_ratio, hot_ratio
        
    except HttpError as e:
        if e.resp.status == 403:
            logger.error("API配额耗尽或密钥无效")
        else:
            logger.warning(f"获取视频统计数据失败: {e}")
        return 0, 0, 0
    except Exception as e:
        logger.warning(f"获取视频统计数据异常: {e}")
        return 0, 0, 0

# 收集潜力频道
@retry(Exception, tries=3, delay=2, backoff=2)
def collect_potential_channels():
    if not SEARCH_KEYWORDS:
        logger.info("未配置搜索关键词，跳过潜力频道收集")
        return
    
    all_channel_stats = {}
    
    for kw in SEARCH_KEYWORDS:
        logger.info(f"开始收集潜力频道，关键词: {kw}")
        published_after = (datetime.utcnow() - timedelta(days=COLLECT_DAYS)).isoformat("T") + "Z"

        videos = []
        next_page_token = None
        try:
            while len(videos) < RESULT_LIMIT:
                add_quota(100)  # search.list 每次调用100
                request = youtube.search().list(
                    part="id,snippet",
                    q=kw,
                    type="video",
                    maxResults=50,
                    order="viewCount",
                    publishedAfter=published_after,
                    pageToken=next_page_token
                )
                response = request.execute()
                
                for item in response.get("items", []):
                    videos.append({
                        "videoId": item["id"]["videoId"],
                        "channelId": item["snippet"]["channelId"],
                        "channelTitle": item["snippet"]["channelTitle"]
                    })
                
                next_page_token = response.get("nextPageToken")
                if not next_page_token or len(videos) >= RESULT_LIMIT:
                    break
                
                time.sleep(1.0)  # 避免请求过快
        except HttpError as e:
            if e.resp.status == 403:
                logger.error("API配额耗尽或密钥无效")
                break
            else:
                logger.warning(f"搜索视频失败: {e}")
                continue
        except StopIteration:
            break

        total_videos = len(videos)
        logger.info(f"[{kw}] 搜索到 {total_videos} 个视频")

        # 过滤短视频
        long_videos = []
        for i in range(0, len(videos), 50):
            batch_ids = [v["videoId"] for v in videos[i:i+50]]
            try:
                add_quota(len(batch_ids))  # videos.list 每视频1
                resp = youtube.videos().list(
                    part="contentDetails", 
                    id=",".join(batch_ids)
                ).execute()
                
                for item in resp.get("items", []):
                    duration_sec = isodate.parse_duration(
                        item["contentDetails"]["duration"]
                    ).total_seconds()
                    if duration_sec >= 60:  # 至少1分钟
                        vid = next(v for v in videos if v["videoId"] == item["id"])
                        long_videos.append(vid)
            except Exception as e:
                logger.warning(f"获取视频时长失败: {e}")
        
        videos = long_videos
        logger.info(f"[{kw}] 短视频过滤完成，剩余视频数: {len(videos)}")

        # 提取频道并去重
        channel_ids = list({v["channelId"] for v in videos})
        matched_channels = {}
        
        for i in range(0, len(channel_ids), 50):
            batch_ids = channel_ids[i:i+50]
            try:
                add_quota(len(batch_ids))  # channels.list 每频道1
                resp = youtube.channels().list(
                    part="snippet,statistics", 
                    id=",".join(batch_ids)
                ).execute()
                
                for item in resp.get("items", []):
                    subs = int(item["statistics"].get("subscriberCount", 0))
                    if subs < MIN_SUBS or subs > MAX_SUBS:
                        continue
                    
                    avg_views, view_sub_ratio, hot_ratio = get_video_stats_cached(item["id"])
                    if view_sub_ratio >= MIN_VIEW_SUB_RATIO and hot_ratio >= MIN_HOT_RATIO:
                        matched_channels[item["id"]] = item["snippet"]["title"]
            except Exception as e:
                logger.warning(f"获取频道信息失败: {e}")
                continue

        # 保存到总结果
        all_channel_stats.update(matched_channels)
        
        # 统计数据
        total_channels = len(channel_ids)
        potential_count = len(matched_channels)
        hot_rate = potential_count / max(total_channels, 1)
        
        # 保存关键词评分
        save_keyword_score(
            kw, total_videos, len(videos),
            total_channels, potential_count, hot_rate
        )
        
        logger.info(f"[{kw}] 完成: 频道总数 {total_channels}, 潜力频道 {potential_count}, 爆款率 {hot_rate:.2%}")

    # 更新 channels.csv
    if not all_channel_stats:
        logger.warning("没有符合条件的频道")
        return
    
    try:
        df = read_channel_list(CHANNEL_FILE)
    except FileNotFoundError:
        df = pd.DataFrame(columns=["name", "url", "id", "current_subs", "last_subs"])
    
    added_count = 0
    for cid, name in all_channel_stats.items():
        url = f"https://www.youtube.com/channel/{cid}"
        
        # 如果频道已存在，更新信息
        if cid in df["id"].values:
            df.loc[df["id"] == cid, ["name", "url"]] = [name, url]
            logger.info(f"更新已有频道: {name}")
        else:
            new_row = pd.DataFrame([{
                "name": name, "url": url, "id": cid,
                "current_subs": 0, "last_subs": 0,
                "growth": 0, "growth_rate": 0, "update_time": ""
            }])
            df = pd.concat([df, new_row], ignore_index=True)
            logger.info(f"新增频道: {name}")
            added_count += 1
    
    if added_count > 0:
        write_atomic_csv(CHANNEL_FILE, df)
        logger.info(f"潜力频道收集完成，新增 {added_count} 个频道，当前监控总数: {len(df)}")
    else:
        logger.info("未发现新的潜力频道")