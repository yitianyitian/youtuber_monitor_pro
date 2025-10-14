import os
import time
import csv
import random
import logging
import functools
import chardet
from httplib2 import ProxyInfo, Http, socks
import pandas as pd
from datetime import datetime, timedelta
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from config import API_KEY, PROXY, REQUEST_TIMEOUT, HISTORY_DIR, MAX_HISTORY_RECORDS,LOG_LEVEL,MONITOR_FILE,COLLECT_FILE

# 初始化日志
def setup_logger():
    logging.basicConfig(
        level=LOG_LEVEL,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    return logging.getLogger("yt-monitor")

logger = setup_logger()

# ---------- HTTP / Google API 客户端 ----------
def create_http_client():
    if PROXY:
        proto, rest = PROXY.split("://", 1)
        host, port = rest.split(":")
        proxy_info = ProxyInfo(
            socks.PROXY_TYPE_HTTP, 
            host, 
            int(port)
        )
        return Http(proxy_info=proxy_info, timeout=REQUEST_TIMEOUT)
    else:
        return Http(timeout=REQUEST_TIMEOUT)

http = create_http_client()
youtube = build("youtube", "v3", developerKey=API_KEY, http=http,cache_discovery=False)

# ---------- 重试装饰器 ----------
def retry(ExceptionToCheck=Exception, tries=3, delay=1, backoff=2):
    def deco_retry(f):
        @functools.wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    msg = f"{e}, Retrying in {mdelay} sec..."
                    logger.warning(msg)
                    time.sleep(mdelay + random.random() * 0.5)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)
        return f_retry
    return deco_retry

# ---------- 文件操作 ----------
def ensure_dirs():
    os.makedirs(HISTORY_DIR, exist_ok=True)
    os.makedirs(os.path.join(HISTORY_DIR, "video_cache"), exist_ok=True)

def write_atomic_csv(path: str, df):
    tmp = f"{path}.tmp"
    df.to_csv(tmp, index=False, encoding='utf-8')
    os.replace(tmp, path)

# ---------- 频道ID解析 ----------
def parse_channel_id(youtube_url: str) -> str:
    url = youtube_url.strip()
    if "/channel/" in url:
        return url.split("/channel/")[1].split("/")[0]
    if "/user/" in url:
        username = url.split("/user/")[1].split("/")[0]
        resp = youtube.channels().list(forUsername=username, part="id").execute()
        items = resp.get("items", [])
        if not items:
            raise ValueError("user -> channel 未找到")
        return items[0]["id"]
    if "/@" in url:
        handle = url.split("/@")[1].split("/")[0]
        resp = youtube.search().list(q=f"@{handle}", type="channel", part="snippet", maxResults=1).execute()
        items = resp.get("items", [])
        if not items:
            raise ValueError("handle -> channel 未找到")
        return items[0]["snippet"]["channelId"]
    return url

# ========== 编码检测函数 ==========
def detect_encoding(file_path):
    """自动检测文件编码"""
    try:
        with open(file_path, 'rb') as f:
            rawdata = f.read(10000)
        result = chardet.detect(rawdata)
        return result['encoding'] or 'utf-8'
    except Exception as e:
        logger.warning(f"编码检测失败: {e}")
        return 'utf-8'

# ========== 安全的CSV读取函数 ==========
def safe_read_csv(file_path: str, dtype=None):
    """安全读取CSV文件，自动处理编码问题"""
    if not os.path.exists(file_path):
        return pd.DataFrame()
    
    detected_encoding = detect_encoding(file_path)
    logger.debug(f"检测到文件编码: {detected_encoding} for {file_path}")
    
    try:
        return pd.read_csv(file_path, encoding=detected_encoding, dtype=dtype)
    except UnicodeDecodeError:
        try:
            return pd.read_csv(file_path, encoding='latin1', dtype=dtype)
        except:
            logger.warning(f"使用错误恢复模式读取文件: {file_path}")
            return pd.read_csv(file_path, encoding='utf-8', errors='replace', dtype=dtype)

# ---------- 频道列表操作 ----------
def read_channel_list(path: str):
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} not found")
    
    # 使用统一的读取函数
    df = safe_read_csv(path, dtype={"id": str})
    
    # 如果读取失败，创建空DataFrame
    if df.empty:
        df = pd.DataFrame(columns=["name", "url", "id", "current_subs", "last_subs"])
    
    # 确保必要的列存在
    for col in ["name", "url"]:
        if col not in df.columns:
            raise ValueError(f"csv 必须包含列: {col}")
    
    # 初始化缺失的列
    if "current_subs" not in df.columns:
        df["current_subs"] = 0
    if "last_subs" not in df.columns:
        df["last_subs"] = 0
    
    return df

# ---------- 历史记录操作 ----------
def append_history(channel_id: str, record_date: str, subs: int):
    ensure_dirs()
    history_file = os.path.join(HISTORY_DIR, f"{channel_id}.csv")
    
    # 使用统一的读取函数
    history_df = safe_read_csv(history_file)
    
    # 如果是空DataFrame，则创建新结构
    if history_df.empty:
        history_df = pd.DataFrame(columns=["date", "subscribers"])
    
    # 添加新记录
    new_record = pd.DataFrame([[record_date, subs]], columns=["date", "subscribers"])
    history_df = pd.concat([history_df, new_record], ignore_index=True)
    
    # 去重并保留最新记录
    history_df = history_df.sort_values("date", ascending=False)
    history_df = history_df.drop_duplicates(subset=["date"])
    history_df = history_df.head(MAX_HISTORY_RECORDS)
    history_df = history_df.sort_values("date")
    
    # 保存文件
    history_df.to_csv(history_file, index=False, encoding='utf-8')

def get_channel_history(channel_id: str, limit: int = None):
    history_file = os.path.join(HISTORY_DIR, f"{channel_id}.csv")
    
    # 使用统一的读取函数
    df = safe_read_csv(history_file)
    
    if df.empty:
        return pd.DataFrame(columns=["date", "subscribers"])
    
    if limit:
        return df.tail(limit)
    return df

# ---------- 关键词评分 ----------
def save_keyword_score(keyword: str, total_videos: int, long_videos: int, 
                      total_channels: int, potential_channels: int, hot_rate: float):
    ensure_dirs()
    score_path = os.path.join(HISTORY_DIR, "keyword_score.csv")
    
    if not os.path.exists(score_path):
        with open(score_path, "w", encoding="utf-8") as f:
            f.write("date,keyword,total_videos,long_videos,total_channels,potential_channels,hot_rate\n")
    
    with open(score_path, "a", encoding="utf-8") as f:
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        f.write(f"{date_str},{keyword},{total_videos},{long_videos},{total_channels},{potential_channels},{hot_rate}\n")



def deduplicate_csv(input_file: str, output_file: str, column_name: str, keep: str = "first"):
    """
    按某一列去重并保存新文件 (CSV)
    """
    # 尝试不同编码读取
    encodings = ["utf-8", "utf-8-sig", "gbk", "latin1"]
    for enc in encodings:
        try:
            df = pd.read_csv(input_file, encoding=enc)
            print(f"成功使用编码 {enc} 读取文件")
            break
        except UnicodeDecodeError:
            continue
    else:
        raise ValueError("无法识别文件编码，请手动检查")

    # 按指定列去重
    df_unique = df.drop_duplicates(subset=[column_name], keep=keep)

    # 保存新文件（用 utf-8-sig 确保 Excel 打开不乱码）
    df_unique.to_csv(output_file, index=False, encoding="utf-8-sig")

    print(f"去重完成，共 {len(df_unique)} 条记录，已保存至 {output_file}")


# ---------- 新增：短视频检测函数 ----------
@retry(Exception, tries=3, delay=1, backoff=2)
def is_short_video_channel(channel_id: str, max_duration: int = 60) -> bool:
    """
    检测频道是否为短视频频道
    :param channel_id: YouTube频道ID
    :param max_duration: 短视频最大时长（秒）
    :return: 是否为短视频频道
    """
    try:
        # 获取频道最新视频
        resp = youtube.search().list(
            part="snippet",
            channelId=channel_id,
            maxResults=5,  # 检查最近5个视频
            order="date",
            type="video"
        ).execute()
        
        video_ids = [item["id"]["videoId"] for item in resp.get("items", []) if "videoId" in item["id"]]

        if not video_ids:
            return False
            
        # 获取视频详情
        video_resp = youtube.videos().list(
            part="contentDetails",
            id=",".join(video_ids)
        ).execute()
        
        # 检查视频时长
        short_video_count = 0
        for item in video_resp.get("items", []):
            duration = item["contentDetails"]["duration"]
            # 解析ISO 8601时长格式
            if duration.startswith("PT"):
                time_str = duration[2:]
                minutes = 0
                seconds = 0
                
                if "M" in time_str:
                    minutes = int(time_str.split("M")[0])
                    time_str = time_str.split("M")[1] if "M" in time_str else time_str
                if "S" in time_str:
                    seconds = int(time_str.split("S")[0])
                
                total_seconds = minutes * 60 + seconds
                if total_seconds <= max_duration:
                    short_video_count += 1
        
        # 如果大多数视频都是短视频，则标记为短视频频道
        return short_video_count >= 3  # 5个中有3个以上是短视频
    except HttpError as e:
        if e.resp.status == 403:
            logger.error("API配额耗尽或密钥无效")
        else:
            logger.warning(f"API 获取视频信息失败: {channel_id} - {e}")
        return False
    except Exception as e:
        logger.warning(f"检测短视频频道失败: {channel_id} - {e}")
        return False

# ---------- 新增：频道数据写入函数 ----------
def append_channel_to_csv(channel_data, csv_file="collect_channels.csv", check_duplicate=True):
    """
    将频道数据追加到CSV文件，确保不重复添加相同频道
    :param channel_data: 字典，包含频道信息
    :param csv_file: CSV文件路径
    :param check_duplicate: 是否检查重复
    """
    ensure_dirs()
    
    # 检查文件是否存在，决定是否写入表头
    file_exists = os.path.isfile(csv_file)
    
    # 如果文件存在且需要检查重复，检查是否已存在相同频道
    if file_exists and check_duplicate:
        try:
            # 读取现有数据
            existing_df = safe_read_csv(csv_file)
            
            # 检查是否已存在相同ID的频道
            channel_id = channel_data.get("id")
            if channel_id and not existing_df.empty and "id" in existing_df.columns:
                if channel_id in existing_df["id"].values:
                    logger.info(f"频道已存在，跳过: {channel_data.get('name', '未知')} ({channel_id})")
                    return False  # 返回False表示未添加
        except Exception as e:
            logger.warning(f"读取现有频道文件失败: {e}")
    
    # 确保所有必需的列都存在
    required_columns = ["url", "name", "id", "current_subs", "last_subs", 
                       "growth", "growth_rate", "update_time", "short_video"]
    
    # 为缺失的列提供默认值
    for col in required_columns:
        if col not in channel_data:
            if col == "short_video":
                channel_data[col] = False
            elif col in ["current_subs", "last_subs", "growth"]:
                channel_data[col] = 0
            elif col == "growth_rate":
                channel_data[col] = 0.0
            elif col == "update_time":
                channel_data[col] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            else:
                channel_data[col] = ""
    
    with open(csv_file, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=required_columns)
        
        if not file_exists:
            writer.writeheader()
        
        writer.writerow(channel_data)
    
    logger.info(f"已添加频道到 {csv_file}: {channel_data.get('name', '未知')}")
    return True  # 返回True表示成功添加

# 新增函数：添加频道到监控列表（仅长视频）
def add_to_monitor_if_long_video(channel_data):
    """
    如果频道是长视频，则添加到监控列表
    """
    # 检查是否为短视频
    is_short = channel_data.get("short_video", False)
    
    if not is_short:
        # 添加到监控列表
        success = append_channel_to_csv(
            channel_data, 
            csv_file=MONITOR_FILE, 
            check_duplicate=True
        )
        if success:
            logger.info(f"已添加到监控列表: {channel_data.get('name', '未知')}")
        return success
    else:
        logger.info(f"跳过短视频频道，不添加到监控列表: {channel_data.get('name', '未知')}")
        return False

# 新增函数：从收藏列表同步长视频到监控列表
def sync_long_videos_to_monitor():
    """
    从收藏列表同步所有长视频频道到监控列表
    """
    try:
        collect_df = safe_read_csv(COLLECT_FILE)
        if collect_df.empty:
            logger.warning("收藏列表为空")
            return 0
        
        # 过滤长视频频道
        long_video_df = collect_df[collect_df["short_video"] == False]
        
        if long_video_df.empty:
            logger.info("没有长视频频道需要同步")
            return 0
        
        # 读取现有监控列表
        monitor_df = safe_read_csv(MONITOR_FILE)
        
        # 找出需要添加的频道（在收藏列表中但不在监控列表中）
        if not monitor_df.empty and "id" in monitor_df.columns:
            existing_ids = set(monitor_df["id"].values)
            new_channels = long_video_df[~long_video_df["id"].isin(existing_ids)]
        else:
            new_channels = long_video_df
        
        # 添加到监控列表
        added_count = 0
        for _, row in new_channels.iterrows():
            channel_data = row.to_dict()
            success = append_channel_to_csv(
                channel_data, 
                csv_file=MONITOR_FILE, 
                check_duplicate=False  # 已经检查过了
            )
            if success:
                added_count += 1
        
        logger.info(f"同步完成: 新增 {added_count} 个长视频频道到监控列表")
        return added_count
        
    except Exception as e:
        logger.error(f"同步长视频频道失败: {e}")
        return 0


@retry(Exception, tries=3, delay=1, backoff=2)
def is_short_video_channel_from_playboard(item_data: dict, max_duration: int = 180) -> bool:
    """
    使用Playboard返回的视频ID通过YouTube API检测短视频频道
    :param item_data: Playboard返回的单个频道完整数据
    :param max_duration: 短视频最大时长（秒），默认3分钟
    :return: 是否为短视频频道
    """
    try:
        # 从Playboard数据中提取视频ID
        videos = item_data.get("videos", [])
        if not videos:
            return True
            
        video_ids = [video.get("videoId") for video in videos if video.get("videoId")]
        if not video_ids:
            return True
            
        # 获取视频详情
        video_resp = youtube.videos().list(
            part="contentDetails",
            id=",".join(video_ids)
        ).execute()
        
        # 检查视频时长
        short_video_count = 0
        for item in video_resp.get("items", []):
            duration = item["contentDetails"]["duration"]
            # 解析ISO 8601时长格式
            total_seconds = parse_duration_to_seconds(duration)
            if total_seconds <= max_duration:
                short_video_count += 1
        
        # 如果至少有一个视频是短视频，则标记为短视频频道
        return short_video_count >= 1
        
    except HttpError as e:
        if e.resp.status == 403:
            logger.error("API配额耗尽或密钥无效")
        else:
            logger.warning(f"API 获取视频信息失败: {e}")
        return False
    except Exception as e:
        logger.warning(f"检测短视频频道失败: {e}")
        return False

def parse_duration_to_seconds(duration: str) -> int:
    """
    将ISO 8601时长格式转换为秒数
    :param duration: ISO 8601格式的时长字符串
    :return: 总秒数
    """
    if not duration.startswith("PT"):
        return 0
        
    time_str = duration[2:]
    hours = 0
    minutes = 0
    seconds = 0
    
    if "H" in time_str:
        hours = int(time_str.split("H")[0])
        time_str = time_str.split("H")[1] if "H" in time_str else ""
    if "M" in time_str:
        minutes = int(time_str.split("M")[0])
        time_str = time_str.split("M")[1] if "M" in time_str else ""
    if "S" in time_str:
        seconds = int(time_str.split("S")[0])
    
    return hours * 3600 + minutes * 60 + seconds