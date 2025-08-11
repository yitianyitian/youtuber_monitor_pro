import os
import time
import random
import logging
import functools
import chardet
from httplib2 import ProxyInfo, Http, socks
import pandas as pd
from datetime import datetime, timedelta
from googleapiclient.discovery import build
from config import API_KEY, PROXY, REQUEST_TIMEOUT, HISTORY_DIR, MAX_HISTORY_RECORDS,LOG_LEVEL

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
youtube = build("youtube", "v3", developerKey=API_KEY, http=http)

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
            raise ValueError(f"channels.csv 必须包含列: {col}")
    
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