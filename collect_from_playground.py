import requests
import time
import csv
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dataclasses import dataclass
from datetime import datetime, date, time as dtime, timedelta
from config import PROXY,COLLECT_FILE

# 新增导入
from utils import is_short_video_channel, append_channel_to_csv,is_short_video_channel_from_playboard,add_to_monitor_if_long_video

# Playboard API 基础地址
BASE_URL = "https://lapi.playboard.co/v1/chart/channel"

# 请求头（至少要带 UA）
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/115.0.0.0 Safari/537.36"
}

# 可选代理（如果不用代理，设为 None）
PROXIES = None
if PROXY:
  PROXIES = {
      "http": PROXY,
      "https": PROXY,
  }

# 错误日志文件
ERROR_LOG = "errors.log"

# 创建一个带自动重试的 Session
def create_session():
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

session = create_session()


def log_error(message: str):
    """写入错误日志"""
    with open(ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(message + "\n")


def get_8am_timestamp(days_ago: int = 0) -> int:
    """
    获取指定天数前的 8 点时间戳（本地时间）
    :param days_ago: 0=今天, 1=昨天, 2=前天...
    :return: int 时间戳（秒）
    """
    target_date = date.today() - timedelta(days=days_ago)
    dt = datetime.combine(target_date, dtime(8, 0, 0))
    return int(dt.timestamp())


@dataclass
class FetchConfig:
    period: int
    periodTypeId: int
    indexDimensionId: int
    indexTypeId: int
    indexTarget: int


def fetch_by_country(country: str, config: FetchConfig, max_pages=3):
    """抓取某个国家的排行榜前若干页"""
    cursor = None
    page = 1

    while page <= max_pages:
        print(cursor)
        params = {
            "locale": "en",
            "countryCode": country,
            "period": config.period,
            "size": 20,
            "chartTypeId": 10,
            "periodTypeId": config.periodTypeId,
            "indexDimensionId": config.indexDimensionId,
            "indexTypeId": config.indexTypeId,
            "indexTarget": config.indexTarget,
            "indexCountryCode": country,
        }
        if cursor:
            params['cursor']=cursor
        print(params)
        try:
            resp = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=10, proxies=PROXIES)
            resp.raise_for_status()
            data = resp.json()
            print(data)
            items = data.get("list", [])
            if not items:
                print(f"[End] {country} 没有更多数据")
                break

            for item in items:
                ch = item.get("channel", {})
                channelId = ch.get("channelId")
                subscriberCount=ch.get("subscriberCount")
                
                # 只处理订阅数小于30万的频道
                if subscriberCount <= 300000:
                    row = {
                        "country": country,
                        "channel_id": channelId,
                        "channel_url": f"https://www.youtube.com/channel/{channelId}",
                        "title": ch.get("name"),
                        "subscribers": subscriberCount,
                    }
                    
                    # 检测是否为短视频频道
                    is_short = is_short_video_channel(channelId)
                    print(f"{ch.get('name')} ({channelId}): 短视频频道 - {is_short}")

                    # 使用Playboard提供的视频ID进行检测,使用优化后的is_short_video_channel检测
                    # is_short = is_short_video_channel_from_playboard(item, max_duration=180)
                    # print(f"{ch.get('name')} ({channelId}): 短视频频道 - {is_short}")
                    
                    # 准备写入 channel.csv 的数据
                    channel_data = {
                        "url": f"https://www.youtube.com/channel/{channelId}",
                        "name": ch.get("name"),
                        "id": channelId,
                        "current_subs": subscriberCount,
                        "last_subs": 0,
                        "growth": 0,
                        "growth_rate": 0,
                        "update_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        "short_video": is_short
                    }
                    
                    # 1. 总是添加到收藏列表
                    append_channel_to_csv(channel_data, csv_file=COLLECT_FILE)
                    
                    # 2. 如果是长视频，则添加到监控列表
                    if not is_short:
                        add_to_monitor_if_long_video(channel_data)
                    
                    # 避免请求过快
                    time.sleep(1.2)

            print(f"[OK] {country} Page {page} - {len(items)} channels")

            # 更新 cursor
            new_cursor = data.get("cursor")
            if not new_cursor or new_cursor == cursor:
                print(f"[End] {country} 已到最后一页")
                break
            cursor = new_cursor
            page += 1

        except requests.exceptions.RequestException as e:
            msg = f"[RequestError] {country} Page {page} - {e}"
            print(msg)
            log_error(msg)
            break
        except Exception as e:
            msg = f"[Error] {country} Page {page} - {e}"
            print(msg)
            log_error(msg)
            break

        time.sleep(10)



def get_country_config():
    """
    基于YouTube频道密度和增长潜力的国家配置
    """
    return {
        # 一级市场：高密度+高增长，拉取3页
        "US": (3, 5), 
        "BR": (3, 3), "MX": (3, 3), 
        "JP": (3, 3), "KR": (3, 3), "GB": (3, 3), #(3*5+5)*20
        
        # 二级市场：中等密度，拉取2页
        "DE": (2, 2), "FR": (2, 2), "ES": (2, 2), "IT": (2, 2),
        "AU": (2, 2), "CA": (2, 2),
        "AR": (2, 2), "CO": (2, 2), "CL": (2, 2), "PE": (2, 2),
        "ID": (2, 2), "TH": (2, 2), "MY": (2, 2), "SG": (2, 2),"TW": (2, 2), "HK": (2, 2), #16*20
        
        # 三级市场：低密度但有潜力，拉取1页
        "NL": (1, 1), "SE": (1, 1),
        "IN": (1, 1), "VN" :(1, 1),
        "ZA": (1, 1), "TR": (1, 1), "PL": (1, 1), #7*20
    }


def collect_from_playground():
   # 昨日 8 点作为 period
        period = get_8am_timestamp(1)
        
        country_config = get_country_config()
        for country, (weight, pages) in country_config.items():
          config = FetchConfig(
              period=period,
              periodTypeId=2, #Daily
              indexDimensionId=31,#growth
              indexTypeId=1,#ALL 为1时，indexTarget是国家
              indexTarget=country
          )

          fetch_by_country(country, config, max_pages=pages)
          time.sleep(15 if weight == 3 else 10)  # 根据权重调整间隔
