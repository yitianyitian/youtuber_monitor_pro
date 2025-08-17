import requests
import time
import csv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dataclasses import dataclass
from datetime import datetime, date, time as dtime, timedelta

from countries import countries_dict

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
PROXIES = {
    "http": "http://127.0.0.1:7897",
    "https": "http://127.0.0.1:7897",
}

# 错误日志文件
ERROR_LOG = "errors.log"
# 输出数据文件
OUTPUT_FILE = "playboard_channels.csv"

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


def fetch_country(country: str, writer: csv.DictWriter, config: FetchConfig, max_pages=3):
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
                row = {
                    "country": country,
                    "channel_id": channelId,
                    "channel_url": f"https://www.youtube.com/channel/{channelId}",
                    "title": ch.get("name"),
                    "subscribers": subscriberCount,
                }
                if subscriberCount<=300000:
                  writer.writerow(row)

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


def main():
    # 打开 CSV 文件，实时写入
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "country", "channel_id", "channel_url", "title", "subscribers"
        ])
        writer.writeheader()

        # 昨日 8 点作为 period
        period = get_8am_timestamp(1)
        # 活跃的国家或地区
        countries=["AR","AU","AT","BE","BR","CA","CL","CN","CO","CZ","DK","EG","FR","DE","HK","HU","IN","ID","IE","IL","IT","JP","KZ","KR",
                   "MY","MX","MA","NP","NL","NZ","NG","NO","PE","PH","PL","PT","RO","RU","SA","SG","ZA","ES","SE","CH","TW","TH","TR","UA",
                   "US","GB"]
        for country in countries:
          config = FetchConfig(
              period=period,
              periodTypeId=2, #Daily
              indexDimensionId=31,#growth
              indexTypeId=1,#ALL 为1时，indexTarget是国家
              indexTarget=country
          )

          # 采集 US 前 5 页
          fetch_country(country, writer, config, max_pages=2)
          print(country)
          time.sleep(20)

    print(f"\n✅ 完成采集，结果已保存到 {OUTPUT_FILE}")
    print(f"⚠️ 如果有错误，请查看 {ERROR_LOG}")


if __name__ == "__main__":
    main()
