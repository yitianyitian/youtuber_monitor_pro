import os

# ---------- 全局配置 ----------
API_KEY = os.getenv("YOUTUBE_API_KEY") #AIzaSyAXH1tds1nmuLYFTRcbNU_9UEQ0brmjWtQ
CHANNEL_FILE = os.getenv("CHANNEL_FILE", "channels.csv")
HISTORY_DIR = os.getenv("HISTORY_DIR", "history")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "10"))
MAX_HISTORY_RECORDS = int(os.getenv("MAX_HISTORY_RECORDS", "7"))
PROXY =os.getenv("HTTP_PROXY")  # 如 http://127.0.0.1:7890

# 告警配置
ALERT_EMAIL = os.getenv("ALERT_EMAIL") #julieblue0320@gmail.com
EMAIL_APP_PASSWORD = os.getenv("EMAIL_APP_PASSWORD") #crjqwtzsivyytfml
MIN_GROWTH_RATE = float(os.getenv("MIN_GROWTH_RATE", "10"))  # 百分比阈值

# 收集器配置
SEARCH_KEYWORDS = [kw.strip() for kw in os.getenv("SEARCH_KEYWORDS", "").split(",") if kw.strip()]
COLLECT_DAYS = int(os.getenv("COLLECT_DAYS", "30"))
MIN_SUBS = int(os.getenv("MIN_SUBS", "10000"))
MAX_SUBS = int(os.getenv("MAX_SUBS", "300000"))
RESULT_LIMIT = int(os.getenv("RESULT_LIMIT", "100"))
MIN_VIEW_SUB_RATIO = float(os.getenv("MIN_VIEW_SUB_RATIO", "0.5"))
MIN_HOT_RATIO = float(os.getenv("MIN_HOT_RATIO", "0.2"))
CACHE_DAYS = int(os.getenv("CACHE_DAYS", "3"))

# 不活跃频道配置
MIN_INACTIVE_DAYS = int(os.getenv("MIN_INACTIVE_DAYS", "7"))
GROWTH_THRESHOLD = float(os.getenv("GROWTH_THRESHOLD", "0.01"))

# 日志配置
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

print(API_KEY)
print(ALERT_EMAIL)
print(EMAIL_APP_PASSWORD)
print(PROXY)