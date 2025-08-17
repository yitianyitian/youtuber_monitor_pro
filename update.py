#!/usr/bin/env python3
"""更新频道数据入口"""
from utils import ensure_dirs, logger,deduplicate_csv
from monitor import update_channel_data

if __name__ == "__main__":
    ensure_dirs()
    logger.info("开始更新频道数据")
    # 本地每次更新channel要执行一遍去重
    # deduplicate_csv("channels.csv", "channels.csv", "url")
    update_channel_data()
    logger.info("频道数据更新完成")