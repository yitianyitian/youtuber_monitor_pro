#!/usr/bin/env python3
"""清理不活跃频道入口"""
from utils import ensure_dirs, logger
from monitor import remove_inactive_channels

if __name__ == "__main__":
    ensure_dirs()
    logger.info("开始清理不活跃频道")
    remove_inactive_channels()
    logger.info("不活跃频道清理完成")