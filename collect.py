#!/usr/bin/env python3
"""收集潜力频道入口"""
from utils import ensure_dirs, logger
from collector import collect_potential_channels

if __name__ == "__main__":
    ensure_dirs()
    logger.info("开始收集潜力频道")
    collect_potential_channels()
    logger.info("潜力频道收集完成")