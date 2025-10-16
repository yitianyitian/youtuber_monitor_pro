#!/usr/bin/env python3
"""周度增长报告"""
from generate_report import generate_report
from utils import logger

if __name__ == "__main__":
    logger.info("开始生成周度增长报告...")
    generate_report(days=7, report_type="weekly")
    logger.info("周度报告生成流程完成")