#!/usr/bin/env python3
"""月度增长报告"""
from generate_report import generate_report
from utils import logger

if __name__ == "__main__":
    logger.info("开始生成月度增长报告...")
    generate_report(days=30, report_type="monthly")
    logger.info("月度报告生成流程完成")