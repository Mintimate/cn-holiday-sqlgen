# -*- coding: utf-8 -*-
"""
中国节假日SQL生成器

本程序基于 chinese_calendar 库自动识别中国法定节假日、工作日、周末和补班日，
并生成标准的数据库 INSERT 语句和 CSV 数据文件，用于工作日历系统。

主要功能：
    - 自动识别工作日、周末、节假日和补班日
    - 生成 SQL INSERT 语句（支持 Oracle 等数据库）
    - 导出 CSV 格式数据文件
    - 支持 YAML 配置文件管理
    - 完善的日志输出和错误处理

使用方法：
    1. 配置 config.yaml 文件中的参数
    2. 运行: python main.py
    3. 在指定目录下获取生成的 .sql 和 .csv 文件

注意事项：
    - 需要在公网环境运行以同步最新节假日数据
    - 次年数据通常在前一年11月后可用（如：2025年数据需在2024年11月后获取）
    - 建议定期更新 chinesecalendar 库: pip install -U chinesecalendar

Author: Mintimate
Created: 2024-07-26
Updated: 2025-11-04
Version: 2.0
License: MIT
"""

import datetime
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple

import chinese_calendar as calendar
import pandas as pd
import yaml


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class DateTypeConfig:
    """日期类型配置数据类"""
    code: str
    description: str


@dataclass
class Config:
    """应用配置数据类"""
    table_name: str
    target_year: int
    save_path: str
    date_types: dict


class ConfigLoader:
    """配置加载器"""
    
    @staticmethod
    def load_config(config_path: str = "config.yaml") -> Config:
        """
        加载配置文件
        
        Args:
            config_path: 配置文件路径
            
        Returns:
            Config: 配置对象
        """
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f)
            
            return Config(
                table_name=config_data['database']['table_name'],
                target_year=config_data['generation']['target_year'],
                save_path=config_data['generation']['save_path'],
                date_types=config_data['date_types']
            )
        except FileNotFoundError:
            logger.error(f"配置文件 {config_path} 不存在")
            raise
        except KeyError as e:
            logger.error(f"配置文件缺少必要字段: {e}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"配置文件格式错误: {e}")
            raise


class DateTypeJudge:
    """日期类型判断器"""
    
    def __init__(self, config: Config):
        """
        初始化日期类型判断器
        
        Args:
            config: 配置对象
        """
        self.config = config
        self.workday_code = config.date_types['workday']['code']
        self.weekend_code = config.date_types['weekend']['code']
        self.holiday_code = config.date_types['holiday']['code']
        self.working_holiday_code = config.date_types['working_holiday']['code']
    
    def judge_date_type(self, date_str: str) -> Tuple[str, str]:
        """
        判断日期的类型
        
        Args:
            date_str: 日期字符串，格式为 YYYY-MM-DD
            
        Returns:
            Tuple[str, str]: (日期类型代码, 备注信息)
        """
        date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
        
        if calendar.is_holiday(date):
            on_holiday, holiday_name = calendar.get_holiday_detail(date)
            if holiday_name is not None:
                logger.info(f"{date_str} 是节假日: {holiday_name}")
                return self.holiday_code, str(holiday_name)
            else:
                logger.info(f"{date_str} 是普通周末")
                return self.weekend_code, ""
        elif calendar.is_workday(date):
            on_holiday, holiday_name = calendar.get_holiday_detail(date)
            if holiday_name is not None:
                logger.info(f"{date_str} 是补班日: {holiday_name}")
                return self.working_holiday_code, str(holiday_name)
            else:
                return self.workday_code, ""
        else:
            logger.error(f"{date_str} 没有匹配到任何类型")
            raise ValueError(f"无法判断日期类型: {date_str}")


class CalendarGenerator:
    """日历生成器"""
    
    def __init__(self, config: Config):
        """
        初始化日历生成器
        
        Args:
            config: 配置对象
        """
        self.config = config
        self.date_judge = DateTypeJudge(config)
    
    @staticmethod
    def get_whole_year(year: int) -> List[str]:
        """
        获取一年内所有的日期
        
        Args:
            year: 年份
            
        Returns:
            List[str]: 日期字符串列表
        """
        begin = datetime.date(year, 1, 1)
        end = datetime.date(year, 12, 31)
        delta = datetime.timedelta(days=1)
        
        days = []
        current = begin
        while current <= end:
            days.append(current.strftime("%Y-%m-%d"))
            current += delta
        
        return days
    
    @staticmethod
    def ensure_dir_exists(dir_path: str, file_name: str = None) -> str:
        """
        确保目录存在，不存在则创建并返回绝对路径
        
        Args:
            dir_path: 目录路径
            file_name: 文件名（可选）
            
        Returns:
            str: 绝对路径
        """
        path = Path(dir_path)
        path.mkdir(parents=True, exist_ok=True)
        
        abs_path = path.absolute()
        if file_name is not None:
            abs_path = abs_path / file_name
        
        return str(abs_path)
    
    def generate_sql(self, year: int, date: str, date_type: str, 
                    remark: str) -> str:
        """
        生成单条SQL插入语句
        
        Args:
            year: 年份
            date: 日期
            date_type: 日期类型代码
            remark: 备注信息
            
        Returns:
            str: SQL语句
        """
        # 转义单引号
        escaped_remark = re.sub(r"'", "''", remark)
        
        sql = (
            f"INSERT INTO {self.config.table_name} VALUES ("
            f"'{year}', "
            f"'{date}', "
            f"'{date_type}', "
            f"'{escaped_remark}'"
            f");\n"
        )
        return sql
    
    def generate(self) -> None:
        """生成工作日历SQL和CSV文件"""
        logger.info(f"开始生成 {self.config.target_year} 年的工作日历数据")
        
        # 初始化数据容器
        df = pd.DataFrame(columns=['YEAR', 'CALENDAR_DATE', 'DATE_TYPE', 'COMMENTS'])
        sql_statements = []
        
        # 获取全年日期并处理
        dates = self.get_whole_year(self.config.target_year)
        for index, date_str in enumerate(dates):
            date_type, remark = self.date_judge.judge_date_type(date_str)
            
            # 生成SQL
            sql = self.generate_sql(
                self.config.target_year, 
                date_str, 
                date_type, 
                remark
            )
            sql_statements.append(sql)
            
            # 添加到DataFrame
            df.loc[index] = [self.config.target_year, date_str, date_type, remark]
        
        # 保存文件
        self._save_files(sql_statements, df)
        
        logger.info("生成完成！")
        logger.info(f"\n{df}")
    
    def _save_files(self, sql_statements: List[str], df: pd.DataFrame) -> None:
        """
        保存SQL和CSV文件
        
        Args:
            sql_statements: SQL语句列表
            df: 数据DataFrame
        """
        file_base_path = self.ensure_dir_exists(
            self.config.save_path,
            f"{self.config.target_year}Day"
        )
        
        # 保存SQL文件
        sql_file = f"{file_base_path}.sql"
        with open(sql_file, 'w', encoding='utf-8') as f:
            f.writelines(sql_statements)
        logger.info(f"SQL文件已保存: {sql_file}")
        
        # 保存CSV文件
        csv_file = f"{file_base_path}.csv"
        df.to_csv(csv_file, index=False, encoding='utf-8-sig')
        logger.info(f"CSV文件已保存: {csv_file}")


def main():
    """主函数"""
    try:
        # 加载配置
        config = ConfigLoader.load_config()
        
        # 创建生成器并执行
        generator = CalendarGenerator(config)
        generator.generate()
        
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        raise


if __name__ == "__main__":
    main()
