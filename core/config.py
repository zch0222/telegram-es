# config.py
import os
from dotenv import load_dotenv

# 在模块加载时执行，从项目根目录的.env文件加载环境变量
# [5, 6]
load_dotenv()

class Config:
    """
    应用程序配置类，从环境变量中加载所有必要的设置。
    """
    # Telegram API 配置
    API_ID = os.getenv('API_ID')
    API_HASH = os.getenv('API_HASH')
    SESSION_NAME = os.getenv('SESSION_NAME', 'telegram_scraper')

    # MySQL 数据库连接配置
    MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
    MYSQL_PORT = int(os.getenv('MYSQL_PORT', 3306))
    MYSQL_USER = os.getenv('MYSQL_USER')
    MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
    MYSQL_DB = os.getenv('MYSQL_DB')

    # Elasticsearch 连接配置
    ES_HOSTS = os.getenv('ES_HOSTS', 'http://localhost:9200').split(',')
    ES_INDEX_NAME = os.getenv('ES_INDEX_NAME', 'telegram_messages')

    # 性能调优参数
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', 1000))
    QUEUE_MAX_SIZE = int(os.getenv('QUEUE_MAX_SIZE', 10000))
    MYSQL_CONSUMERS = int(os.getenv('MYSQL_CONSUMERS', 2))
    ES_CONSUMERS = int(os.getenv('ES_CONSUMERS', 2))

# 创建一个全局可用的配置实例
config = Config()