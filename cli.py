import os
import re
import argparse
import asyncio
import pymysql
from telethon import TelegramClient, events
from telethon.tl.types import Message
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import signal
import sys
from datetime import datetime

load_dotenv()  # 加载.env文件中的环境变量

# 从环境变量获取配置
API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
ES_HOST = os.getenv('ES_HOST')
MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASS = os.getenv('MYSQL_PASSWORD')
MYSQL_DB = os.getenv('MYSQL_DATABASE')

print(ES_HOST)

# 初始化Elasticsearch和MySQL连接
es = Elasticsearch(ES_HOST, timeout=120)
mysql_conn = pymysql.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASS,
    database=MYSQL_DB,
    charset='utf8mb4',
    connect_timeout=12000,
    read_timeout=12000,
    write_timeout=12000,
)

# 全局变量用于优雅退出
running = True

def signal_handler(signum, frame):
    global running
    print("\n收到退出信号，正在优雅关闭...")
    running = False

# 注册信号处理器
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# MySQL连接管理
def ensure_mysql_connection():
    """确保MySQL连接有效，如果断开则重新连接"""
    global mysql_conn
    try:
        mysql_conn.ping(reconnect=True)
    except Exception as e:
        print(f"MySQL连接断开，正在重新连接: {e}")
        mysql_conn = pymysql.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASS,
            database=MYSQL_DB,
            charset='utf8mb4',
            connect_timeout=12000,
            read_timeout=12000,
            write_timeout=12000,
        )

# ... existing code ...

# 订阅新消息的处理函数
async def handle_new_message(event, chat_name):
    """处理新消息事件"""
    global running
    if not running:
        return
        
    try:
        message = event.message
        if not isinstance(message, Message):
            return
            
        # 处理消息
        doc = process_message(message, chat_name)
        
        # 保存到数据库和ES
        save_to_es([doc])
        save_to_mysql([doc])
        
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 新消息已保存: {chat_name} - ID:{message.id}")
        if doc['message']:
            print(f"内容预览: {doc['message'][:100]}...")
            
    except Exception as e:
        print(f"处理新消息时出错: {e}")

# 订阅频道/群组新消息的主函数
async def subscribe_messages(client, chat_names):
    """订阅指定频道/群组的新消息"""
    print(f"开始订阅以下频道/群组的新消息: {', '.join(chat_names)}")
    
    # 验证频道/群组是否存在并获取实体
    entities = {}
    for chat_name in chat_names:
        try:
            entity = await client.get_entity(chat_name)
            entities[chat_name] = entity
            print(f"✓ 成功连接到: {entity.title} ({chat_name})")
        except Exception as e:
            print(f"✗ 无法连接到 {chat_name}: {e}")
            return
    
    # 为每个频道/群组注册新消息事件处理器
    for chat_name, entity in entities.items():
        @client.on(events.NewMessage(chats=entity))
        async def new_message_handler(event):
            await handle_new_message(event, chat_name)
    
    print("订阅设置完成，开始监听新消息...")
    print("按 Ctrl+C 停止监听")
    
    # 保持运行状态
    try:
        while running:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\n收到中断信号，正在停止...")
    finally:
        print("订阅已停止")

# 批量订阅功能
async def subscribe_batch_messages(client, chat_names):
    """批量订阅多个频道/群组的新消息"""
    print(f"开始批量订阅 {len(chat_names)} 个频道/群组的新消息")
    
    # 验证所有频道/群组
    valid_entities = {}
    for chat_name in chat_names:
        try:
            entity = await client.get_entity(chat_name.strip())
            valid_entities[chat_name.strip()] = entity
            print(f"✓ {entity.title} ({chat_name.strip()})")
        except Exception as e:
            print(f"✗ 跳过无效的频道/群组 {chat_name}: {e}")
    
    if not valid_entities:
        print("没有有效的频道/群组可以订阅")
        return
    
    # 注册事件处理器
    @client.on(events.NewMessage(chats=list(valid_entities.values())))
    async def batch_message_handler(event):
        # 找到对应的chat_name
        chat_id = event.chat_id
        chat_name = None
        for name, entity in valid_entities.items():
            if entity.id == chat_id:
                chat_name = name
                break
        
        if chat_name:
            await handle_new_message(event, chat_name)
    
    print(f"成功订阅 {len(valid_entities)} 个频道/群组")
    print("开始监听新消息... 按 Ctrl+C 停止")
    
    # 保持运行
    try:
        while running:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\n收到中断信号，正在停止...")
    finally:
        print("批量订阅已停止")

# ... existing code ...

# 修改存储到MySQL的函数
def save_to_mysql(docs):
    ensure_mysql_connection()  # 确保连接有效
    with mysql_conn.cursor() as cursor:
        sql = """
              INSERT INTO telegram_message (message_id, chat_id, message, date, sender_id, views, forwards, media_type, message_link) \
              VALUES (%(message_id)s, %(chat_id)s, %(message)s, %(date)s, \
                      %(sender_id)s, %(views)s, %(forwards)s, %(media_type)s, %(message_link)s) ON DUPLICATE KEY \
              UPDATE \
                  message = \
              VALUES (message), views = \
              VALUES (views), forwards = \
              VALUES (forwards), media_type = \
              VALUES (media_type) \
              """
        cursor.executemany(sql, docs)
    mysql_conn.commit()

# ... existing code ...

# 创建Elasticsearch索引（带中文分词）
def create_es_index():
    index_name = "telegram_messages"
    if not es.indices.exists(index=index_name):
        es.indices.create(
            index=index_name,
            # body={
            #     "settings": {
            #         "analysis": {
            #             "analyzer": {
            #                 "ik_pinyin_analyzer": {
            #                     "type": "custom",
            #                     "tokenizer": "ik_max_word",
            #                     "filter": ["pinyin_filter"]
            #                 }
            #             },
            #             "filter": {
            #                 "pinyin_filter": {
            #                     "type": "pinyin",
            #                     "keep_original": True,
            #                     "keep_first_letter": True,
            #                     "keep_separate_first_letter": True
            #                 }
            #             }
            #         }
            #     },
            #     "mappings": {
            #         "properties": {
            #             "id": {"type": "long"},
            #             "chat_id": {"type": "keyword"},
            #             "message": {
            #                 "type": "text",
            #                 "analyzer": "ik_pinyin_analyzer",
            #                 "fields": {
            #                     "keyword": {"type": "keyword"}
            #                 }
            #             },
            #             "date": {"type": "date"},
            #             "sender_id": {"type": "long"},
            #             "views": {"type": "integer"},
            #             "forwards": {"type": "integer"},
            #             "media_type": {"type": "keyword"},
            #             "message_link": {"type": "keyword"},
            #         }
            #     }
            # }
            body= {
              "settings": {
                "analysis": {
                  "analyzer": {
                    "my_analyzer": {
                      "tokenizer": "ik_max_word",
                      "filter": "py"
                    }
                  },
                  "filter": {
                    "py": {
                      "type": "pinyin",
                      "keep_full_pinyin": False,
                      "keep_joined_full_pinyin": True,
                      "keep_original": True,
                      "limit_first_letter_length": 16,
                      "remove_duplicated_term": True,
                      "none_chinese_pinyin_tokenize": False
                    }
                  }
                }
              },
              "mappings": {
                "properties": {
                  "id": {
                    "type": "long"
                  },
                  "chat_id": {
                    "type": "keyword"
                  },

                  "message": {
                    "type": "text",
                    "analyzer": "my_analyzer",
                    "search_analyzer": "ik_max_word",
                    "copy_to": ["all", "suggest"]
                  },
                  "date": {"type": "date"},
                  "sender_id": {"type": "long"},
                  "views": {"type": "integer"},
                  "forwards": {"type": "integer"},
                  "media_type": {"type": "keyword"},
                  "message_link": {"type": "keyword"},
                  "all": {
                    "type": "text",
                    "search_analyzer": "ik_max_word",
                    "analyzer": "my_analyzer"
                  },
                  "suggest": {
                    "type": "completion",
                    "analyzer": "my_analyzer",
                    "search_analyzer": "ik_max_word"
                  }
                }
              }
            }
        )
        print(f"Created Elasticsearch index: {index_name}")


# 创建MySQL表
def create_mysql_table():
    with mysql_conn.cursor() as cursor:
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS telegram_message
                       (
                           id
                           BIGINT
                           UNSIGNED
                           NOT
                           NULL
                           AUTO_INCREMENT
                           COMMENT
                           '自增主键',
                           message_id
                           BIGINT
                           NOT
                           NULL
                           COMMENT
                           'Telegram消息ID',
                           chat_id
                           VARCHAR(300)
                           NOT
                           NULL
                           COMMENT
                           '群组/频道ID',
                           message
                           TEXT
                           CHARACTER
                           SET
                           utf8mb4
                           COLLATE
                           utf8mb4_unicode_ci
                           COMMENT
                           '消息内容',
                           date
                           DATETIME
                           NOT
                           NULL
                           COMMENT
                           '消息发送时间',
                           sender_id
                           BIGINT
                           COMMENT
                           '发送者ID',
                           views
                           INT
                           COMMENT
                           '查看次数',
                           forwards
                           INT
                           COMMENT
                           '转发次数',
                           media_type
                           VARCHAR
                       (
                           800
                       ) COMMENT '媒体类型(photo/video/document等)',
                           message_link VARCHAR(800) COMMENT '消息链接',
                           PRIMARY KEY
                       (
                           id
                       ),
                           UNIQUE KEY uniq_msg
                       (
                           chat_id,
                           message_id
                       ),
                           KEY idx_chat
                       (
                           chat_id
                       ),
                           KEY idx_date
                       (
                           date
                       ),
                           KEY idx_sender
                       (
                           sender_id
                       )
                           ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='存储Telegram消息'
                       """)
    mysql_conn.commit()
    print("Created MySQL table")

# 修改主函数以支持新的订阅功能
async def main():
    parser = argparse.ArgumentParser(description='Telegram消息爬取和订阅工具')
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # 原有的爬取命令
    scrape_parser = subparsers.add_parser('scrape', help='爬取历史消息')
    scrape_parser.add_argument('--chats', required=True, help='逗号分隔的频道/群组ID')
    scrape_parser.add_argument('--start', type=int, required=True, help='起始消息ID')
    scrape_parser.add_argument('--end', type=int, required=True, help='结束消息ID')
    
    # 新增的订阅命令
    subscribe_parser = subparsers.add_parser('subscribe', help='订阅新消息')
    subscribe_parser.add_argument('--chat', required=True, help='要订阅的频道/群组名称或ID')
    
    # 新增的批量订阅命令
    batch_subscribe_parser = subparsers.add_parser('batch-subscribe', help='批量订阅多个频道/群组的新消息')
    batch_subscribe_parser.add_argument('--chats', required=True, help='逗号分隔的频道/群组名称或ID列表')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # 创建数据库结构
    create_es_index()
    create_mysql_table()
    
    async with TelegramClient('session_name', API_ID, API_HASH) as client:
        if args.command == 'scrape':
            # 原有的爬取功能
            chat_ids = [args.chats]
            for chat_id in chat_ids:
                await scrape_messages(client, chat_id, args.start, args.end)
                
        elif args.command == 'subscribe':
            # 新的订阅功能
            await subscribe_messages(client, [args.chat])
            
        elif args.command == 'batch-subscribe':
            # 批量订阅功能
            chat_names = [name.strip() for name in args.chats.split(',')]
            await subscribe_batch_messages(client, chat_names)

if __name__ == '__main__':
    asyncio.run(main())