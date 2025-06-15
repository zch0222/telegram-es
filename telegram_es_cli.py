import os
import re
import argparse
import asyncio
import pymysql
from telethon import TelegramClient
from telethon.tl.types import Message
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

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
es = Elasticsearch(ES_HOST)
mysql_conn = pymysql.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASS,
    database=MYSQL_DB,
    charset='utf8mb4'
)


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
                           50
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


# 消息处理函数
def process_message(msg: Message, chat_id: str):
    # 提取媒体类型
    media_type = None
    if msg.media:
        media_type = str(msg.media).split('.')[-1].split("'")[0].lower()

    print(msg.from_id)
    print(f"message_id:{int(msg.id)}\n"
          f"chat_id:{chat_id}\n"
          f"date:{msg.date}\n"
          f"sender_id:{msg.sender_id if hasattr(msg, 'sender_id') else None}\n"
          f"views:{msg.views}\n"
          f"forwards:{msg.forwards}\n")

    # 构建消息文档
    doc = {
        "message_id": msg.id,
        "chat_id": chat_id,
        "message": msg.message or "",
        "date": msg.date,
        "sender_id": msg.sender_id if hasattr(msg, 'sender_id') else None,
        "views": msg.views if hasattr(msg, 'views') else 0,
        "forwards": msg.forwards if hasattr(msg, 'forwards') else 0,
        "media_type": media_type,
        "message_link": f"https://t.me/{chat_id[1:]}/{msg.id}"
    }

    # 处理HTML特殊字符
    if doc['message']:
        doc['message'] = re.sub(r'<[^>]+>', '', doc['message'])

    return doc


# 存储到Elasticsearch
def save_to_es(docs):
    actions = [
        {
            "_index": "telegram_messages",
            "_id": f"{doc['chat_id']}_{doc['message_id']}",
            "_source": doc
        }
        for doc in docs
    ]
    bulk(es, actions)


# 存储到MySQL
def save_to_mysql(docs):
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


# 主爬取函数
async def scrape_messages(client, channel_name, min_id, max_id):
    print(f"Starting scrape for chat {channel_name}. Range: {min_id} to {max_id}")

    # 获取最小/最大ID的实际值
    real_min_id = 0 if min_id < 0 else min_id
    real_max_id = None if max_id < 0 else max_id

    total_count = 0
    batch = []
    entity = await client.get_entity(channel_name)
    async for message in client.iter_messages(
            entity=entity,
            min_id=real_min_id,
            max_id=real_max_id,
            reverse=True  # 从旧到新获取
    ):
        if not isinstance(message, Message):
            continue

        doc = process_message(message, channel_name)
        batch.append(doc)

        # 每100条批量保存一次
        if len(batch) >= 100:
            save_to_es(batch)
            save_to_mysql(batch)
            total_count += len(batch)
            print(f"Processed {total_count} messages in chat {channel_name}")
            batch = []

    # 处理剩余消息
    if batch:
        save_to_es(batch)
        save_to_mysql(batch)
        total_count += len(batch)

    print(f"Completed! Total {total_count} messages processed in chat {channel_name}")


# 主函数
async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--chats', required=True, help='逗号分隔的频道/群组ID')
    parser.add_argument('--start', type=int, required=True, help='起始消息ID')
    parser.add_argument('--end', type=int, required=True, help='结束消息ID')
    args = parser.parse_args()

    # 创建数据库结构
    create_es_index()
    create_mysql_table()

    # 处理多个聊天
    chat_ids = [str(x.strip()) for x in args.chats.split(',')]

    async with TelegramClient('session_name', API_ID, API_HASH) as client:
        for chat_id in chat_ids:
            await scrape_messages(client, chat_id, args.start, args.end)


if __name__ == '__main__':
    asyncio.run(main())