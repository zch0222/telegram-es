# db_utils.py
import pymysql
from elasticsearch import Elasticsearch, NotFoundError
from config import config


def create_mysql_table():
    """创建MySQL表（如果不存在）"""
    try:
        connection = pymysql.connect(
            host=config.MYSQL_HOST,
            port=config.MYSQL_PORT,
            user=config.MYSQL_USER,
            password=config.MYSQL_PASSWORD,
            database=config.MYSQL_DB,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        with connection.cursor() as cursor:
            # SQL语句来自第二部分
            create_table_query = """
                                 CREATE TABLE IF NOT EXISTS `telegram_messages` \
                                 ( \
                                     `id` \
                                     BIGINT \
                                     UNSIGNED \
                                     NOT \
                                     NULL \
                                     AUTO_INCREMENT \
                                     COMMENT \
                                     '自增主键ID', \
                                     `channel_id` \
                                     BIGINT \
                                     NOT \
                                     NULL \
                                     COMMENT \
                                     '来源Telegram频道/群组的ID', \
                                     `message_id` \
                                     INT \
                                     UNSIGNED \
                                     NOT \
                                     NULL \
                                     COMMENT \
                                     '消息在频道/群组内的唯一ID', \
                                     `message_text` \
                                     LONGTEXT \
                                     CHARACTER \
                                     SET \
                                     utf8mb4 \
                                     COLLATE \
                                     utf8mb4_unicode_ci \
                                     NULL \
                                     COMMENT \
                                     '消息的文本内容', \
                                     `sender_id` \
                                     BIGINT \
                                     NULL \
                                     COMMENT \
                                     '发送者的用户ID', \
                                     `reply_to_message_id` \
                                     INT \
                                     UNSIGNED \
                                     NULL \
                                     COMMENT \
                                     '此消息回复的另一条消息的ID', \
                                     `sent_at` \
                                     DATETIME \
                                     NOT \
                                     NULL \
                                     COMMENT \
                                     '消息发送时间', \
                                     `edited_at` \
                                     DATETIME \
                                     NULL \
                                     COMMENT \
                                     '消息最后编辑时间', \
                                     `has_media` \
                                     BOOLEAN \
                                     NOT \
                                     NULL \
                                     DEFAULT \
                                     FALSE \
                                     COMMENT \
                                     '消息是否包含媒体文件', \
                                     `media_info` \
                                     JSON \
                                     NULL \
                                     COMMENT \
                                     '媒体文件的元数据（如文件名、大小、S3路径等）', \
                                     `raw_message` \
                                     JSON \
                                     NOT \
                                     NULL \
                                     COMMENT \
                                     '从Telethon获取的原始消息对象的JSON序列化', \
                                     `created_at` \
                                     TIMESTAMP \
                                     NOT \
                                     NULL \
                                     DEFAULT \
                                     CURRENT_TIMESTAMP \
                                     COMMENT \
                                     '记录创建时间', \
                                     `updated_at` \
                                     TIMESTAMP \
                                     NOT \
                                     NULL \
                                     DEFAULT \
                                     CURRENT_TIMESTAMP \
                                     ON \
                                     UPDATE \
                                     CURRENT_TIMESTAMP \
                                     COMMENT \
                                     '记录最后更新时间', \
                                     PRIMARY \
                                     KEY \
                                 ( \
                                     `id` \
                                 ),
                                     UNIQUE KEY `uk_channel_message` \
                                 ( \
                                     `channel_id`, \
                                     `message_id` \
                                 ) COMMENT '确保同一频道内的消息唯一，防止重复插入'
                                     ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE =utf8mb4_unicode_ci COMMENT='存储从Telegram抓取的消息'; \
                                 """
            cursor.execute(create_table_query)
        connection.commit()
        print("MySQL table 'telegram_messages' is ready.")
    finally:
        if 'connection' in locals() and connection.open:
            connection.close()


def create_es_index():
    """创建Elasticsearch索引和映射（如果不存在）"""
    es_client = Elasticsearch(hosts=config.ES_HOSTS)

    # 索引映射定义来自第二部分
    index_body = {
        "settings": {
            "analysis": {
                "analyzer": {
                    "pinyin_analyzer": {"tokenizer": "standard", "filter": ["my_pinyin_filter"]},
                    "ik_analyzer": {"tokenizer": "ik_smart"}
                },
                "filter": {
                    "my_pinyin_filter": {
                        "type": "pinyin", "keep_full_pinyin": True, "keep_joined_full_pinyin": False,
                        "keep_original": True, "limit_first_letter_length": 16, "lowercase": True,
                        "remove_duplicated_term": True
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "channel_id": {"type": "long"},
                "message_id": {"type": "integer"},
                "message_text": {
                    "type": "text", "analyzer": "ik_analyzer",
                    "fields": {
                        "pinyin": {"type": "text", "analyzer": "pinyin_analyzer"},
                        "keyword": {"type": "keyword", "ignore_above": 256}
                    }
                },
                "sender_id": {"type": "long"},
                "reply_to_message_id": {"type": "integer"},
                "sent_at": {"type": "date"},
                "edited_at": {"type": "date"},
                "has_media": {"type": "boolean"}
            }
        }
    }

    try:
        # 检查索引是否存在
        if not es_client.indices.exists(index=config.ES_INDEX_NAME):
            # [64, 65]
            es_client.indices.create(index=config.ES_INDEX_NAME, body=index_body)
            print(f"Elasticsearch index '{config.ES_INDEX_NAME}' created with custom mapping.")
        else:
            print(f"Elasticsearch index '{config.ES_INDEX_NAME}' already exists.")
    except Exception as e:
        print(f"Error creating Elasticsearch index: {e}")
    finally:
        es_client.close()