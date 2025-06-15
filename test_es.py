from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import os

ES_HOST = os.getenv('ES_HOST')
load_dotenv()  # 加载.env文件中的环境变量

es = Elasticsearch("http://localhost:9200",
                   headers={"Accept": "application/vnd.elasticsearch+json; compatible-with=8"})


# 创建Elasticsearch索引（带中文分词）
def create_es_index():
    index_name = "telegram_messages"
    if not es.indices.exists(index=index_name):
        es.indices.create(
            index=index_name,
            body={
                "settings": {
                    "analysis": {
                        "analyzer": {
                            "ik_pinyin_analyzer": {
                                "type": "custom",
                                "tokenizer": "ik_max_word",
                                "filter": ["pinyin_filter"]
                            }
                        },
                        "filter": {
                            "pinyin_filter": {
                                "type": "pinyin",
                                "keep_original": True,
                                "keep_first_letter": True,
                                "keep_separate_first_letter": True
                            }
                        }
                    }
                },
                "mappings": {
                    "properties": {
                        "id": {"type": "long"},
                        "chat_id": {"type": "long"},
                        "message": {
                            "type": "text",
                            "analyzer": "ik_pinyin_analyzer",
                            "fields": {
                                "keyword": {"type": "keyword"}
                            }
                        },
                        "date": {"type": "date"},
                        "sender_id": {"type": "long"},
                        "views": {"type": "integer"},
                        "forwards": {"type": "integer"},
                        "media_type": {"type": "keyword"}
                    }
                }
            }
        )
        print(f"Created Elasticsearch index: {index_name}")

if __name__ == "__main__":
    create_es_index()