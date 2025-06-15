import asyncio
import os
import json
from datetime import datetime

from telethon import TelegramClient
from telethon.tl.types import Message
from celery.utils.log import get_task_logger
import redis

from core.celery_app import celery_app
from services.database import get_async_session, Channel, Message as MessageModel
from services.elasticsearch import get_es_client, bulk_index_messages

# 从环境变量加载配置
SESSION_NAME = "telegram_scraper_session"
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# 初始化 Redis 客户端用于进度更新
redis_client = redis.from_url(REDIS_URL, decode_responses=True)
logger = get_task_logger(__name__)

         # 定义 Celery 任务
         @ celery_app.task(bind=True)


def scrape_telegram_channel(self, channel_id: int, start_message_id: int, end_message_id: int):
    """
    Celery task to scrape messages from a Telegram channel.
    """
    task_id = self.request.id
    progress_key = f"task_progress:{task_id}"

    async def main():
        # 初始化 Telethon 客户端
        client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
        await client.start()
        logger.info(f"Task {task_id}: Telethon client started.")

        try:
            # 获取频道实体
            entity = await client.get_entity(channel_id)
            logger.info(f"Task {task_id}: Found entity '{entity.title}' for ID {channel_id}.")

            # 处理特殊的 message_id
            min_id = start_message_id if start_message_id > 0 else 0
            # 如果 end_message_id < 0，则 max_id 应该为 0，iter_messages 会抓取到最新
            max_id = end_message_id if end_message_id > 0 else 0

            # 如果是抓取到最后，先获取第一条消息的 ID 作为总数参考
            # 注意：这只是一个估算，因为消息可能被删除
            total_messages_estimate = 0
            if max_id == 0:
                try:
                    latest_msg = await client.get_messages(entity, limit=1)
                    if latest_msg:
                        total_messages_estimate = latest_msg.id - min_id
                except Exception:
                    pass  # 可能频道为空

            # 初始化进度
            progress_data = {
                "status": "STARTING",
                "current": 0,
                "total": total_messages_estimate if total_messages_estimate > 0 else "Calculating...",
                "channel_title": entity.title
            }
            redis_client.set(progress_key, json.dumps(progress_data))

            message_batch =
            count = 0
            BATCH_SIZE = 100

            # 使用 iter_messages 进行异步迭代
            async for message in client.iter_messages(
                    entity,
                    limit=None,  # 不限制总数
                    min_id=min_id,
                    max_id=max_id,
                    reverse=True  # 从旧到新遍历
            ):
                if not isinstance(message, Message) or not message.text:
                    continue

                message_batch.append(message)
                count += 1

                if len(message_batch) >= BATCH_SIZE:
                    await process_batch(message_batch, entity)
                    message_batch.clear()

                    # 更新进度
                    progress_data.update({"status": "RUNNING", "current": count})
                    redis_client.set(progress_key, json.dumps(progress_data))
                    logger.info(f"Task {task_id}: Processed batch of {BATCH_SIZE}. Total processed: {count}")

            # 处理最后一批不足 BATCH_SIZE 的消息
            if message_batch:
                await process_batch(message_batch, entity)
                logger.info(f"Task {task_id}: Processed final batch of {len(message_batch)}. Total processed: {count}")

            # 任务完成
            final_status = f"SUCCESS: Scraped a total of {count} messages from '{entity.title}'."
            progress_data.update({"status": "SUCCESS", "current": count, "total": count, "details": final_status})
            redis_client.set(progress_key, json.dumps(progress_data), ex=3600)  # 缓存1小时
            logger.info(f"Task {task_id}: {final_status}")
            return final_status

        except Exception as e:
            error_message = f"FAILURE: An error occurred: {str(e)}"
            logger.error(f"Task {task_id}: {error_message}", exc_info=True)
            progress_data = {"status": "FAILURE", "details": str(e)}
            redis_client.set(progress_key, json.dumps(progress_data), ex=3600)
            # 抛出异常，让 Celery 将任务状态标记为 FAILURE
            raise

        finally:
            if client.is_connected():
                await client.disconnect()
            logger.info(f"Task {task_id}: Telethon client disconnected.")

    # 在 Celery 任务中运行异步代码
    return asyncio.run(main())


async def process_batch(batch: list, entity):
    """
    Asynchronously process a batch of messages: save to MySQL and index in Elasticsearch.
    """
    # 1. 准备数据
    db_messages =
    es_messages =

    for msg in batch:
        msg_data = {
            "id": msg.id,
            "channel_id": entity.id,
            "message_text": msg.text,
            "sender_id": msg.sender_id,
            "reply_to_msg_id": msg.reply_to.reply_to_msg_id if msg.reply_to else None,
            "date": msg.date,
            "views": msg.views,
            "raw_message_json": msg.to_json()
        }
        db_messages.append(msg_data)

        es_doc = {
            "message_id": msg.id,
            "channel_id": entity.id,
            "message_text": msg.text,
            "sender_id": msg.sender_id,
            "reply_to_msg_id": msg.reply_to.reply_to_msg_id if msg.reply_to else None,
            "date": msg.date.isoformat(),
            "views": msg.views
        }
        es_messages.append(es_doc)

    # 2. 异步批量写入数据库和 ES
    async with get_async_session() as db_session, get_es_client() as es_client:
        # 写入 MySQL
        async with db_session.begin():
            # 先确保 channel 存在
            channel_obj = await db_session.get(Channel, entity.id)
            if not channel_obj:
                channel_obj = Channel(
                    id=entity.id,
                    title=entity.title,
                    username=getattr(entity, 'username', None),
                    description=getattr(entity, 'about', None),
                    participants_count=getattr(entity, 'participants_count', None)
                )
                db_session.add(channel_obj)

            # 批量插入消息
            await db_session.execute(
                MessageModel.__table__.insert().prefix_with("IGNORE"),
                db_messages
            )
            await db_session.commit()

        # 索引到 Elasticsearch
        await bulk_index_messages(es_client, "telegram_messages", es_messages)