import os
from celery import Celery

# 从环境变量中获取 Redis 的连接 URL，如果未设置则使用默认值
# 这种方式便于在不同环境（开发、生产）中使用不同的配置
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# 创建 Celery 应用实例
# 第一个参数是当前模块的名称，这对于自动生成任务名称很重要
# broker 参数指定了消息代理的 URL
# backend 参数指定了结果后端的 URL
celery_app = Celery(
    "telegram_scraper",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=["app.tasks.telegram_scraper"]  # 自动发现任务的模块列表
)

# 可选的 Celery 配置
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Asia/Shanghai",
    enable_utc=True,
    # # 任务执行超时时间
    # task_time_limit=3600, # 1 hour
    # # 任务软超时时间
    # task_soft_time_limit=3500,
)

if __name__ == "__main__":
    celery_app.start()