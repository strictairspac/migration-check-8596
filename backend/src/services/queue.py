import logging
import redis
from src.config.settings import REDIS_URL,QUEUE_KEY
# Redis 只承担“通知”角色；消息丢失/重复/乱序都由数据库扫描与行锁兜底
queue=redis.Redis.from_url(REDIS_URL)
def publish(run_id):
 """尽力通知；失败不影响受理，扫描会在恢复后补发。"""
 try:
  queue.rpush(QUEUE_KEY,int(run_id))
  return True
 except redis.RedisError:
  logging.getLogger('src.services.queue').warning('队列通知失败，受理已持久化，等待扫描补发: run=%s',run_id)
  return False
