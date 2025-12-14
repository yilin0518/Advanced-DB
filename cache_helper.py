import redis
import json
import hashlib
import time

class CacheHelper:
    """Redis 缓存辅助类 - Cache-Aside Pattern"""
    
    def __init__(self, host='localhost', port=6379, db=0, ttl=300):
        try:
            self.client = redis.Redis(host=host, port=port, db=db, decode_responses=True)
            self.client.ping()
            self.ttl = ttl
            self.enabled = True
        except Exception as e:
            print(f"警告: Redis 连接失败 ({e})，缓存功能已禁用")
            self.enabled = False
    
    def get(self, key):
        if not self.enabled:
            return None
        try:
            data = self.client.get(key)
            return json.loads(data) if data else None
        except:
            return None
    
    def set(self, key, value, ttl=None):
        if not self.enabled:
            return False
        try:
            ttl = ttl or self.ttl
            self.client.setex(key, ttl, json.dumps(value, default=str))
            return True
        except:
            return False
    
    def cache_aside(self, key, fetch_func, ttl=None):
        """Cache-Aside 模式：先查缓存，未命中则查数据库并写回"""
        cached = self.get(key)
        if cached is not None:
            return cached
        
        data = fetch_func()
        self.set(key, data, ttl)
        return data
    
    def clear_all(self):
        if self.enabled:
            self.client.flushdb()