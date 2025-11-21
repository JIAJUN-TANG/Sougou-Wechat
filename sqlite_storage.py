#!/usr/bin/env python3
"""
SQLite 存储适配器
"""

import json
import logging
import sqlite3
from datetime import datetime
from typing import List, Dict
import hashlib
import os
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class SQLiteArticleStorage:
    """SQLite文章存储器"""
    
    def __init__(self, 
                 db_path: str = None, 
                 # 保持原有参数以兼容接口
                 endpoint: str = None,
                 access_key: str = None, 
                 secret_key: str = None,
                 bucket_name: str = None,
                 secure: bool = False):
        
        # 使用SQLite数据库路径
        self.db_path = db_path or os.getenv('SQLITE_DB_PATH', 'wechat_articles.db')
        
        # 初始化数据库
        self._init_database()
        logger.info(f"SQLite存储初始化成功: {self.db_path}")
    
    @contextmanager
    def get_connection(self):
        """获取数据库连接的上下文管理器"""
        conn = None
        try:
            # 创建数据库连接
            conn = sqlite3.connect(self.db_path)
            # 设置行工厂为字典模式
            conn.row_factory = sqlite3.Row
            # 设置PRAGMA
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("PRAGMA journal_mode = WAL")
            
            yield conn
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"数据库操作错误: {str(e)}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
    
    def _init_database(self):
        """初始化数据库，创建表和索引"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # 创建文章表
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                object_key TEXT UNIQUE NOT NULL,
                title TEXT NOT NULL,
                summary TEXT,
                source TEXT,
                publish_time TEXT,
                address TEXT,
                real_url TEXT,
                content TEXT,
                saved_at TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            try:
                cursor.execute(create_table_sql)
                logger.debug("文章表创建成功或已存在")
            except sqlite3.Error as e:
                logger.error(f"创建文章表失败: {str(e)}")
                raise
            
            # 创建索引以提高查询性能
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_articles_object_key ON articles(object_key)",
                "CREATE INDEX IF NOT EXISTS idx_articles_publish_time ON articles(publish_time)",
                "CREATE INDEX IF NOT EXISTS idx_articles_source ON articles(source)",
                "CREATE INDEX IF NOT EXISTS idx_articles_title ON articles(title)"
            ]
            
            for index_sql in indexes:
                try:
                    cursor.execute(index_sql)
                    logger.debug("索引创建成功或已存在")
                except sqlite3.Error as e:
                    logger.error(f"创建索引失败: {str(e)}")
                    raise
    
    def _generate_object_key(self, article_data: Dict) -> str:
        """生成对象键名（保持兼容）"""
        # 使用标题和时间生成唯一键
        title = article_data.get('title', 'unknown')
        publish_time = article_data.get('publish_time', datetime.now().isoformat())
        
        # 生成hash确保唯一性
        content_hash = hashlib.md5(f"{title}{publish_time}".encode('utf-8')).hexdigest()[:8]
        
        # 按日期分组存储
        date_str = publish_time[:10] if len(publish_time) >= 10 else datetime.now().strftime('%Y-%m-%d')
        
        return f"{date_str}/{content_hash}"
    
    def save_article(self, article_data: Dict) -> bool:
        """保存文章到SQLite数据库"""
        try:
            # 生成对象键（用于唯一性检查）
            object_key = self._generate_object_key(article_data)
            
            # 添加保存时间戳
            article_data['saved_at'] = datetime.now().isoformat()
            
            # 构建插入SQL
            insert_sql = """
            INSERT OR IGNORE INTO articles 
            (object_key, title, summary, source, publish_time, address, real_url, content, saved_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(insert_sql, (
                    object_key,
                    article_data.get('title', ''),
                    article_data.get('summary', ''),
                    article_data.get('source', ''),
                    article_data.get('publish_time', ''),
                    article_data.get('address', ''),
                    article_data.get('real_url', ''),
                    article_data.get('content', ''),
                    article_data['saved_at']
                ))
                
                # 检查是否插入成功（0表示已存在）
                if cursor.rowcount == 0:
                    logger.info(f"文章已存在，跳过保存: {article_data.get('title', '')[:30]}...")
                    return False
                
                return True
                
        except Exception as e:
            logger.error(f"保存文章失败: {e}")
            return False
    
    def _article_exists(self, object_key: str) -> bool:
        """检查文章是否已存在"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(1) FROM articles WHERE object_key = ?", (object_key,))
                result = cursor.fetchone()
                return result[0] > 0
        except Exception as e:
            logger.error(f"检查文章是否存在失败: {e}")
            return False
        

# 兼容性适配器
class SQLiteArticleStorageAdapter(SQLiteArticleStorage):
    """SQLite存储适配器"""
    
    def save_articles(self, articles: List[Dict]) -> int:
        """批量保存文章"""
        saved_count = 0
        for article in articles:
            if self.save_article(article):
                saved_count += 1
        return saved_count
