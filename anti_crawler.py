"""
防反爬系统模块
包含多种反反爬技术，用于避免触发网站的反爬机制
"""

import requests
import time
import random
import json
import logging
from typing import List, Dict, Optional
from dataclasses import dataclass
from datetime import datetime
import threading
import os
from fake_useragent import UserAgent


@dataclass
class ProxyInfo:
    """代理信息"""
    host: str
    port: int
    username: Optional[str] = None
    password: Optional[str] = None
    protocol: str = "http"
    is_working: bool = True
    last_used: Optional[datetime] = None
    success_count: int = 0
    fail_count: int = 0


@dataclass
class RequestStats:
    """请求统计信息"""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    captcha_encounters: int = 0
    blocked_requests: int = 0
    start_time: datetime = None
    
    def __post_init__(self):
        if self.start_time is None:
            self.start_time = datetime.now()


class UserAgentRotator:
    """User-Agent轮换器"""
    
    def __init__(self):
        self.user_agents = UserAgent(["chrome", "firefox"])
        self.current_index = 0
    
    def get_random_ua(self) -> str:
        """获取随机User-Agent"""
        return self.user_agents.random
    

class ProxyPool:
    """代理池管理器"""
    
    def __init__(self, proxy_list: List[Dict] = None):
        self.proxies: List[ProxyInfo] = []
        self.current_index = 0
        self.lock = threading.Lock()
        
        if proxy_list:
            self.load_proxies(proxy_list)
        else:
            # 默认免费代理列表（实际使用时需要替换为有效的代理）
            self.load_default_proxies()
    
    def load_proxies(self, proxy_list: List[Dict]):
        """加载代理列表"""
        for proxy_data in proxy_list:
            proxy = ProxyInfo(
                host=proxy_data.get('host'),
                port=proxy_data.get('port'),
                username=proxy_data.get('username'),
                password=proxy_data.get('password'),
                protocol=proxy_data.get('protocol', 'http')
            )
            self.proxies.append(proxy)
    
    def load_default_proxies(self):
        """加载默认代理（示例，实际使用时需要替换）"""
        # 这里可以添加一些免费代理或从代理服务商获取
        # 示例代理（需要替换为真实可用的代理）
        default_proxies = [
            # {"host": "127.0.0.1", "port": 8080, "protocol": "http"},
            # {"host": "127.0.0.1", "port": 1080, "protocol": "socks5"},
        ]
        self.load_proxies(default_proxies)
    
    def get_proxy(self) -> Optional[ProxyInfo]:
        """获取下一个可用代理"""
        with self.lock:
            if not self.proxies:
                return None
            
            # 过滤出可用的代理
            working_proxies = [p for p in self.proxies if p.is_working]
            if not working_proxies:
                # 如果没有可用代理，重置所有代理状态
                for proxy in self.proxies:
                    proxy.is_working = True
                working_proxies = self.proxies
            
            # 轮换选择代理
            proxy = working_proxies[self.current_index % len(working_proxies)]
            self.current_index += 1
            proxy.last_used = datetime.now()
            return proxy
    
    def mark_proxy_failed(self, proxy: ProxyInfo):
        """标记代理失败"""
        proxy.fail_count += 1
        if proxy.fail_count >= 3:  # 连续失败3次则标记为不可用
            proxy.is_working = False
    
    def mark_proxy_success(self, proxy: ProxyInfo):
        """标记代理成功"""
        proxy.success_count += 1
        proxy.fail_count = 0  # 重置失败计数
    
    def get_proxy_dict(self, proxy: ProxyInfo) -> Dict[str, str]:
        """将代理信息转换为requests可用的格式"""
        if proxy.username and proxy.password:
            auth = f"{proxy.username}:{proxy.password}@"
        else:
            auth = ""
        
        proxy_url = f"{proxy.protocol}://{auth}{proxy.host}:{proxy.port}"
        return {
            "http": proxy_url,
            "https": proxy_url
        }


class DelayStrategy:
    """智能延迟策略"""
    
    def __init__(self):
        self.base_delay = 2.0  # 基础延迟（秒）
        self.max_delay = 10.0  # 最大延迟（秒）
        self.delay_multiplier = 1.0  # 延迟倍数
        self.consecutive_failures = 0  # 连续失败次数
        self.last_request_time = 0
    
    def get_delay(self) -> float:
        """计算延迟时间"""
        # 基础延迟
        delay = self.base_delay * self.delay_multiplier
        
        # 根据连续失败次数增加延迟
        if self.consecutive_failures > 0:
            delay *= (1 + self.consecutive_failures * 0.5)
        
        # 添加随机性
        delay += random.uniform(0.5, 2.0)
        
        # 限制最大延迟
        delay = min(delay, self.max_delay)
        
        return delay
    
    def wait(self):
        """执行延迟等待"""
        delay = self.get_delay()
        time.sleep(delay)
        self.last_request_time = time.time()
    
    def on_success(self):
        """请求成功时的处理"""
        self.consecutive_failures = 0
        # 逐渐减少延迟倍数
        self.delay_multiplier = max(0.5, self.delay_multiplier * 0.9)
    
    def on_failure(self):
        """请求失败时的处理"""
        self.consecutive_failures += 1
        # 增加延迟倍数
        self.delay_multiplier = min(3.0, self.delay_multiplier * 1.2)


class AntiCrawlerDetector:
    """反爬检测器"""
    
    def __init__(self):
        self.captcha_keywords = [
            "验证码", "captcha", "robot", "antispider", "blocked",
            "access denied", "forbidden", "rate limit", "too many requests"
        ]
        
        self.blocked_keywords = [
            "ip blocked", "ip banned", "temporarily unavailable",
            "service unavailable", "maintenance"
        ]
    
    def detect_captcha(self, response_text: str) -> bool:
        """检测是否出现验证码"""
        text_lower = response_text.lower()
        return any(keyword in text_lower for keyword in self.captcha_keywords)
    
    def detect_blocked(self, response_text: str) -> bool:
        """检测是否被屏蔽"""
        text_lower = response_text.lower()
        return any(keyword in text_lower for keyword in self.blocked_keywords)
    
    def detect_anti_crawler(self, response_text: str) -> Dict[str, bool]:
        """综合检测反爬机制"""
        return {
            "captcha": self.detect_captcha(response_text),
            "blocked": self.detect_blocked(response_text),
            "suspicious": len(response_text) < 1000  # 响应内容过少可能被拦截
        }


class AntiCrawlerSession:
    """防反爬会话管理器"""
    
    def __init__(self, use_proxy: bool = False, max_retries: int = 3):
        self.session = requests.Session()
        self.ua_rotator = UserAgentRotator()
        self.proxy_pool = ProxyPool() if use_proxy else None
        self.delay_strategy = DelayStrategy()
        self.detector = AntiCrawlerDetector()
        self.stats = RequestStats()
        self.max_retries = max_retries
        
        # 设置默认请求头
        self.update_headers()
        
        # 设置会话配置
        self.session.timeout = 15
        self.session.max_redirects = 5
        
        # 启用重试适配器
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
    
    def update_headers(self):
        """更新请求头信息"""
        # 使用fake_useragent生成随机UA
        try:
            ua = self.user_agents.random
        except:
            # 提供备用UA作为fallback
            ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            
        # 基础请求头
        headers = {
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Cache-Control": "max-age=0",
        }
        
        # 根据User-Agent添加特定浏览器的请求头
        if "Chrome" in ua:
            headers.update({
                "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"'
            })
        elif "Firefox" in ua:
            headers.update({
                "DNT": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1"
            })
        
        self.session.headers.update(headers)
    
    def get_proxy_config(self) -> Optional[Dict[str, str]]:
        """获取代理配置"""
        if not self.proxy_pool:
            return None
        
        proxy = self.proxy_pool.get_proxy()
        if proxy:
            return self.proxy_pool.get_proxy_dict(proxy)
        return None
    
    def make_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """发送请求（带防反爬机制）"""
        for attempt in range(self.max_retries):
            try:
                # 更新请求头
                if attempt > 0:
                    self.update_headers()
                
                # 获取代理配置
                proxies = self.get_proxy_config()
                if proxies:
                    kwargs['proxies'] = proxies
                
                # 执行延迟
                if attempt > 0:
                    self.delay_strategy.wait()
                
                # 发送请求
                self.stats.total_requests += 1
                response = self.session.request(method, url, **kwargs)
                
                # 检测反爬机制
                detection = self.detector.detect_anti_crawler(response.text)
                
                if detection["captcha"]:
                    self.stats.captcha_encounters += 1
                    logging.warning(f"检测到验证码！")
                    if attempt < self.max_retries - 1:
                        self.delay_strategy.on_failure()
                        continue
                
                if detection["blocked"]:
                    self.stats.blocked_requests += 1
                    logging.warning(f"请求被屏蔽: {url}")
                    if attempt < self.max_retries - 1:
                        self.delay_strategy.on_failure()
                        continue
                
                # 请求成功
                self.stats.successful_requests += 1
                self.delay_strategy.on_success()
                
                # 标记代理成功
                if self.proxy_pool and proxies:
                    proxy = self.proxy_pool.get_proxy()
                    if proxy:
                        self.proxy_pool.mark_proxy_success(proxy)
                
                return response
                
            except requests.RequestException as e:
                self.stats.failed_requests += 1
                self.delay_strategy.on_failure()
                
                # 标记代理失败
                if self.proxy_pool and proxies:
                    proxy = self.proxy_pool.get_proxy()
                    if proxy:
                        self.proxy_pool.mark_proxy_failed(proxy)
                
                if attempt == self.max_retries - 1:
                    raise e
                
                logging.warning(f"请求失败 (尝试 {attempt + 1}/{self.max_retries}): {e}")
                continue
        
        raise requests.RequestException(f"请求失败，已重试 {self.max_retries} 次")
    
    def get(self, url: str, **kwargs) -> requests.Response:
        """GET请求"""
        return self.make_request("GET", url, **kwargs)
    
    def post(self, url: str, **kwargs) -> requests.Response:
        """POST请求"""
        return self.make_request("POST", url, **kwargs)


class AntiCrawlerManager:
    """防反爬管理器"""
    
    def __init__(self, config_file: str = "anti_crawler_config.json"):
        self.config_file = config_file
        self.config = self.load_config()
        self.sessions: List[AntiCrawlerSession] = []
        self.current_session_index = 0
        self.lock = threading.Lock()
        
        # 初始化会话池
        self.init_session_pool()
    
    def load_config(self) -> Dict:
        """加载配置文件"""
        default_config = {
            "session_pool_size": 3,
            "use_proxy": False,
            "max_retries": 3,
            "base_delay": 2.0,
            "max_delay": 10.0,
            "proxy_list": [],
            "custom_headers": {},
            "enable_ua_rotation": True,
            "enable_delay_strategy": True
        }
        
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    default_config.update(config)
            except Exception as e:
                logging.warning(f"加载配置文件失败: {e}，使用默认配置")
        
        return default_config
    
    def save_config(self):
        """保存配置文件"""
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.error(f"保存配置文件失败: {e}")
    
    def init_session_pool(self):
        """初始化会话池"""
        pool_size = self.config.get("session_pool_size", 3)
        use_proxy = self.config.get("use_proxy", False)
        max_retries = self.config.get("max_retries", 3)
        
        for i in range(pool_size):
            session = AntiCrawlerSession(use_proxy=use_proxy, max_retries=max_retries)
            self.sessions.append(session)
    
    def get_session(self) -> AntiCrawlerSession:
        """获取会话（轮换）"""
        with self.lock:
            session = self.sessions[self.current_session_index]
            self.current_session_index = (self.current_session_index + 1) % len(self.sessions)
            return session


# 全局防反爬管理器实例
anti_crawler_manager = AntiCrawlerManager()


def get_anti_crawler_session() -> AntiCrawlerSession:
    """获取防反爬会话"""
    return anti_crawler_manager.get_session()


def create_anti_crawler_session(use_proxy: bool = False, max_retries: int = 3) -> AntiCrawlerSession:
    """创建新的防反爬会话"""
    return AntiCrawlerSession(use_proxy=use_proxy, max_retries=max_retries)
