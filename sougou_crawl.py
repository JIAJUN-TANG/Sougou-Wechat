import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
import json
import os
import time
import random
from typing import List, Dict, Optional
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import logging
from sqlite_storage import SQLiteArticleStorage
from anti_crawler import create_anti_crawler_session


@dataclass
class WeChatArticle:
    """微信文章数据结构"""
    title: str = ""
    summary: str = ""
    source: str = ""
    publish_time: str = ""
    address: str = ""
    sogou_url: str = ""
    real_url: str = ""
    crawl_time: str = ""
    success: bool = False
    content: str = ""  # 文章正文内容（纯文本）
    content_fetched: bool = False  # 是否成功获取内容

class WeChatCrawler:
    """微信公众号爬虫类 - 封装用于FastAPI集成"""
    
    def __init__(self, config_file: str = "wechat_accounts.txt", use_anti_crawler: bool = True):
        # 设置日志
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('sougou_crawl.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # 初始化防反爬系统
        self.use_anti_crawler = use_anti_crawler
        
        # 始终保留headers属性，用于兼容性
        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "zh-CN,zh;q=0.9,ja;q=0.8",
            "Connection": "keep-alive",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
            "sec-ch-ua": '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
        }
        
        if self.use_anti_crawler:
            self.anti_crawler_session = create_anti_crawler_session(use_proxy=False, max_retries=3)
            self.logger.info("防反爬系统初始化完成")
        else:
            self.logger.info("使用传统请求方式")
        
        # 初始化存储
        self.storage = SQLiteArticleStorage()
        self.logger.info("存储初始化完成")
        
        # 配置文件路径
        self.config_file = config_file
        
        
    def load_wechat_accounts(self) -> List[str]:
        """从配置文件加载微信公众号列表"""
        accounts = []
        try:
            if not os.path.exists(self.config_file):
                self.logger.warning(f"配置文件 {self.config_file} 不存在，使用默认配置")
                return []
            
            with open(self.config_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    # 跳过空行和注释行
                    if line and not line.startswith('#'):
                        accounts.append(line)
                        
            if accounts:
                self.logger.info(f"从配置文件加载了 {len(accounts)} 个公众号: {accounts}")
            else:
                self.logger.warning("配置文件为空，使用默认配置")
                accounts = []
                
        except Exception as e:
            self.logger.error(f"读取配置文件失败: {e}，使用默认配置")
            accounts = []
            
        return accounts
    
    def extract_real_url(self, response_text: str) -> Optional[str]:
        """从搜狗微信重定向页面的JavaScript中提取真实的微信文章URL"""
        # 使用正则表达式匹配JavaScript中的URL构建部分
        url_pattern = r"url \+= '([^']+)';"
        matches = re.findall(url_pattern, response_text)
        
        if matches:
            # 将所有匹配的部分拼接成完整URL
            real_url = ''.join(matches)
            return real_url
        
        # 备用方法：直接匹配完整的URL模式
        full_url_pattern = r'https://mp\.weixin\.qq\.com/s\?[^"\']* '
        full_match = re.search(full_url_pattern, response_text)
        if full_match:
            return full_match.group(0)
        
        return None
    
    def get_real_wechat_url(self, sogou_url: str) -> Optional[str]:
        """获取搜狗微信链接对应的真实微信文章URL"""
        try:
            if self.use_anti_crawler:
                response = self.anti_crawler_session.get(sogou_url, timeout=10)
            else:
                response = requests.get(sogou_url, headers=self.headers, timeout=10)
            
            response.raise_for_status()
            real_url = self.extract_real_url(response.text)
            if real_url:
                self.logger.info(f"成功提取真实URL: {real_url[:100]}...")
                return real_url
            else:
                self.logger.warning(f"未能提取到真实URL: {sogou_url[:100]}...")
                return None
                
        except requests.RequestException as e:
            self.logger.error(f"请求失败: {e}")
            return None
    
    def extract_article_text(self, html_content: str) -> str:
        """从HTML中提取文章正文内容"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 移除脚本和样式标签
        for script in soup(["script", "style"]):
            script.decompose()
        
        # 尝试多种选择器来定位文章正文
        content_selectors = [
            '#js_content',  # 微信文章主要内容区域
            '.rich_media_content',  # 微信文章内容
            '.article-content',
            '.content',
            'article',
            '.post-content'
        ]
        
        content_text = ""
        for selector in content_selectors:
            content_elem = soup.select_one(selector)
            if content_elem:
                # 获取文本内容并清理
                content_text = content_elem.get_text(separator='\n', strip=True)
                break
        
        # 如果没有找到特定的内容区域，使用body标签
        if not content_text:
            body = soup.find('body')
            if body:
                content_text = body.get_text(separator='\n', strip=True)
        
        # 清理文本：移除多余的空行和空格
        lines = [line.strip() for line in content_text.split('\n') if line.strip()]
        clean_content = '\n'.join(lines)
        
        address_selector = "#js_ip_wording"
        address_elem = soup.select_one(address_selector)
        if address_elem:
            address_text = address_elem.get_text(strip=True)

        return clean_content, address_text
    
    def fetch_article_content(self, real_url: str) -> Dict[str, str]:
        """获取微信文章的正文内容"""
        try:
            if self.use_anti_crawler:
                # 使用防反爬系统
                response = self.anti_crawler_session.get(real_url, timeout=15)
            else:
                # 为微信文章设置特殊的请求头
                wechat_headers = self.headers.copy()
                wechat_headers.update({
                    "Referer": "https://weixin.sogou.com/",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
                })
                response = requests.get(real_url, headers=wechat_headers, timeout=15)
            
            response.raise_for_status()
            
            # 提取正文内容
            clean_content, address_text = self.extract_article_text(response.text)
            
            return {
                "content": clean_content if clean_content else "",
                "address": address_text if address_text else "",
                "success": True
                }
            
        except requests.RequestException as e:
            self.logger.error(f"获取文章内容失败: {e}")
            return {
                "content": "",
                "address": "",
                "success": False
            }
        except Exception as e:
            self.logger.error(f"提取文章内容失败: {e}")
            return {
                "content": "",
                "address": "",
                "success": False
            }
    
    def search_articles(self, query: str, page: int = 1, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> List[WeChatArticle]:
        """搜索微信文章"""
        
        url = "https://weixin.sogou.com/weixin"
        params = {
            "query": query,
            "_sug_type_": "",
            "s_from": "input",
            "_sug_": "y",
            "type": "2",
            "page": str(page),
            "ie": "utf8"
        }
        
        # 增强请求头
        enhanced_headers = self.headers.copy()
        enhanced_headers.update({
            "Accept-Encoding": "gzip, deflate, br",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Cache-Control": "max-age=0",
            "Referer": "https://weixin.sogou.com/"
        })
        
        try:
            if self.use_anti_crawler:
                # 使用防反爬系统
                self.anti_crawler_session.get("https://weixin.sogou.com/", timeout=10)
                time.sleep(random.uniform(2, 4))  # 随机延迟
                
                response = self.anti_crawler_session.get(url, params=params, timeout=15)
            else:
                # 访问首页建立会话
                requests.get("https://weixin.sogou.com/", headers=enhanced_headers, timeout=10)
                time.sleep(random.uniform(2, 4))  # 随机延迟
                
                response = requests.get(url, headers=enhanced_headers, params=params, timeout=15)
            
            
            # 获取cookies 更新到 headers（仅在不使用防反爬系统时）
            if not self.use_anti_crawler and response.cookies:
                cookies_str = '; '.join([f"{key}={value}" for key, value in response.cookies.items()])
                self.headers['Cookie'] = cookies_str
            response.raise_for_status()
            
            articles = self._parse_search_results(response.text, query)
            
            # 如果指定了时间范围，进行过滤
            if start_time or end_time:
                articles = self._filter_articles_by_time(articles, start_time, end_time)
            
            self.logger.info(f"搜索 '{query}' 第{page}页，找到 {len(articles)} 篇文章")
            return articles
            
        except requests.RequestException as e:
            self.logger.error(f"搜索请求失败: {e}")
            return []
    
    def _parse_search_results(self, html_content: str, query: str) -> List[WeChatArticle]:
        """解析搜索结果页面"""
        soup = BeautifulSoup(html_content, 'html.parser')
        articles = []
        
        # 精准定位：搜狗微信搜索结果在 ul.news-list 下的 li 元素中
        news_items = soup.select('ul.news-list li')
        
        for item in news_items:
            article = WeChatArticle()
            article.crawl_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 1. 提取标题
            title_elem = item.select_one('h3 a')
            if title_elem:
                article.title = title_elem.get_text(strip=True)
            else:
                title_elem = item.select_one('h3')
                if title_elem:
                    article.title = title_elem.get_text(strip=True)
            
            if not article.title:
                continue
            
            # 2. 提取简要
            summary_elems = item.select('p')
            for p_elem in summary_elems:
                text = p_elem.get_text(strip=True)
                if (len(text) > 20 and 
                    not re.search(r'\d{4}-\d{1,2}-\d{1,2}', text) and
                    not re.search(r'今日|昨日|\d+小时前|\d+分钟前', text) and
                    '微信公众平台' not in text):
                    article.summary = text[:300] + '...' if len(text) > 300 else text
                    break
            
            # 3. 提取搜狗链接
            link_elem = item.select_one('h3 a')
            if link_elem:
                href = link_elem.get('href', '')
                if href:
                    if href.startswith('/'):
                        article.sogou_url = 'https://weixin.sogou.com' + href
                    else:
                        article.sogou_url = href
            
            # 4. 提取来源
            source_elem = item.select_one('div.s-p span.all-time-y2')
            if source_elem:
                source_text = source_elem.get_text(strip=True)
                if source_text and source_text != '微信公众平台':
                    article.source = source_text
            
            # 5. 提取时间
            time_script_elem = item.select_one('div.s-p span.s2 script')
            if time_script_elem:
                script_text = time_script_elem.get_text()
                timestamp_match = re.search(r'timeConvert\(\'(\d+)\'\)', script_text)
                if timestamp_match:
                    timestamp = int(timestamp_match.group(1))
                    article.publish_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        
        return articles
    
    def get_real_urls_batch(self, articles: List[WeChatArticle], max_workers: int = 3) -> List[WeChatArticle]:
        """批量获取真实URL"""
        def process_article(article: WeChatArticle) -> WeChatArticle:
            if article.sogou_url:
                real_url = self.get_real_wechat_url(article.sogou_url)
                if real_url:
                    article.real_url = real_url
                    article.success = True
                else:
                    article.success = False
                # 添加延时避免请求过快
                time.sleep(1)
            return article
        
        self.logger.info(f"开始批量获取 {len(articles)} 篇文章的真实URL...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            processed_articles = list(executor.map(process_article, articles))
        
        successful_count = sum(1 for article in processed_articles if article.success)
        self.logger.info(f"批量处理完成，成功获取 {successful_count}/{len(articles)} 个真实URL")
        
        return processed_articles
    
    def save_article_to_storage(self, article: WeChatArticle) -> bool:
        """保存文章到SQLite数据库"""
        try:
            # 转换为字典格式以兼容MinIO存储
            article_dict = {
                "title": article.title,
                "summary": article.summary,
                "source": article.source,
                "publish_time": article.publish_time,
                "address": article.address,
                "sogou_url": article.sogou_url,
                "real_url": article.real_url,
                "content": article.content,
                "crawl_time": article.crawl_time,
                "success": article.success,
                "content_fetched": article.content_fetched
            }
            
            # 使用SQLite存储保存单篇文章
            success = self.storage.save_article(article_dict)
            
            if success:
                return True
            else:
                self.logger.warning(f"文章可能重复，未保存: {article.title[:30]}...")
                return False
            
        except Exception as e:
            self.logger.error(f"保存文章失败: {e}")
            return False
    
    def fetch_contents_batch(self, articles: List[WeChatArticle], max_workers: int = 2) -> List[WeChatArticle]:
        """批量获取文章正文内容并保存到数据库"""
        def fetch_content(article: WeChatArticle) -> WeChatArticle:
            if article.real_url and article.success:
                content_result = self.fetch_article_content(article.real_url)
                article.content = content_result["content"]
                article.address = content_result["address"]
                article.content_fetched = content_result["success"]
                
                # 保存存储
                if article.content_fetched:
                    storage_success = self.save_article_to_storage(article)
                    if storage_success:
                        self.logger.info(f"文章已保存到SQLite数据库: {article.title[:30]}...")
                
                # 添加延时避免请求过快
                time.sleep(2)
            return article
        
        # 只处理成功获取真实URL的文章
        valid_articles = [article for article in articles if article.success and article.real_url]
        
        if not valid_articles:
            self.logger.warning("没有有效的文章URL可以获取内容")
            return articles
        
        self.logger.info(f"开始批量获取 {len(valid_articles)} 篇文章的正文内容...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 只处理有效文章
            processed_valid = list(executor.map(fetch_content, valid_articles))
        
        # 更新原始列表中的对应文章
        valid_dict = {id(article): article for article in processed_valid}
        for i, article in enumerate(articles):
            if id(article) in valid_dict:
                articles[i] = valid_dict[id(article)]
        
        successful_content_count = sum(1 for article in articles if article.content_fetched)
        self.logger.info(f"内容获取完成，成功获取 {successful_content_count}/{len(valid_articles)} 篇文章内容")
        
        return articles
    
    def crawl_and_extract(self, query: str, page: int = 1, get_real_urls: bool = True, fetch_content: bool = False, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> Dict:
        """完整的爬取和提取流程"""
        try:
            start_time_exec = time.time()
        
            # 1. 搜索文章
            articles = self.search_articles(query, page, start_time, end_time)
        
            if not articles:
                return {
                    "success": False,
                    "message": "未找到相关文章",
                    "data": [],
                    "stats": {"total": 0, "real_urls_extracted": 0, "content_fetched": 0, "duration": 0}
                }
        
            # 2. 获取真实URL（可选）
            if get_real_urls:
                articles = self.get_real_urls_batch(articles)
        
            # 3. 获取完整内容（可选）
            if fetch_content and get_real_urls:
                articles = self.fetch_contents_batch(articles)
        
            # 4. 统计结果
            total_articles = len(articles)
            successful_extractions = sum(1 for article in articles if article.success)
            content_fetched_count = sum(1 for article in articles if article.content_fetched)
            duration = time.time() - start_time_exec
        
            return {
            "success": True,
            "message": f"成功爬取 {total_articles} 篇文章" + (f"，获取 {content_fetched_count} 篇完整内容" if fetch_content else ""),
            "stats": {
                "total": total_articles,
                "real_urls_extracted": successful_extractions,
                "content_fetched": content_fetched_count,
                "duration": round(duration, 2),
                "query": query,
                "page": page
            }
        }

        except Exception as e:
            return {
                "success": False,
                "message": e,
                "stats": {"total": 0, "real_urls_extracted": 0, "content_fetched": 0, "duration": 0}
            }
    
    def crawl_all_configured_accounts(
        self,
        get_real_urls: bool = True,
        fetch_content: bool = False,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict:
        """
        爬取所有配置文件中的公众号
        """
        accounts = self.load_wechat_accounts()
        
        if not accounts:
            return {
                "success": False,
                "message": "没有配置任何公众号",
                "data": [],
                "stats": {"queries": 0, "total_found": 0}
            }
                
        all_articles = []
        total_articles = 0
        
        for i, account in enumerate(accounts, 1):
            self.logger.info(f"[{i}/{len(accounts)}] 正在爬取公众号：{account}")
            
            try:
                result = self.crawl_and_extract(
                    query=account,
                    page=1,
                    get_real_urls=get_real_urls,
                    fetch_content=fetch_content,
                    start_time=start_time,
                    end_time=end_time
                )
                
                if result['success']:
                    all_articles.extend(result['data'])
                    total_articles += len(result['data'])
                    self.logger.info(f"公众号 '{account}' 爬取成功：{len(result['data'])} 篇文章")
                else:
                    self.logger.error(f"公众号 '{account}' 爬取失败：{result.get('message', '未知错误')}")
                
                # 添加延迟避免请求过快
                if i < len(accounts):
                    time.sleep(2)
                    
            except Exception as e:
                self.logger.error(f"爬取公众号 '{account}' 时发生异常：{e}")
                continue
    
    def get_anti_crawler_stats(self) -> Dict:
        """获取防反爬系统统计信息"""
        if self.use_anti_crawler:
            return self.anti_crawler_session.get_stats()
        else:
            return {"message": "防反爬系统未启用"}
    
    def reset_anti_crawler_stats(self):
        """重置防反爬系统统计信息"""
        if self.use_anti_crawler:
            self.anti_crawler_session.reset_stats()
            self.logger.info("防反爬系统统计信息已重置")


def main():
    """主函数"""
     
    # 创建爬虫实例
    crawler = WeChatCrawler()
    
    # 从配置文件加载公众号列表
    crawler.load_wechat_accounts()
    
    crawler.crawl_all_configured_accounts(get_real_urls=True, fetch_content=True)


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))   
    main()