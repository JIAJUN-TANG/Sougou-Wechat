# 微信公众号数据获取端口

一个基于Python的微信公众号文章爬虫，通过搜狗微信搜索API获取微信公众号文章数据，并支持自动登录、防反爬机制和数据持久化存储。

## 功能特性

- ✅ **自动登录**：使用Playwright自动化登录，支持二维码扫描登录
- ✅ **防反爬机制**：集成防反爬系统，支持代理和请求重试
- ✅ **批量爬取**：支持同时爬取多个公众号文章
- ✅ **数据存储**：使用SQLite数据库持久化存储文章数据
- ✅ **完整内容获取**：支持获取文章正文内容
- ✅ **可配置**：通过配置文件管理要爬取的公众号列表
- ✅ **多线程处理**：使用多线程提高爬取效率
- ✅ **日志记录**：详细的日志记录，便于调试和监控

## 技术栈

- **Python 3.x**
- **Playwright**：用于自动化浏览器操作和登录
- **BeautifulSoup4**：用于HTML解析
- **Requests**：用于HTTP请求
- **SQLite3**：用于数据存储
- **concurrent.futures**：用于多线程处理

## 安装说明

### 1. 克隆仓库

```bash
git clone https://github.com/JIAJUN-TANG/Sougou-Wechat.git
cd Sougou-Wechat
```

### 2. 安装依赖

```bash
pip install -r requirements.txt
```

### 3. 安装Playwright浏览器

```bash
playwright install
```

## 配置说明

### 1. 公众号配置文件

在项目根目录下创建或编辑 `wechat_accounts.txt` 文件，每行添加一个要爬取的公众号名称或关键词，例如：

```
老年机器人
科技前沿
人工智能
```

### 2. 配置说明

- 空行和以 `#` 开头的行将被忽略
- 支持同时配置多个公众号
- 爬虫会依次爬取配置文件中的所有公众号

## 使用方法

### 1. 基本使用

直接运行主脚本即可开始爬取：

```bash
python sougou_crawl.py
```

程序会自动：
- 启动Playwright浏览器
- 显示登录二维码
- 使用微信扫描二维码登录
- 从配置文件加载公众号列表
- 开始爬取文章数据
- 将数据保存到SQLite数据库

### 2. 手动配置爬取

您可以修改 `main()` 函数来自定义爬取参数：

```python
def main():
    # 创建爬虫实例，指定搜索关键词
    crawler = WeChatCrawler(
        config_file="wechat_accounts.txt",  # 配置文件路径
        use_anti_crawler=True,  # 是否使用防反爬系统
        login_cookie_path="login_cookies.pkl"  # 登录cookie保存路径
    )
    
    # 确保登录
    if not crawler.is_logged_in:
        crawler.login()
    
    # 从配置文件加载公众号列表
    crawler.load_wechat_accounts()
    
    # 执行爬取
    crawler.crawl_all_configured_accounts(
        get_real_urls=True,  # 是否获取真实微信文章URL
        fetch_content=True,  # 是否获取文章正文内容
        page=None  # 爬取的页数，None表示爬取所有页
    )
    
    # 关闭Playwright浏览器
    crawler.close_playwright()
```

### 3. 爬取单个公众号

```python
crawler = WeChatCrawler()
crawler.login()
result = crawler.crawl_and_extract(
    query="老年机器人",  # 公众号名称或关键词
    page=1,  # 爬取的页码
    get_real_urls=True,  # 是否获取真实URL
    fetch_content=True  # 是否获取文章正文
)
```

## 项目结构

```
Sougou-Wechat/
├── sougou_crawl.py          # 主爬虫脚本
├── sqlite_storage.py        # SQLite存储模块
├── anti_crawler.py          # 防反爬系统模块
├── wechat_accounts.txt      # 公众号配置文件
├── login_cookies.pkl        # 登录cookie文件（自动生成）
├── sougou_crawl.log         # 日志文件（自动生成）
├── wechat_articles.db       # SQLite数据库文件（自动生成）
└── requirements.txt         # 依赖包列表
```

## 数据存储

爬取的数据将保存到SQLite数据库 `wechat_articles.db` 中，包含以下字段：

| 字段名 | 类型 | 描述 |
|--------|------|------|
| id | INTEGER | 自增主键 |
| title | TEXT | 文章标题 |
| summary | TEXT | 文章摘要 |
| source | TEXT | 文章来源（公众号名称） |
| publish_time | TEXT | 文章发布时间 |
| address | TEXT | 文章地址 |
| sogou_url | TEXT | 搜狗微信链接 |
| real_url | TEXT | 真实微信文章URL |
| content | TEXT | 文章正文内容 |
| crawl_time | TEXT | 爬取时间 |
| success | INTEGER | 爬取是否成功（0/1） |
| content_fetched | INTEGER | 是否获取到正文（0/1） |
| keyword | TEXT | 搜索关键词 |

## 日志记录

程序会生成详细的日志文件 `sougou_crawl.log`，包含：
- 爬取过程中的所有操作
- 错误信息和异常堆栈
- 爬取结果统计
- 防反爬系统状态

## 注意事项

1. **登录问题**：
   - 首次运行需要微信扫描二维码登录
   - 登录状态会保存到 `login_cookies.pkl` 文件中
   - 后续运行会自动加载登录状态

2. **反爬措施**：
   - 程序已集成防反爬机制，但建议不要频繁爬取
   - 爬取过程中会自动添加随机延迟
   - 若遇到验证码或封禁，请暂停一段时间后再试

3. **性能优化**：
   - 建议不要同时爬取过多公众号
   - 可根据网络情况调整线程数
   - 爬取大量数据时会占用较多内存

4. **法律合规**：
   - 本工具仅用于学习和研究目的
   - 请遵守相关网站的 robots.txt 规则
   - 不要用于商业用途或恶意爬取

## 常见问题

### Q: 无法启动浏览器或显示二维码？
A: 请确保已正确安装Playwright浏览器，执行 `playwright install` 命令。

### Q: 爬取结果为空？
A: 请检查是否已登录成功，或尝试更换关键词。

### Q: 遇到验证码？
A: 这是搜狗微信的反爬机制，请手动完成验证码后重新运行，或暂停一段时间后再试。

### Q: 数据库文件在哪里？
A: 数据库文件 `wechat_articles.db` 会在首次爬取成功后自动生成在项目根目录下。

## 开发说明

### 1. 模块化设计

项目采用模块化设计，便于扩展和维护：
- `WeChatCrawler` 类：核心爬虫逻辑
- `SQLiteArticleStorage` 类：数据存储管理
- `AntiCrawlerSession` 类：防反爬系统

### 2. 扩展建议

- 添加更多数据存储方式（MySQL、MongoDB等）
- 实现分布式爬取
- 添加Web界面管理爬取任务
- 实现数据可视化

## 更新日志

- **v1.0.0**：初始版本，支持基本的微信公众号文章爬取
- **v1.1.0**：添加防反爬机制和多线程支持
- **v1.2.0**：完善数据存储和日志记录

## 许可证

MIT License

## 贡献

欢迎提交Issue和Pull Request！

## 联系方式

如有问题或建议，请通过以下方式联系：
- GitHub Issues：https://github.com/JIAJUN-TANG/Sougou-Wechat/issues

## 免责声明

本项目仅用于学习和研究目的，请勿用于商业用途。使用本项目产生的一切后果由使用者自行承担。

---

**使用说明：**
1. 请确保您已阅读并理解本项目的注意事项和法律合规要求
2. 合理使用爬虫，避免对目标网站造成过大压力
3. 尊重知识产权，不要滥用爬取的数据
4. 定期更新代码，以适应网站结构变化

祝您使用愉快！ 🚀

