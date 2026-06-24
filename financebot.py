import os
import time
import pytz
import requests
import feedparser
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from zhipuai import ZhipuAI
from dotenv import load_dotenv
from newspaper import Article

# 加载环境变量
load_dotenv()

# =================配置区域=================
# 使用智谱AI API Key (环境变量名可自定义，此处沿用 ZhipuAI_API_KEY 以便兼容)
zhipu_api_key = os.getenv("ZhipuAI_API_KEY")
if not zhipu_api_key:
    raise ValueError("环境变量 ZhipuAI_API_KEY 未设置，请在 .env 文件中配置智谱AI的API Key")

# Server酱 SendKeys
server_chan_keys_env = os.getenv("SERVER_CHAN_KEYS")
if not server_chan_keys_env:
    raise ValueError("环境变量 SERVER_CHAN_KEYS 未设置")
server_chan_keys = server_chan_keys_env.split(",")

# 初始化客户端 (指向智谱AI兼容接口)
client = ZhipuAI(api_key=os.getenv("ZHIPU_API_KEY"))

# RSS源地址列表
rss_feeds = {
    "💲 华尔街见闻": {
        "华尔街见闻": "https://dedicated.wallstreetcn.com/rss.xml",
    },
    "💻 36氪": {
        "36氪": "https://36kr.com/feed",
    },
    "🇨🇳 中国经济": {
        "东方财富": "http://rss.eastmoney.com/rss_partener.xml",
        "中新网": "https://www.chinanews.com.cn/rss/finance.xml",
    },
    "🇺🇸 美国经济": {
        "华尔街日报 - 经济": "https://feeds.content.dowjones.io/public/rss/WSJcomUSBusiness",
        "华尔街日报 - 市场": "https://feeds.content.dowjones.io/public/rss/RSSMarketsMain",
        "MarketWatch美股": "https://www.marketwatch.com/rss/topstories",
        "ETF Trends": "https://www.etftrends.com/feed/",
    },
    "🌍 世界经济": {
        "华尔街日报 - 全球": "https://feeds.content.dowjones.io/public/rss/socialeconomyfeed",
    },
}


# =================工具函数=================

def get_beijing_time_str():
    """获取格式化的北京时间字符串"""
    now = datetime.now(pytz.timezone("Asia/Shanghai"))
    return now.strftime("%Y-%m-%d %H:%M:%S")


def fetch_article_text(url):
    try:
        # 直接使用默认配置初始化
        article = Article(url)
        article.download()
        article.parse()

        if not article.text or len(article.text.strip()) < 50:
            return ""
        return article.text[:1500]
    except Exception as e:
        return ""


def fetch_feed_with_headers(url):
    """带 Header 的 RSS 解析"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/xml, text/xml, */*; q=0.01'
    }
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status() # 如果状态码不是 200，直接抛出异常
        return feedparser.parse(resp.content)
    except Exception as e:
        print(f"⚠️ RSS 请求失败 [{url}]: {e}")
        return None # 明确返回 None


def process_single_source(source_name, url):
    """处理单个 RSS 源"""
    titles_list = []
    analysis_texts = []

    # 1. 获取 feed
    feed = fetch_feed_with_headers(url)

    # 2. 【关键修复】立即检查 feed 是否有效
    if feed is None:
        print(f"❌ [{source_name}] 跳过：无法获取 RSS 数据")
        return source_name, [], ""

    # 3. 检查是否有 entries
    if not hasattr(feed, 'entries') or len(feed.entries) == 0:
        print(f"⚠️ [{source_name}] 跳过：RSS 中无新闻条目")
        return source_name, [], ""

    print(f"✅ [{source_name}] 解析到 {len(feed.entries)} 条新闻，开始抓取正文...")

    # 4. 遍历条目
    for entry in feed.entries[:3]:  # 只取前3条
        title = entry.get('title', '无标题')
        link = entry.get('link', '') or entry.get('guid', '')

        if not link:
            continue

        # 爬取正文
        content = fetch_article_text(link)
        if content:
            analysis_texts.append(f"【{title}】\n{content}\n")
            titles_list.append(f"- [{title}]({link})")
        else:
            # 可选：打印哪些链接没抓到正文，方便调试
            # print(f"   - 正文为空: {title[:30]}...")
            pass

    return source_name, titles_list, "\n".join(analysis_texts)


def fetch_rss_articles_parallel(rss_feeds_dict):
    """并行获取 RSS 内容"""
    all_news_data = {}
    all_analysis_text = ""

    # 构建任务列表
    tasks = []
    for category, sources in rss_feeds_dict.items():
        for source_name, url in sources.items():
            tasks.append((category, source_name, url))

    print(f"🚀 开始并行爬取 {len(tasks)} 个 RSS 源...")

    # 使用线程池并行爬取，最大_workers 设为 5 避免被封 IP
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_info = {}
        for category, source_name, url in tasks:
            future = executor.submit(process_single_source, source_name, url)
            future_to_info[future] = (category, source_name)

        for future in as_completed(future_to_info):
            category, source_name = future_to_info[future]
            try:
                src_name, titles, analysis_txt = future.result()

                # 组装展示用的 Markdown
                if titles:
                    if category not in all_news_data:
                        all_news_data[category] = ""
                    all_news_data[category] += f"### {src_name}\n" + "\n".join(titles) + "\n\n"

                # 组装分析用的文本
                if analysis_txt:
                    all_analysis_text += analysis_txt + "\n---\n"

            except Exception as e:
                print(f"❌ 处理 {source_name} 时发生错误: {e}")

    return all_news_data, all_analysis_text


def summarize(text):
    """调用智谱 GLM-4 API 生成摘要"""
    if not text.strip():
        return "⚠️ 未能获取足够的新闻内容进行分析。"

    print("🤖 正在调用 AI 进行深度分析...")
    try:
        completion = client.chat.completions.create(
            model="glm-4-flash",  # 使用免费且快速的模型
            messages=[
                {"role": "system", "content": """
             你是一名专业的财经新闻分析师，请根据以下新闻内容，按照以下步骤完成任务：
             1. 提取新闻中涉及的主要行业和主题，找出近1天涨幅最高的3个行业或主题，以及近3天涨幅较高且此前2周表现平淡的3个行业/主题。（如新闻未提供具体涨幅，请结合描述和市场情绪推测热点）
             2. 针对每个热点，输出：
                - 催化剂：分析近期上涨的可能原因（政策、数据、事件、情绪等）。
                - 复盘：梳理过去3个月该行业/主题的核心逻辑、关键动态与阶段性走势。
                - 展望：判断该热点是短期炒作还是有持续行情潜力。
             3. 将以上分析整合为一篇1500字以内的财经热点摘要，逻辑清晰、重点突出，适合专业投资者阅读。
                 """},
                {"role": "user", "content": text}
            ],
            temperature=0.3,  # 降低随机性，使分析更稳定
            max_tokens=1500
        )
        return completion.choices[0].message.content.strip()
    except Exception as e:
        return f"❌ AI 分析失败: {str(e)}"


def send_to_server_chan(title, content):
    """发送消息到 Server 酱"""
    base_url = "https://sctapi.ftqq.com/{}.send"
    for key in server_chan_keys:
        url = base_url.format(key)
        payload = {
            "title": title,
            "desp": content
        }
        try:
            resp = requests.post(url, data=payload, timeout=10)
            resp_json = resp.json()
            if resp_json.get('code') == 0:
                print(f"✅ Server酱推送成功 (Key: {key[:4]}...)")
            else:
                print(f"⚠️ Server酱推送失败: {resp_json.get('message')}")
        except Exception as e:
            print(f"❌ 发送请求错误: {e}")


# =================主执行流程=================
def main():
    print(f"⏰ 任务启动时间: {get_beijing_time_str()}")

    # 1. 获取新闻数据
    news_markdown, analysis_raw_text = fetch_rss_articles_parallel(rss_feeds)

    if not analysis_raw_text:
        print("⚠️ 未获取到任何有效文章内容，退出。")
        return

    # 2. AI 分析
    ai_summary = summarize(analysis_raw_text)

    # 3. 组装最终推送内容
    # 为了美观，我们将 AI 摘要放在最前面，原始新闻链接放在后面作为附录
    final_content = f"""
{ai_summary}

---
### 📰 原始新闻来源参考
"""
    for category, md_content in news_markdown.items():
        final_content += f"\n**{category}**\n{md_content}"

    # 截断过长的内容，Server酱对 desp 字段有长度限制（通常 32KB，但微信展示有限）
    if len(final_content) > 50000:
        final_content = final_content[:50000] + "\n\n...(内容过长，已截断)"

    # 4. 推送
    title = f"📅 财经内参 | {datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%m-%d')}"
    send_to_server_chan(title, final_content)

    print("🏁 任务结束。")


if __name__ == "__main__":
    main()
