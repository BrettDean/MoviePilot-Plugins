from app.api.endpoints.mediaserver import not_exists
from app.api.endpoints.subscribe import subscribe_mediaid, update_subscribe_status
from app.chain.media import MediaChain
from app.chain.subscribe import SubscribeChain
from app.chain.tmdb import TmdbChain
from app.chain.transfer import TransferChain
from app.core.config import settings
from app.core.context import MediaInfo as core_MediaInfo
from app.core.event import eventmanager
from app.db import ScopedSession
from app.db.models.subscribe import Subscribe
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.context import MediaInfo
from app.schemas.types import EventType
from app.schemas.types import MediaType as schemas_MediaType
from app.utils.timer import TimerUtils
from apscheduler.schedulers.background import BackgroundScheduler
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from db.subscribe_oper import SubscribeOper
from pathlib import Path
from playwright.sync_api import sync_playwright
from typing import List, Tuple, Dict, Any, Optional
import pytz
import random
import re
import threading
import time
import traceback
import pymysql

lock = threading.Lock()


class mysqlSubscribe(_PluginBase):
    # 插件名称
    plugin_name = "mysqlSubscribe"
    # 插件描述
    plugin_desc = "通过mysql获取电视剧或电影并订阅"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/BrettDean/MoviePilot-Plugins/main/icons/mysqlsubscribe.png"
    # 插件版本
    plugin_version = "1.0.0"
    # 插件作者
    plugin_author = "Dean"
    # 作者主页
    author_url = "https://github.com/BrettDean/MoviePilot-Plugins"
    # 插件配置项ID前缀
    plugin_config_prefix = "mysqlSubscribe_"
    # 加载顺序
    plugin_order = 4
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _scheduler = None
    transferchian = None
    tmdbchain = None
    subchain = None
    _enabled = False
    _onlyonce = False
    # 存储源目录与目的目录关系
    _dirconf: Dict[str, Optional[Path]] = {}
    # 存储源目录转移方式
    _transferconf: Dict[str, Optional[str]] = {}
    _overwrite_mode: Dict[str, Optional[str]] = {}
    # 每次获取的电视剧数量
    _tv_limit: int = 100
    # 退出事件
    _event = threading.Event()

    def init_plugin(self, config: dict = None):
        self.transferchian = TransferChain()
        self.tmdbchain = TmdbChain()
        self.mediacahin = MediaChain()
        self.subchain = SubscribeChain()
        self.subscribeoper = SubscribeOper()
        # 清空配置
        self._dirconf = {}
        self._transferconf = {}
        self._overwrite_mode = {}

        # 读取配置
        if config:
            self._enabled = config.get("enabled")
            self._onlyonce = config.get("onlyonce")
            self._tv_limit = config.get("tv_limit", 100)

        # 停止现有任务
        self.stop_service()

        if self._enabled or self._onlyonce:
            # 定时服务管理器
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)

            if self._onlyonce:
                # 定时服务
                self._scheduler = BackgroundScheduler(timezone=settings.TZ)
                logger.info("立即运行一次")
                # 运行一次定时服务
                self._scheduler.add_job(
                    func=self.main,
                    trigger="date",
                    run_date=datetime.now(tz=pytz.timezone(settings.TZ))
                    + timedelta(seconds=3),
                    name="mysqlSubscribe运行一次",
                )

                # 关闭一次性开关
                self._onlyonce = False
                # 保存配置
                self.__update_config()

            # 启动服务
            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

    def __update_config(self):
        """
        更新配置
        """
        self.update_config(
            {
                "enabled": self._enabled,
                "onlyonce": self._onlyonce,
                "tv_limit": self._tv_limit,
            }
        )

    def random_sleep(self):
        # 生成 1 到 3 分钟之间的随机数，单位为秒
        sleep_time = random.uniform(60, 180)
        logger.info(f"随机等待 {sleep_time} 秒后继续执行")

        time.sleep(sleep_time)

    # 通用的滚动函数
    def scroll_down(self, page, selector: str):
        """
        通用的滚动函数
        :param page: playwright page对象
        :param selector: 要滚动到的元素的CSS选择器
        """
        for _ in range(1):
            page.evaluate(
                f"""
                const items = document.querySelectorAll('{selector}');
                if (items.length > 0) {{
                    items[items.length - 1].scrollIntoView({{ behavior: 'smooth' }});
                }}
                """
            )
            logger.info(f"滚动前随机等待6-40秒")
            time.sleep(random.randint(6, 40))

    def get_qq_tv_list(self) -> List:
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)

                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                    locale="zh-CN",
                    timezone_id="Asia/Shanghai",
                )

                page = context.new_page()

                page.evaluate(
                    """
                    () => {
                        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3]});
                        Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'zh-CN']});
                        delete window.chrome;
                        delete navigator.__proto__.webdriver;
                    }
                """
                )

                page.set_viewport_size(
                    {
                        "width": 1400,
                        "height": 900,
                        "device_scale_factor": 1,
                        "is_mobile": False,
                    }
                )
                page.set_extra_http_headers({"Accept-Language": "zh-CN,zh;q=0.9"})

                url = "https://v.qq.com/channel/tv/list?filter_params=sort%3D75&page_id=channel_list_second_page&channel_id=100113"

                # 打开页面
                for retry in range(5):
                    try:
                        logger.info(f"尝试第 {retry + 1} 次加载页面：{url}")
                        page.goto(url, timeout=30000, wait_until="domcontentloaded")
                        break
                    except Exception as e:
                        logger.info(
                            f"第 {retry + 1} 次尝试加载页面失败: {str(e)}，等待2秒后重试"
                        )
                        if retry == 4:  # 最后一次尝试也失败
                            logger.info("页面加载失败次数达到上限，返回空结果")
                            browser.close()
                            return []
                        time.sleep(2)

                # 等待页面加载完成
                page.wait_for_selector('div[dt-eid="choose_item"]')
                page.locator(
                    "div.filter span.filter__text", has_text="最新上架"
                ).click()  # 点击最新上架按钮 # 法1:CSS定位

                # page.locator(
                #     '//div[contains(@class, "filter")]//span[contains(text(), "最新上架")]'
                # ).click()  # 点击最新上架按钮 # 法2:XPath定位

                # 等待内容更新
                time.sleep(2)

                tv_set = set()  # 使用集合存储不重复的电视剧信息
                retry_count = 0
                max_retries = 5

                def process_items():
                    content = page.content()
                    soup = BeautifulSoup(content, "html.parser")
                    tv_items = soup.find_all("div", class_="item-info")

                    for item in tv_items:
                        try:
                            title_elem = item.find("span", class_="item-title")
                            title = (
                                title_elem.get("title") if title_elem else "未知标题"
                            )

                            poster_view = item.find_previous(
                                "div", class_="poster-view"
                            )
                            if poster_view:
                                update_status = poster_view.find(
                                    "span",
                                    class_="absolute fourth-label__text",
                                    string=lambda x: "更新" in str(x) or "全" in str(x),
                                )
                                update_text = (
                                    update_status.text
                                    if update_status
                                    else "暂无更新信息"
                                )
                                page.wait_for_selector(
                                    "div.poster-view__layer span.fourth-label__text",
                                    timeout=10000,
                                )
                                year = (
                                    page.locator(
                                        "div.poster-view__layer span.fourth-label__text"
                                    )
                                    .nth(0)
                                    .text_content()
                                )
                            else:
                                update_text = "暂无更新信息"

                            # 将信息转换为元组并添加到集合中
                            tv_info = (title, update_text, year)
                            tv_set.add(tv_info)
                        except Exception as item_error:
                            logger.info(f"处理条目时出错: {str(item_error)}")
                            continue

                    return len(tv_items)

                while len(tv_set) < self._tv_limit:
                    logger.info(f"累计抓取到 {len(tv_set)} 条数据")
                    current_count = process_items()

                    if len(tv_set) >= self._tv_limit:
                        break

                    self.scroll_down(page, "div.grid__item")

                    new_count = process_items()
                    if new_count == current_count:
                        retry_count += 1
                        if retry_count >= max_retries:
                            logger.info(
                                f"没有更多内容加载，当前获取到 {len(tv_set)} 个不重复条目"
                            )
                            break
                    else:
                        retry_count = 0
                logger.info(f"不重复条目最终为: {len(tv_set)}")

                # 转换集合为列表并打印结果
                tv_list = [
                    {"title": title, "status": status, "year": year}
                    for title, status, year in list(tv_set)[: self._tv_limit]
                ]
                for idx, tv in enumerate(tv_list, start=1):
                    logger.info(
                        f"腾讯视频({idx}/{len(tv_list)}): 剧名: {tv['title']}, 更新状态: {tv['status']}, 年份: {tv['year']}"
                    )

                browser.close()
                return tv_list

        except Exception as e:
            logger.debug(f"发生错误: {str(e)}， traceback: {traceback.format_exc()}")
            logger.info("腾讯的就是经常会出错")
            return []

    def get_youku_tv_list(self):
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)

                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                    locale="zh-CN",
                    timezone_id="Asia/Shanghai",
                )

                page = context.new_page()

                page.evaluate(
                    """
                    () => {
                        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3]});
                        Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'zh-CN']});
                        delete window.chrome;
                        delete navigator.__proto__.webdriver;
                    }
                """
                )

                page.set_viewport_size(
                    {
                        "width": 1400,
                        "height": 900,
                        "device_scale_factor": 1,
                        "is_mobile": False,
                    }
                )
                page.set_extra_http_headers({"Accept-Language": "zh-CN,zh;q=0.9"})

                url = "https://www.youku.com/channel/webtv/list?filter=type_电视剧_sort_1&spm=a2hja.14919748_WEBTV_JINGXUAN.drawer3.d_sort_2"
                # 打开页面
                for retry in range(5):
                    try:
                        logger.info(f"尝试第 {retry + 1} 次加载页面：{url}")
                        page.goto(url, timeout=30000, wait_until="domcontentloaded")
                        break
                    except Exception as e:
                        logger.info(
                            f"第 {retry + 1} 次尝试加载页面失败: {str(e)}，等待2秒后重试"
                        )
                        if retry == 4:  # 最后一次尝试也失败
                            logger.info("页面加载失败次数达到上限，返回空结果")
                            browser.close()
                            return []
                        time.sleep(2)

                tv_set = set()  # 使用集合存储不重复的电视剧信息
                retry_count = 0
                max_retries = 5

                def process_items():
                    content = page.content()
                    soup = BeautifulSoup(content, "html.parser")
                    tv_items = soup.find_all("div", class_="categorypack_yk_pack_v")

                    for item in tv_items:
                        try:
                            # 获取标题
                            title_elem = item.find("div", class_="categorypack_title")
                            title = (
                                title_elem.find("a").get("title")
                                if title_elem
                                else "未知标题"
                            )

                            # 获取更新状态
                            status_elem = item.find("span", class_="categorypack_p_rb")
                            update_text = (
                                status_elem.get_text(strip=True)
                                if status_elem
                                else "暂无更新信息"
                            )

                            # 将信息转换为元组并添加到集合中
                            tv_info = (title, update_text, "0")
                            tv_set.add(tv_info)
                        except Exception as item_error:
                            logger.info(f"处理条目时出错: {str(item_error)}")
                            continue

                    return len(tv_items)

                while len(tv_set) < self._tv_limit:
                    logger.info(f"累计抓取到 {len(tv_set)} 条数据")
                    current_count = process_items()

                    if len(tv_set) >= self._tv_limit:
                        break

                    self.scroll_down(page, "div.categorypack_yk_pack_v")

                    new_count = process_items()
                    if new_count == current_count:
                        retry_count += 1
                        if retry_count >= max_retries:
                            logger.info(
                                f"没有更多内容加载，当前获取到 {len(tv_set)} 个不重复条目"
                            )
                            break
                    else:
                        retry_count = 0
                logger.info(f"不重复条目最终为: {len(tv_set)}")

                # 转换集合为列表并打印结果
                tv_list = [
                    {"title": title, "status": status, "year": year}
                    for title, status, year in list(tv_set)[: self._tv_limit]
                ]
                for idx, tv in enumerate(tv_list, start=1):
                    logger.info(
                        f"优酷视频({idx}/{len(tv_list)}): 剧名: {tv['title']}, 更新状态: {tv['status']}, 年份: {tv['year']}"
                    )

                browser.close()
                return tv_list

        except Exception as e:
            logger.info(f"发生错误: {str(e)}， traceback: {traceback.format_exc()}")
            return []

    def get_iqiyi_tv_list(self) -> List:
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)

                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                    locale="zh-CN",
                    timezone_id="Asia/Shanghai",
                )

                page = context.new_page()

                page.evaluate(
                    """
                    () => {
                        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3]});
                        Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'zh-CN']});
                        delete window.chrome;
                        delete navigator.__proto__.webdriver;
                    }
                """
                )

                page.set_viewport_size(
                    {
                        "width": 1400,
                        "height": 900,
                        "device_scale_factor": 1,
                        "is_mobile": False,
                    }
                )
                page.set_extra_http_headers({"Accept-Language": "zh-CN,zh;q=0.9"})

                url = "https://www.iqiyi.com/tv/"
                # 打开页面
                for retry in range(5):
                    try:
                        logger.info(f"尝试第 {retry + 1} 次加载页面：{url}")
                        page.goto(url, timeout=30000, wait_until="domcontentloaded")
                        logger.debug("爱奇艺有可能作妖，随机等待5-20秒")
                        time.sleep(random.uniform(5, 20))
                        break
                    except Exception as e:
                        logger.info(
                            f"第 {retry + 1} 次尝试加载页面失败: {str(e)}，等待2秒后重试"
                        )
                        if retry == 4:  # 最后一次尝试也失败
                            logger.info("页面加载失败次数达到上限，返回空结果")
                            browser.close()
                            return []
                        logger.debug("爱奇艺有可能作妖，随机等待5-20秒")
                        time.sleep(random.uniform(5, 20))

                # 等待并点击"全部剧集"按钮
                if page.locator(
                    "div.halo_divContainer__czfwR span#text", has_text="全部剧集"
                ).is_visible():
                    logger.debug("点击'全部剧集'")
                    page.locator("div.halo_divContainer__czfwR").filter(
                        has_text="全部剧集"
                    ).click()
                else:
                    logger.info("未找到目标元素")
                    return []
                logger.debug("爱奇艺有可能作妖，随机等待5-20秒")
                time.sleep(random.uniform(5, 20))

                # 等待并点击"最新"按钮
                logger.debug("点击'最新'")
                page.wait_for_selector("div.filmlib_itemwrap__3wgIE")
                page.locator("div.filmlib_itemwrap__3wgIE", has_text="最新").click()
                time.sleep(2)

                tv_set = set()  # 使用集合存储不重复的电视剧信息
                retry_count = 0
                max_retries = 5

                def process_items():
                    content = page.content()
                    soup = BeautifulSoup(content, "html.parser")
                    tv_items = soup.find_all(
                        "div", class_="tiles-item_container__OaNPB"
                    )

                    for item in tv_items:
                        try:
                            # 获取标题
                            title_elem = item.find(
                                "p", class_="tiles-item_title__H5i8p"
                            )
                            title = (
                                title_elem.get_text(strip=True)
                                if title_elem
                                else "未知标题"
                            )

                            # 获取更新状态
                            status_elem = item.find(
                                "span", class_="card-memos_update_info_text__M8ybR"
                            )
                            update_text = (
                                status_elem.get_text(strip=True)
                                if status_elem
                                else "暂无更新信息"
                            )

                            # 将信息转换为元组并添加到集合中
                            tv_info = (title, update_text, "0")
                            tv_set.add(tv_info)
                        except Exception as item_error:
                            logger.info(f"处理条目时出错: {str(item_error)}")
                            continue

                    return len(tv_items)

                while len(tv_set) < self._tv_limit:
                    logger.info(f"累计抓取到 {len(tv_set)} 条数据")
                    current_count = process_items()

                    if len(tv_set) >= self._tv_limit:
                        break

                    self.scroll_down(page, "div.tiles-item_container__OaNPB")

                    new_count = process_items()
                    if new_count == current_count:
                        retry_count += 1
                        if retry_count >= max_retries:
                            logger.info(
                                f"没有更多内容加载，当前获取到 {len(tv_set)} 个不重复条目"
                            )
                            break
                    else:
                        retry_count = 0
                logger.info(f"不重复条目最终为: {len(tv_set)}")

                # 转换集合为列表并打印结果
                tv_list = [
                    {"title": title, "status": status, "year": year}
                    for title, status, year in list(tv_set)[: self._tv_limit]
                ]
                for idx, tv in enumerate(tv_list, start=1):
                    logger.info(
                        f"爱奇艺({idx}/{len(tv_list)}): 剧名: {tv['title']}, 更新状态: {tv['status']}, 年份: {tv['year']}"
                    )

                browser.close()
                return tv_list

        except Exception as e:
            logger.info(f"发生错误: {str(e)}， traceback: {traceback.format_exc()}")
            return []

    def get_tv_list(self) -> List:
        # 获取爱奇艺视频电视剧列表
        logger.info("开始获取爱奇艺视频电视剧列表")
        iqiyi_tv_list = self.get_iqiyi_tv_list() or []
        if len(iqiyi_tv_list) == 0:
            iqiyi_tv_list = self.get_iqiyi_tv_list() or []
        logger.info(f"爱奇艺视频电视剧列表获取完成，共获取到{len(iqiyi_tv_list)}条信息")

        # 获取优酷视频电视剧列表
        logger.info("开始获取优酷视频电视剧列表")
        youku_tv_list = self.get_youku_tv_list() or []
        if len(youku_tv_list) == 0:
            youku_tv_list = self.get_youku_tv_list() or []
        logger.info(f"优酷视频电视剧列表获取完成，共获取到{len(youku_tv_list)}条信息")

        # 获取腾讯视频电视剧列表
        logger.info("开始获取腾讯视频电视剧列表")
        qq_tv_list = self.get_qq_tv_list() or []
        if len(qq_tv_list) == 0:
            qq_tv_list = self.get_qq_tv_list() or []
        logger.info(f"腾讯视频电视剧列表获取完成，共获取到{len(qq_tv_list)}条信息")

        # 合并三个列表并根据名字去重，优先保留有年份的
        all_tv_list = []
        tv_dict = {}

        # 用于去除季数部分的正则表达式
        def clean_title(title):
            # 去掉 " 第X季" 格式的季数信息
            return re.sub(r" 第\d+季", "", title)

        # 处理所有列表
        for tv_list in [qq_tv_list, youku_tv_list, iqiyi_tv_list]:
            if tv_list:
                for tv in tv_list:
                    title = tv["title"]
                    cleaned_title = re.sub(
                        r"[第\s共]+[0-9一二三四五六七八九十\-\s]+季", "", title
                    )  # 清理掉季数部分

                    if cleaned_title in tv_dict:
                        # 已存在相同标题的条目
                        existing_tv = tv_dict[cleaned_title]
                        if existing_tv["year"] == "0":
                            # 如果已存储的条目的年份是0，用新条目替换
                            tv_dict[cleaned_title] = tv
                        else:
                            # 如果已存储的条目的年份不是0，跳过新条目
                            continue
                    else:
                        # 新条目，直接添加
                        tv_dict[cleaned_title] = tv

        # 将字典转换回列表
        all_tv_list = list(tv_dict.values())

        # 打印合并后的结果
        for idx, tv in enumerate(all_tv_list, start=1):
            # 删除剧名中的`[普通话版]`
            tv["title"] = tv["title"].replace("[普通话版]", "")

            logger.info(
                f"总列表({idx}/{len(all_tv_list)}): 剧名: {tv['title']}, 更新状态: {tv['status']}, 年份: {tv['year']}"
            )

        return all_tv_list

    def get_qq_movie_list(self) -> List:
        pass

    def get_youku_movie_list(self) -> List:
        pass

    def get_iqiyi_movie_list(self) -> List:
        pass

    def get_movie_list(self) -> List:
        # TODO: 电影也要搞，有时间把那个脚本中搞电影的地方也移植过来
        pass

    def main(self):
        """
        立即运行一次
        """
        try:
            logger.info(f"插件{self.plugin_name} v{self.plugin_version} 开始运行")

            tv_list = self.get_tv_list()
            # tv_list = [
            #     {"title": "一梦枕星河", "status": "", "year": "2024"},
            #     {"title": "白鹿原", "status": "", "year": "2012"},
            #     {"title": "生活大爆炸 第二季", "status": "", "year": "0"},
            #     {"title": "乡村爱情17", "status": "", "year": "0"},
            #     {"title": "喧闹一家亲", "status": "", "year": "2016"},
            #     {"title": "绝命毒师", "status": "16集全", "year": "2008"},
            #     {"title": "亲爱的阮小枫", "status": "更新至14集", "year": "2025"},
            #     {"title": "夜魔侠：重生", "status": "更新至2集", "year": "2025"},
            #     {
            #         "title": "幸福加奈子的快乐杀手生活",
            #         "status": "更新至3集",
            #         "year": "2025",
            #     },
            #     {"title": "走向大西南", "status": "23集全", "year": "0"},
            # ]
            subscribe_update_list = []
            db = ScopedSession()
            for idx, tv in enumerate(tv_list, start=1):
                try:
                    logger.info(
                        f"处理抓取到的条目({idx}/{len(tv_list)}): {tv['title']}, 更新状态: {tv['status']}, 年份: {tv['year']}"
                    )
                    # 识别
                    fake_path_1 = (
                        f"{tv['title']} ({tv['year']})"
                        if tv["year"] != "0"
                        else tv["title"]
                    )
                    # 面向结果编程 e.g. '白鹿原 (2012) {[mtype=tv]}'
                    fake_path_2 = f"{fake_path_1} {{[mtype=tv]}}"
                    recognize_result = self.mediacahin.recognize_by_path(fake_path_2)
                    if recognize_result.media_info is not None:
                        tmdb_id = recognize_result.media_info.tmdb_id
                        title = recognize_result.media_info.title
                        year = recognize_result.media_info.year
                        first_air_date = recognize_result.media_info.first_air_date
                        last_air_date = recognize_result.media_info.last_air_date
                        seasons = recognize_result.media_info.seasons
                        status = recognize_result.media_info.status
                        logger.debug(
                            f"'{fake_path_1}' 的识别结果为: '{title} ({year}) tmdb_id={tmdb_id}', 首集播出日期={first_air_date}, 最新集播出日期={last_air_date}, 季集={seasons}, 状态={status}"
                        )

                        # 查询媒体库中是否存在
                        media_in = MediaInfo(
                            title=title,
                            type="电视剧",
                            year=year,
                            tmdb_id=tmdb_id,
                        )
                        tv_show_not_exist = not_exists(
                            media_in=media_in
                        )  # 电视剧返回缺失的剧集
                        if tv_show_not_exist == []:  # 空列表就是所有集都有
                            # if False:  # 调试用，用于直接到else
                            logger.info(
                                f"电视剧 '{title} ({year}) tmdb_id={tmdb_id}' 所有集已经存在于媒体库中，跳过"
                            )
                            continue
                        else:
                            log_msg = f"电视剧:'{title} ({year}) tmdb_id={tmdb_id}'中共有{len(tv_show_not_exist)}季不完整, 其中,  "
                            need_search_again = False
                            for absent_season in tv_show_not_exist:
                                season_number = absent_season.season
                                missing_episodes = absent_season.episodes
                                if not missing_episodes:  # 某季中缺少的集为: 全部
                                    log_msg += f"第{season_number}季中缺少的集为: 全部"
                                    logger.info(log_msg)
                                    last_air_date = (
                                        recognize_result.media_info.last_air_date
                                    )
                                    if (
                                        last_air_date is not None
                                        and last_air_date
                                        <= datetime.now().strftime("%Y-%m-%d")
                                    ):
                                        logger.debug(
                                            f"电视剧:'{title} ({year}) tmdb_id={tmdb_id}'最新集的air_date={last_air_date}, 小于等于当前日期, 开始更新订阅状态或添加新订阅"
                                        )
                                        need_search_again = True
                                    else:
                                        logger.debug(
                                            f"电视剧:'{title} ({year}) tmdb_id={tmdb_id}'最新集的播出日期={last_air_date}, 大于当前日期或不存在, 跳过"
                                        )
                                else:  # 某季中缺少的集不为: 全部
                                    episode_list = ", ".join(
                                        str(episode) for episode in missing_episodes
                                    )
                                    # 将字符串转换为整数列表
                                    episode_list = [
                                        int(episode.strip())
                                        for episode in episode_list.split(",")
                                    ]
                                    episode_list.sort()  # 从小到大排序
                                    log_msg += f"第{season_number}季中缺少的单集为: {episode_list}"
                                    logger.debug(log_msg)

                                    last_air_date = (
                                        recognize_result.media_info.last_air_date
                                    )

                                    # 如果last_air_date小于等于当前日期
                                    if (
                                        last_air_date is not None
                                        and last_air_date
                                        <= datetime.now().strftime("%Y-%m-%d")
                                    ):
                                        logger.debug(
                                            f"电视剧:'{title} ({year}) tmdb_id={tmdb_id}'最新一集的播出日期={last_air_date}小于等于当前日期，开始更新订阅状态或添加新订阅"
                                        )
                                        need_search_again = True
                                    else:
                                        logger.debug(
                                            f"电视剧:'{title} ({year}) tmdb_id={tmdb_id}'最新一集的播出日期={last_air_date}大于当前日期或不存在，跳过"
                                        )

                                if need_search_again:
                                    # begin:不管是某季缺少的是不是全部，处理都一样，更新订阅状态或添加订阅
                                    """
                                    三种方法：
                                    1.直接更新表state对应的标志位state，或添加新订阅(暂用这个)
                                    2.删除订阅并重新订阅
                                    """

                                    # 查询是否已经存在订阅

                                    exists = self.subscribeoper.exists(
                                        tmdbid=tmdb_id,
                                        season=season_number,
                                    )
                                    if exists:
                                        logger.debug(
                                            f"电视剧:'{title} ({year}) tmdb_id={tmdb_id}' 第{season_number}季已经存在订阅, 判断是否需要更新订阅状态"
                                        )

                                        # 更新state
                                        # 直接查表
                                        # query_result = (
                                        #     db.query(Subscribe)
                                        #     .filter(
                                        #         Subscribe.tmdbid == tmdb_id,
                                        #         Subscribe.season == season_number,
                                        #     )
                                        #     .first()
                                        # )

                                        # 还是用api吧
                                        query_result = subscribe_mediaid(
                                            mediaid=f"tmdb:{tmdb_id}",
                                            season=season_number,
                                            db=db,
                                        )
                                        subscribe_id = query_result.id

                                        # 状态：N-新建 R-订阅中 P-待定 S-暂停
                                        state = query_result.state
                                        if state != "N":
                                            subscribe = Subscribe.get(db, subscribe_id)
                                            subscribe_update_list.append(subscribe)
                                            logger.info(
                                                f"已添加到待更新订阅状态列表: 电视剧:'{title} ({year})', tmdb_id={tmdb_id}, Season={season_number}, 订阅id={subscribe_id}"
                                            )
                                        else:
                                            logger.info(
                                                f"无需更新订阅状态：电视剧:'{title} ({year})', tmdb_id={tmdb_id}, Season{season_number}, 订阅id={subscribe_id}"
                                            )

                                    else:
                                        # 添加订阅

                                        subscribe_id = SubscribeChain().add(
                                            mtype=schemas_MediaType.TV,
                                            title=recognize_result.media_info.title,
                                            year=recognize_result.media_info.year,
                                            tmdbid=tmdb_id,
                                            season=season_number,
                                            username="mysqlSubscribe",
                                            exist_ok=True,
                                        )
                                        if subscribe_id:
                                            logger.info(
                                                f"添加订阅成功: 电视剧:'{title} ({year})', tmdb_id={tmdb_id}, Season{season_number}, 订阅id={subscribe_id[0]}"
                                            )
                                            self.random_sleep()
                                    # end: 不管是某季缺少的是不是全部，处理都一样，更新订阅状态或添加订阅
                    else:
                        logger.info(f"{fake_path_1} 识别不出，跳过")
                except Exception as e:
                    logger.error(
                        f"插件mysqlSubscribe遍历列表出错，错误信息：{e}, traceback：{traceback.format_exc()}"
                    )

            # 批量更新订阅状态为N, 在下次 新增订阅搜索 的时候自动搜索下载
            for idx, subscribe in enumerate(subscribe_update_list, start=1):
                try:
                    # 遇到过报错'0 were matched'，故先查下表
                    exists = self.subscribeoper.exists(
                        tmdbid=subscribe.tmdbid,
                        season=subscribe.season,
                    )

                    if not exists:
                        continue

                    old_subscribe_dict = subscribe.to_dict()
                    name_and_year = (
                        f"{subscribe.name} ({subscribe.year})"
                        if subscribe.year and subscribe.name
                        else "None"
                    )

                    logger.info(
                        f"开始更新订阅状态({idx}/{len(subscribe_update_list)}): 电视剧:'{name_and_year}', tmdb_id={subscribe.tmdbid}, Season={subscribe.season}, 订阅id={subscribe.id}"
                    )
                    subscribe.update(db, {"state": "N"})
                    logger.info(
                        f"更新订阅状态成功({idx}/{len(subscribe_update_list)}): 电视剧:'{name_and_year}', tmdb_id={subscribe.tmdbid}, Season={subscribe.season}, 订阅id={subscribe.id}, 将会在下次 新增订阅搜索 时搜索下载"
                    )
                    # 发送订阅调整事件
                    eventmanager.send_event(
                        EventType.SubscribeModified,
                        {
                            "subscribe_id": subscribe.id,
                            "old_subscribe_info": old_subscribe_dict,
                            "subscribe_info": subscribe.to_dict(),
                        },
                    )
                    self.random_sleep()
                except Exception as e:
                    logger.error(
                        f"插件mysqlSubscribe更新订阅状态失败，subscribe={subscribe.__dict__}, 错误信息：{e}, traceback：{traceback.format_exc()}"
                    )

            logger.info("插件mysqlSubscribe运行完成")

        except Exception as e:
            logger.error(
                f"插件{self.plugin_name} V{self.plugin_version} 运行失败，错误信息:{e}，traceback={traceback.format_exc()}"
            )

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册插件公共服务
        返回一个每天在随机时间运行一次的服务
        """
        if self._enabled:

            # 每天凌晨0点到6点之间，随机执行1次
            triggers = TimerUtils.random_scheduler(
                num_executions=1,
                begin_hour=0,
                end_hour=6,
                max_interval=6 * 60,
                min_interval=2 * 60,
            )

            ret_jobs = []
            for trigger in triggers:
                ret_jobs.append(
                    {
                        "id": f"mysqlSubscribe|{trigger.hour}:{trigger.minute}",
                        "name": "mysqlSubscribe自动订阅",
                        "trigger": "cron",
                        "func": self.main,
                        "kwargs": {"hour": trigger.hour, "minute": trigger.minute},
                    }
                )
            return ret_jobs
        else:
            return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VText",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": "说明: 插件每天在凌晨0-6点随机时间运行一次，\n分别抓取指定数量个爱优腾的最新电视剧(如设置了100，则总共抓取300个电视剧，建议从10逐步增加到100就差不多了。别一开始直接拉满！一天添加1500个订阅)，\n去重后根据本地媒体库是否存在，更新订阅状态或添加订阅。",
                                            "style": {
                                                "white-space": "pre-line",
                                                "word-wrap": "break-word",
                                                "height": "auto",
                                                "max-height": "300px",
                                                "overflow-y": "auto",
                                            },
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12},
                                        "content": [
                                            {
                                                "component": "VProgressLinear",
                                            }
                                        ],
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "onlyonce",
                                            "label": "立即运行一次",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSlider",
                                        "props": {
                                            "model": "tv_limit",
                                            "label": "每个来源的获取数量",
                                            "min": 1,
                                            "max": 500,
                                            "step": 1,
                                            "thumb-label": "always",
                                            "hide-details": "false",
                                            "style": "width: 350px",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "tv_limit": 100,
        }

    def get_page(self) -> List[dict]:
        pass

    def stop_service(self):
        """
        退出插件
        """
        if self._scheduler:
            self._scheduler.remove_all_jobs()
            if self._scheduler.running:
                self._event.set()
                self._scheduler.shutdown()
                self._event.clear()
            self._scheduler = None


# TODO: 抓取结果写入数据库的必要性? 如果随机等待时间过长，而中途容器或插件被关闭或重启，抓取结果就会丢失
# TODO: webUI显示本插件订阅历史的必要性?
# TODO: 每次更新或新增订阅后的随机等待时间通过webUI设置?
# TODO: 是否有必要把抓取结果直接推送到Github，插件再统一从GitHub下载最新数据，避免抓取次数过多被针对?
# TODO: 芒果tv先不搞，观察一段时间，感觉都是短剧，tmdb没有信息: https://www.mgtv.com/lib/2?lastp=list_index&lastp=ch_tv&kind=a1&area=a1&year=all&sort=c1&chargeInfo=a1&fpa=2912
# TODO: hulu: https://www.justwatch.com/us/new
# TODO: 这个网站聚合了很多可以抓的东西，不过中文字幕不能保证，甚至电视剧标题tmdb都不一定有中文翻译，https://www.tvinsider.com/shows/calendar/
