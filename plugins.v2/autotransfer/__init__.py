import traceback
import threading
import shutil
import re
import pytz
import os
import datetime
import time
from typing import List, Tuple, Dict, Any, Optional
from pathlib import Path
from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.background import BackgroundScheduler
from app.utils.system import SystemUtils
from app.utils.string import StringUtils
from app.schemas.types import EventType, MediaType, SystemConfigKey
from app.schemas import Notification
from app.plugins import _PluginBase
from app.modules.filemanager import FileManagerModule
from app.log import logger
from app.helper.downloader import DownloaderHelper
from app.helper.directory import DirectoryHelper
from app.db.transferhistory_oper import TransferHistoryOper
from app.db.downloadhistory_oper import DownloadHistoryOper
from app.core.metainfo import MetaInfoPath
from app.core.meta import MetaBase
from app.core.context import MediaInfo
from app.core.config import settings
from app.chain import ChainBase
from app.chain.transfer import TransferChain
from app.chain.tmdb import TmdbChain
from app.chain.storage import StorageChain
from app.chain.media import MediaChain
from app.schemas import (
    NotificationType,
    TransferInfo,
    TransferDirectoryConf,
    ServiceInfo,
)


lock = threading.Lock()


class autoTransfer(_PluginBase):
    # 插件名称
    plugin_name = "autoTransfer"
    # 插件描述
    plugin_desc = "类似v1的目录监控，可定期整理文件"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/BrettDean/MoviePilot-Plugins/main/icons/autotransfer.png"
    # 插件版本
    plugin_version = "1.0.41"
    # 插件作者
    plugin_author = "Dean"
    # 作者主页
    author_url = "https://github.com/BrettDean/MoviePilot-Plugins"
    # 插件配置项ID前缀
    plugin_config_prefix = "autoTransfer_"
    # 加载顺序
    plugin_order = 4
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _scheduler = None
    transferhis = None
    downloadhis = None
    transferchain = None
    tmdbchain = None
    mediaChain = None
    storagechain = None
    chainbase = None
    _enabled = False
    _notify = False
    _onlyonce = False
    _history = False
    _scrape = False
    _category = False
    _refresh = False
    _reset_plunin_data = False
    _softlink = False
    _strm = False
    _del_empty_dir = False
    _downloaderSpeedLimit = 0
    _pathAfterMoveFailure = None
    _cron = None
    filetransfer = None
    _size = 0
    _downloaders_limit_enabled = False
    # 转移方式
    _transfer_type = "move"
    _monitor_dirs = ""
    _exclude_keywords = ""
    _interval: int = 300
    # 存储源目录与目的目录关系
    _dirconf: Dict[str, Optional[Path]] = {}
    # 存储源目录转移方式
    _transferconf: Dict[str, Optional[str]] = {}
    _overwrite_mode: Dict[str, Optional[str]] = {}
    _medias = {}
    # 退出事件
    _event = threading.Event()
    _move_failed_files = True
    _move_excluded_files = True

    def init_plugin(self, config: dict = None):
        self.transferhis = TransferHistoryOper()
        self.downloadhis = DownloadHistoryOper()
        self.transferchain = TransferChain()
        self.tmdbchain = TmdbChain()
        self.mediaChain = MediaChain()
        self.storagechain = StorageChain()
        self.chainbase = ChainBase()
        self.filetransfer = FileManagerModule()
        self.downloader_helper = DownloaderHelper()
        # 清空配置
        self._dirconf = {}
        self._transferconf = {}
        self._overwrite_mode = {}

        # 读取配置
        if config:
            self._enabled = config.get("enabled")
            self._notify = config.get("notify")
            self._onlyonce = config.get("onlyonce")
            self._history = config.get("history")
            self._scrape = config.get("scrape")
            self._category = config.get("category")
            self._refresh = config.get("refresh")
            self._reset_plunin_data = config.get("reset_plunin_data")
            self._transfer_type = config.get("transfer_type")
            self._monitor_dirs = config.get("monitor_dirs") or ""
            self._exclude_keywords = config.get("exclude_keywords") or ""
            self._interval = config.get("interval") or 300
            self._cron = config.get("cron") or "*/10 * * * *"
            self._size = config.get("size") or 0
            self._softlink = config.get("softlink")
            self._strm = config.get("strm")
            self._del_empty_dir = config.get("del_empty_dir") or False
            self._pathAfterMoveFailure = config.get("pathAfterMoveFailure") or None
            self._downloaderSpeedLimit = config.get("downloaderSpeedLimit") or 0
            self._downloaders = config.get("downloaders")
            self._move_failed_files = config.get("move_failed_files", True)
            self._move_excluded_files = config.get("move_excluded_files", True)
            self._downloaders_limit_enabled = config.get(
                "downloaders_limit_enabled", False
            )

        # 停止现有任务
        self.stop_service()

        if self._enabled or self._onlyonce:
            # 定时服务管理器
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            if self._notify:
                # 追加入库消息统一发送服务
                self._scheduler.add_job(self.send_msg, trigger="interval", seconds=15)

            # 读取目录配置
            monitor_dirs = self._monitor_dirs.split("\n")
            if not monitor_dirs:
                return
            for mon_path in monitor_dirs:
                # 格式源目录:目的目录
                if not mon_path:
                    continue

                # 自定义覆盖方式
                _overwrite_mode = "never"
                if mon_path.count("@") == 1:
                    _overwrite_mode = mon_path.split("@")[1]
                    mon_path = mon_path.split("@")[0]

                # 自定义转移方式
                _transfer_type = self._transfer_type
                if mon_path.count("#") == 1:
                    _transfer_type = mon_path.split("#")[1]
                    mon_path = mon_path.split("#")[0]

                # 存储目的目录
                if SystemUtils.is_windows():
                    if mon_path.count(":") > 1:
                        paths = [
                            mon_path.split(":")[0] + ":" + mon_path.split(":")[1],
                            mon_path.split(":")[2] + ":" + mon_path.split(":")[3],
                        ]
                    else:
                        paths = [mon_path]
                else:
                    paths = mon_path.split(":")

                # 目的目录
                target_path = None
                if len(paths) > 1:
                    mon_path = paths[0]
                    target_path = Path(paths[1])
                    self._dirconf[mon_path] = target_path
                else:
                    self._dirconf[mon_path] = None

                # 转移方式
                self._transferconf[mon_path] = _transfer_type
                self._overwrite_mode[mon_path] = _overwrite_mode

                if self._enabled:
                    # 检查媒体库目录是不是下载目录的子目录
                    try:
                        if target_path and target_path.is_relative_to(Path(mon_path)):
                            logger.warn(
                                f"目的目录:{target_path} 是源目录: {mon_path} 的子目录，无法整理"
                            )
                            self.systemmessage.put(
                                f"目的目录:{target_path} 是源目录: {mon_path} 的子目录，无法整理",
                            )
                            continue
                    except Exception as e:
                        logger.debug(str(e))

            # 重置插件运行数据
            if bool(self._reset_plunin_data):
                self.__runResetPlunindata()
                self._reset_plunin_data = False
                self.__update_config()
                
            # 运行一次定时服务
            if self._onlyonce:
                logger.info("立即运行一次")
                self._scheduler.add_job(
                    name="autotransfer整理文件",
                    func=self.main,
                    trigger="date",
                    run_date=datetime.datetime.now(tz=pytz.timezone(settings.TZ))
                    + datetime.timedelta(seconds=3),
                )
                # 关闭一次性开关
                self._onlyonce = False
                # 保存配置
                self.__update_config()

            # 启动定时服务
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
                "notify": self._notify,
                "onlyonce": self._onlyonce,
                "transfer_type": self._transfer_type,
                "monitor_dirs": self._monitor_dirs,
                "exclude_keywords": self._exclude_keywords,
                "interval": self._interval,
                "history": self._history,
                "softlink": self._softlink,
                "strm": self._strm,
                "scrape": self._scrape,
                "category": self._category,
                "size": self._size,
                "refresh": self._refresh,
                "reset_plunin_data": self._reset_plunin_data,
                "cron": self._cron,
                "del_empty_dir": self._del_empty_dir,
                "pathAfterMoveFailure": self._pathAfterMoveFailure,
                "downloaderSpeedLimit": self._downloaderSpeedLimit,
                "downloaders": self._downloaders,
                "move_failed_files": self._move_failed_files,
                "move_excluded_files": self._move_excluded_files,
                "downloaders_limit_enabled": self._downloaders_limit_enabled,
            }
        )

    @property
    def service_info(self) -> Optional[ServiceInfo]:
        """
        服务信息
        """
        if not self._downloaders:
            logger.warning("尚未配置下载器，请检查配置")
            return None

        services = self.downloader_helper.get_services(name_filters=self._downloaders)

        if not services:
            logger.warning("获取下载器实例失败，请检查配置")
            return None

        active_services = {}
        for service_name, service_info in services.items():
            if service_info.instance.is_inactive():
                logger.warning(f"下载器 {service_name} 未连接，请检查配置")
            elif not self.check_is_qb(service_info):
                logger.warning(
                    f"不支持的下载器类型 {service_name}，仅支持QB，请检查配置"
                )
            else:
                active_services[service_name] = service_info

        if not active_services:
            logger.warning("没有已连接的下载器，请检查配置")
            return None

        return active_services

    def set_download_limit(self, download_limit):
        try:
            try:
                download_limit = int(download_limit)
            except Exception as e:
                logger.error(
                    f"download_limit 转换失败 {str(e)}, traceback={traceback.format_exc()}"
                )
                return False

            flag = True
            for service in self.service_info.values():
                downloader_name = service.name
                downloader_obj = service.instance
                if not downloader_obj:
                    logger.error(f"获取下载器失败 {downloader_name}")
                    continue
                _, upload_limit_current_val = downloader_obj.get_speed_limit()
                flag = flag and downloader_obj.set_speed_limit(
                    download_limit=int(download_limit),
                    upload_limit=int(upload_limit_current_val),
                )
            return flag
        except Exception as e:
            logger.error(
                f"设置下载限速失败 {str(e)}, traceback={traceback.format_exc()}"
            )
            return False

    def check_is_qb(self, service_info) -> bool:
        """
        检查下载器类型是否为 qbittorrent 或 transmission
        """
        if self.downloader_helper.is_downloader(
            service_type="qbittorrent", service=service_info
        ):
            return True
        elif self.downloader_helper.is_downloader(
            service_type="transmission", service=service_info
        ):
            return False
        return False

    def get_downloader_limit_current_val(self):
        """
        获取下载器当前的下载限速和上传限速

        :return: tuple of (download_limit_current_val, upload_limit_current_val)
        """
        for service in self.service_info.values():
            downloader_name = service.name
            downloader_obj = service.instance
            if not downloader_obj:
                logger.error(f"获取下载器失败 {downloader_name}")
                continue
            download_limit_current_val, upload_limit_current_val = (
                downloader_obj.get_speed_limit()
            )

        return download_limit_current_val, upload_limit_current_val

    def moveFailedFilesToPath(self, fail_reason, src):
        """
        转移失败的文件到指定的路径

        :param fail_reason: 失败的原因
        :param src: 需要转移的文件路径
        """
        if self._downloaders_limit_enabled:
            try:
                # 先获取当前下载器的限速
                download_limit_current_val, _ = self.get_downloader_limit_current_val()
                # 记录当前速度限制
                self.save_data(
                    key="download_limit_current_val", value=download_limit_current_val
                )
                if (
                    float(download_limit_current_val)
                    > float(self._downloaderSpeedLimit)
                    or float(download_limit_current_val) == 0
                ):
                    is_download_speed_limited = self.set_download_limit(
                        self._downloaderSpeedLimit
                    )
                    if is_download_speed_limited:
                        logger.info(
                            f"下载器限速成功设置为 {self._downloaderSpeedLimit} KiB/s"
                        )
                        # 记录已限速
                        self.save_data(
                            key="is_download_speed_limited",
                            value=is_download_speed_limited,
                        )
                    else:
                        logger.info(
                            f"下载器限速失败，请检查下载器 {', '.join(self._downloaders)} 的连通性，本次整理将跳过下载器限速"
                        )
                else:
                    logger.info(
                        f"不用设置下载器限速，当前下载器限速为 {download_limit_current_val} KiB/s 大于或等于设定值 {self._downloaderSpeedLimit} KiB/s"
                    )
            except Exception as e:
                logger.error(
                    f"下载器限速失败，请检查下载器 {', '.join(self._downloaders)} 的连通性，本次整理将跳过下载器限速"
                )
                logger.debug(
                    f"下载器限速失败: {str(e)}, traceback={traceback.format_exc()}"
                )
                self.save_data(key="is_download_speed_limited", value=False)

        try:
            logger.info(f"开始转移失败的文件 '{src}'")
            dst = self._pathAfterMoveFailure
            if dst[-1] == "/":
                dst = dst[:-1]
            new_dst = f"{dst}/{fail_reason}{src}"
            new_dst_dir = os.path.dirname(f"{dst}/{fail_reason}{src}")
            os.makedirs(new_dst_dir, exist_ok=True)
            # 检查是否有重名文件
            if os.path.exists(new_dst):
                timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
                filename, ext = os.path.splitext(new_dst)
                new_dst = f"{filename}_{timestamp}{ext}"
            shutil.move(src, new_dst)
            logger.info(f"成功移动转移失败的文件 '{src}' 到 '{new_dst}'")
        except Exception as e:  # noqa: F841
            logger.error(
                f"将转移失败的文件 '{src}' 移动到 '{new_dst}' 失败, traceback={traceback.format_exc()}"
            )

        # 恢复原速
        if self._downloaders_limit_enabled and self.get_data(
            key="is_download_speed_limited"
        ):
            recover_download_limit_success = self.set_download_limit(
                download_limit=self.get_data(key="download_limit_current_val")
                or download_limit_current_val
            )
            if recover_download_limit_success:
                logger.info("取消下载器限速成功")
                # 更新数据库中的限速状态为False
                self.save_data(
                    key="is_download_speed_limited",
                    value=recover_download_limit_success,
                )
            else:
                logger.error("取消下载器限速失败")

    def __update_plugin_state(self, value: str):
        """
        更新插件状态, 可能的值有:
        running: 运行中
        finished: 运行完成
        failed: 运行失败
        toolong: 运行超过30分钟
        """
        # 记录运行状态
        self.save_data(key="plugin_state", value=value)

        # 记录当前时间
        if value != "toolong":
            self.save_data(
                key="plugin_state_time",
                value=str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            )
    def __runResetPlunindata(self):
        """
        重置插件数据
        """
        self.del_data(key="plugin_state")
        self.del_data(key="plugin_state_time")
        self.del_data(key="download_limit_current_val")
        self.del_data(key="is_download_speed_limited")

    def main(self):
        """
        立即运行一次
        """
        try:
            if self.get_data(key="plugin_state") == "running":
                last_state_time = self.get_data(key="plugin_state_time")
                # 如果上次运行在30分钟以内
                if (
                    last_state_time
                    and datetime.datetime.now()
                    - datetime.datetime.strptime(last_state_time, "%Y-%m-%d %H:%M:%S")
                    < datetime.timedelta(minutes=30)
                ):
                    logger.info(
                        f"插件{self.plugin_name} v{self.plugin_version} 上次运行未完成，跳过本次运行"
                    )
                    return
                else:  # 上次运行超过30分钟还没完成, 又来了新的任务，就慢慢排队等
                    pass
                    self.__update_plugin_state("toolong")
            else:
                self.__update_plugin_state("running")

            logger.info(f"插件{self.plugin_name} v{self.plugin_version} 开始运行")

            # 遍历所有目录
            for idx, mon_path in enumerate(self._dirconf.keys(), start=1):
                logger.info(f"开始处理目录({idx}/{len(self._dirconf)}): {mon_path} ...")
                list_files = SystemUtils.list_files(
                    directory=Path(mon_path),
                    extensions=settings.RMT_MEDIAEXT,
                    min_filesize=int(self._size),
                    recursive=True,
                )
                # 去除 .parts 文件
                list_files = [
                    f for f in list_files if not str(f).lower().endswith(".parts")
                ]
                logger.info(f"源目录 {mon_path} 共发现 {len(list_files)} 个视频待整理")
                unique_items = {}

                # 遍历目录下所有文件
                for idx, file_path in enumerate(list_files, start=1):
                    logger.info(
                        f"开始处理文件({idx}/{len(list_files)}) ({file_path.stat().st_size / 2**30:.2f} GiB): {file_path}"
                    )

                    transfer_result = self.__handle_file(
                        event_path=str(file_path), mon_path=mon_path
                    )
                    # 如果返回值是 None，则跳过
                    if transfer_result is None:
                        logger.debug(f"文件 {file_path} 不用刮削")
                        continue

                    transferinfo, mediainfo, file_meta = transfer_result
                    unique_key = Path(transferinfo.target_diritem.path)

                    # 存储不重复的项
                    if unique_key not in unique_items:
                        unique_items[unique_key] = (transferinfo, mediainfo, file_meta)

                # 刮削
                if self._scrape:
                    max_retries = 3  # 最大重试次数
                    for transferinfo, mediainfo, file_meta in unique_items.values():
                        retry_count = 1
                        while retry_count <= max_retries:
                            try:
                                logger.info(
                                    f"开始刮削目录: {transferinfo.target_diritem.path}"
                                )
                                self.mediaChain.scrape_metadata(
                                    fileitem=transferinfo.target_diritem,
                                    meta=file_meta,
                                    mediainfo=mediainfo,
                                )
                                logger.debug(
                                    f"刮削目录成功: {transferinfo.target_diritem.path}"
                                )
                                break  # 成功后跳出循环
                            except Exception as e:
                                logger.warning(
                                    f"目录第 {retry_count}/{max_retries} 次刮削失败: {transferinfo.target_diritem.path} ,错误信息: {e}"
                                )
                                # time.sleep(3 * 60)
                                time.sleep(3)
                                retry_count += 1
                                continue  # 重试

            logger.info("目录内所有文件整理完成！")
            self.__update_plugin_state("finished")

        except Exception as e:
            logger.error(
                f"插件{self.plugin_name} V{self.plugin_version} 运行失败，错误信息:{e}，traceback={traceback.format_exc()}"
            )
            self.__update_plugin_state("failed")

    def __update_file_meta(
        self, file_path: str, file_meta: Dict, get_by_path_result
    ) -> Dict:
        # 更新file_meta.tmdbid
        file_meta.tmdbid = (
            get_by_path_result.tmdbid
            if file_meta.tmdbid is None
            and get_by_path_result is not None
            and get_by_path_result.tmdbid is not None
            else file_meta.tmdbid
        )

        # 将字符串类型的get_by_path_result.type转换为MediaType中的类型
        if (
            get_by_path_result is not None
            and get_by_path_result.type is not None
            and get_by_path_result.type in MediaType._value2member_map_
        ):
            get_by_path_result.type = MediaType(get_by_path_result.type)

        # 更新file_meta.type
        file_meta.type = (
            get_by_path_result.type
            if file_meta.type.name != "TV"
            and get_by_path_result is not None
            and get_by_path_result.type is not None
            else file_meta.type
        )
        return file_meta

    def __handle_file(self, event_path: str, mon_path: str):
        """
        同步一个文件
        :param event_path: 事件文件路径
        :param mon_path: 监控目录
        """
        file_path = Path(event_path)
        try:
            if not file_path.exists():
                return
            # 全程加锁
            with lock:
                transfer_history = self.transferhis.get_by_src(event_path)
                if transfer_history:
                    logger.info(f"文件已处理过: {event_path}")
                    return

                # 回收站及隐藏的文件不处理
                if (
                    event_path.find("/@Recycle/") != -1
                    or event_path.find("/#recycle/") != -1
                    or event_path.find("/.") != -1
                    or event_path.find("/@eaDir") != -1
                ):
                    logger.debug(f"{event_path} 是回收站或隐藏的文件")
                    return

                # 命中过滤关键字不处理
                if self._exclude_keywords:
                    for keyword in self._exclude_keywords.split("\n"):
                        if keyword and re.findall(keyword, event_path):
                            logger.info(
                                f"{event_path} 命中过滤关键字 {keyword}，不处理"
                            )
                            if (
                                self._pathAfterMoveFailure is not None
                                and self._transfer_type == "move"
                                and self._move_excluded_files
                            ):
                                self.moveFailedFilesToPath(
                                    "命中过滤关键字", str(file_path)
                                )
                            return

                # 整理屏蔽词不处理
                transfer_exclude_words = self.systemconfig.get(
                    SystemConfigKey.TransferExcludeWords
                )
                if transfer_exclude_words:
                    for keyword in transfer_exclude_words:
                        if not keyword:
                            continue
                        if keyword and re.search(
                            f"{keyword}", event_path, re.IGNORECASE
                        ):
                            logger.info(
                                f"{event_path} 命中整理屏蔽词 {keyword}，不处理"
                            )
                            if (
                                self._pathAfterMoveFailure is not None
                                and self._transfer_type == "move"
                                and self._move_excluded_files
                            ):
                                self.moveFailedFilesToPath(
                                    "命中整理屏蔽词", str(file_path)
                                )
                            return

                # 不是媒体文件不处理
                if file_path.suffix.lower() not in [
                    ext.lower() for ext in settings.RMT_MEDIAEXT
                ]:
                    logger.debug(f"{event_path} 不是媒体文件")
                    return

                # 判断是不是蓝光目录
                if re.search(r"BDMV[/\\]STREAM", event_path, re.IGNORECASE):
                    # 截取BDMV前面的路径
                    blurray_dir = event_path[: event_path.find("BDMV")]
                    file_path = Path(blurray_dir)
                    logger.info(
                        f"{event_path} 是蓝光目录，更正文件路径为: {str(file_path)}"
                    )
                    # 查询历史记录，已转移的不处理
                    if self.transferhis.get_by_src(str(file_path)):
                        logger.info(f"{file_path} 已整理过")
                        return

                # 元数据
                file_meta = MetaInfoPath(file_path)
                if not file_meta.name:
                    logger.error(f"{file_path.name} 无法识别有效信息")
                    return

                # 通过文件路径从历史下载记录中获取tmdbid和type
                # 先通过文件路径来查
                get_by_path_result = self.downloadhis.get_by_path(str(file_path))
                if get_by_path_result is not None:
                    logger.info(
                        f"通过文件路径 {str(file_path)} 从历史下载记录中获取到tmdbid={get_by_path_result.tmdbid}，type={get_by_path_result.type}"
                    )
                    file_meta = self.__update_file_meta(
                        file_path=str(file_path),
                        file_meta=file_meta,
                        get_by_path_result=get_by_path_result,
                    )
                else:
                    # 不行再通过文件父目录来查
                    if str(file_path.parent) != mon_path:
                        parent_path = str(file_path.parent)
                        get_by_path_result = None

                        # 尝试获取get_by_path_result，最多parent 3次
                        for _ in range(3):
                            # 如果父路径已经是mon_path了，就没意义了
                            if parent_path == mon_path:
                                break

                            get_by_path_result = self.downloadhis.get_by_path(
                                parent_path
                            )
                            if get_by_path_result:
                                break  # 找到结果，跳出循环

                            parent_path = str(
                                Path(parent_path).parent
                            )  # 获取父目录路径

                        if get_by_path_result:
                            logger.info(
                                f"通过文件父目录 {parent_path} 从历史下载记录中获取到tmdbid={get_by_path_result.tmdbid}，type={get_by_path_result.type}"
                            )
                            file_meta = self.__update_file_meta(
                                file_path=str(file_path),
                                file_meta=file_meta,
                                get_by_path_result=get_by_path_result,
                            )
                    else:
                        logger.info(
                            f"未从历史下载记录中获取到 {str(file_path)} 的tmdbid和type，只能走正常识别流程"
                        )

                # 判断文件大小
                if (
                    self._size
                    and float(self._size) > 0
                    and file_path.stat().st_size < float(self._size) * 1024**3
                ):
                    logger.info(f"{file_path} 文件大小小于监控文件大小，不处理")
                    return

                # 查询转移目的目录
                target: Path = self._dirconf.get(mon_path)
                # 查询转移方式
                transfer_type = self._transferconf.get(mon_path)

                # 查找这个文件项
                file_item = self.storagechain.get_file_item(
                    storage="local", path=file_path
                )
                if not file_item:
                    logger.warn(f"{event_path.name} 未找到对应的文件")
                    return
                # 识别媒体信息
                mediainfo: MediaInfo = self.chain.recognize_media(meta=file_meta)
                if not mediainfo:
                    logger.warn(f"未识别到媒体信息，标题: {file_meta.name}")
                    # 新增转移成功历史记录
                    his = self.transferhis.add_fail(  # noqa: F841
                        fileitem=file_item, mode=transfer_type, meta=file_meta
                    )
                    if self._notify:
                        self.post_message(
                            mtype=NotificationType.Manual,
                            title=f"{file_path.name} 未识别到媒体信息，无法入库！\n",
                        )
                    # 转移失败文件到指定目录
                    if (
                        self._pathAfterMoveFailure is not None
                        and self._transfer_type == "move"
                        and self._move_failed_files
                    ):
                        self.moveFailedFilesToPath("未识别到媒体信息", file_item.path)
                    return

                # 如果未开启新增已入库媒体是否跟随TMDB信息变化则根据tmdbid查询之前的title
                if not settings.SCRAP_FOLLOW_TMDB:
                    transfer_history = self.transferhis.get_by_type_tmdbid(
                        tmdbid=mediainfo.tmdb_id, mtype=mediainfo.type.value
                    )
                    if transfer_history:
                        mediainfo.title = transfer_history.title
                logger.info(
                    f"{file_path.name} 识别为: {mediainfo.type.value} {mediainfo.title_year}"
                )

                # 获取集数据
                if mediainfo.type == MediaType.TV:
                    episodes_info = self.tmdbchain.tmdb_episodes(
                        tmdbid=mediainfo.tmdb_id,
                        season=(
                            1
                            if file_meta.begin_season is None
                            else file_meta.begin_season
                        ),
                    )
                else:
                    episodes_info = None

                # 查询转移目的目录
                target_dir = DirectoryHelper().get_dir(
                    mediainfo, src_path=Path(mon_path)
                )
                if (
                    not target_dir
                    or not target_dir.library_path
                    or not target_dir.download_path.startswith(mon_path)
                ):
                    target_dir = TransferDirectoryConf()
                    target_dir.library_path = target
                    target_dir.transfer_type = transfer_type
                    target_dir.scraping = self._scrape
                    target_dir.renaming = True
                    target_dir.notify = False
                    target_dir.overwrite_mode = (
                        self._overwrite_mode.get(mon_path) or "never"
                    )
                    target_dir.library_storage = "local"
                    target_dir.library_category_folder = self._category
                else:
                    target_dir.transfer_type = transfer_type
                    target_dir.scraping = self._scrape

                if not target_dir.library_path:
                    logger.error(f"未配置源目录 {mon_path} 的目的目录")
                    return

                # 下载器限速
                if self._downloaders_limit_enabled:
                    if (
                        target_dir.transfer_type
                        in [
                            "move",
                            "copy",
                            "rclone_copy",
                            "rclone_move",
                        ]
                        and self._downloaders_limit_enabled
                        and self._downloaderSpeedLimit != 0
                    ):
                        try:
                            # 先获取当前下载器的限速
                            download_limit_current_val, _ = (
                                self.get_downloader_limit_current_val()
                            )
                            # 记录当前速度限制
                            self.save_data(
                                key="download_limit_current_val",
                                value=download_limit_current_val,
                            )

                            if (
                                float(download_limit_current_val)
                                > float(self._downloaderSpeedLimit)
                                or float(download_limit_current_val) == 0
                            ):
                                logger.info(
                                    f"下载器限速 - {', '.join(self._downloaders)}，下载速度限制为 {self._downloaderSpeedLimit} KiB/s，因正在移动或复制文件{file_item.path}"
                                )
                                is_download_speed_limited = self.set_download_limit(
                                    self._downloaderSpeedLimit
                                )
                                self.save_data(
                                    key="is_download_speed_limited",
                                    value=is_download_speed_limited,
                                )
                                if not is_download_speed_limited:
                                    logger.error(
                                        f"下载器限速失败，请检查下载器 {', '.join(self._downloaders)}"
                                    )
                            else:
                                logger.info(
                                    f"不用设置下载器限速，当前下载器限速为 {download_limit_current_val} KiB/s 大于或等于设定值 {self._downloaderSpeedLimit} KiB/s"
                                )
                        except Exception as e:
                            logger.error(
                                f"下载器限速失败，请检查下载器 {', '.join(self._downloaders)} 的连通性，本次整理将跳过下载器限速"
                            )
                            logger.debug(
                                f"下载器限速失败: {str(e)}, traceback={traceback.format_exc()}"
                            )
                            self.save_data(
                                key="is_download_speed_limited",
                                value=False,
                            )
                    else:
                        if self._downloaderSpeedLimit == 0:
                            log_msg = "下载速度限制为0或为空，默认关闭限速"
                        elif target_dir.transfer_type not in [
                            "move",
                            "copy",
                            "rclone_copy",
                            "rclone_move",
                        ]:
                            log_msg = "转移方式不是移动或复制，下载器限速默认关闭"
                        logger.info(log_msg)
                else:
                    if not self._downloaders_limit_enabled:
                        logger.info("下载器限速未开启")

                # 转移文件
                transferinfo: TransferInfo = self.chain.transfer(
                    fileitem=file_item,
                    meta=file_meta,
                    mediainfo=mediainfo,
                    target_directory=target_dir,
                    episodes_info=episodes_info,
                )
                # 恢复原速
                if self._downloaders_limit_enabled and self.get_data(
                    key="is_download_speed_limited"
                ):
                    recover_download_limit_success = self.set_download_limit(
                        download_limit=self.get_data(key="download_limit_current_val")
                        or download_limit_current_val
                    )
                    if recover_download_limit_success:
                        logger.info("取消下载器限速成功")
                        # 更新数据库中的限速状态为False
                        self.save_data(key="is_download_speed_limited", value=False)
                    else:
                        logger.error("取消下载器限速失败")

                if not transferinfo:
                    logger.error("文件转移模块运行失败")
                    return

                if not transferinfo.success:
                    # 转移失败
                    logger.warn(f"{file_path.name} 入库失败: {transferinfo.message}")

                    if self._history:
                        # 新增转移失败历史记录
                        self.transferhis.add_fail(
                            fileitem=file_item,
                            mode=transfer_type,
                            meta=file_meta,
                            mediainfo=mediainfo,
                            transferinfo=transferinfo,
                        )
                    if self._notify:
                        self.post_message(
                            mtype=NotificationType.Manual,
                            title=f"{mediainfo.title_year}{file_meta.season_episode} 入库失败！",
                            text=f"原因: {transferinfo.message or '未知'}",
                            image=mediainfo.get_message_image(),
                        )
                    # 转移失败文件到指定目录
                    if (
                        self._pathAfterMoveFailure is not None
                        and self._transfer_type == "move"
                        and self._move_failed_files
                    ):
                        self.moveFailedFilesToPath(transferinfo.message, file_item.path)
                    return

                if self._history:
                    # 新增转移成功历史记录
                    self.transferhis.add_success(
                        fileitem=file_item,
                        mode=transfer_type,
                        meta=file_meta,
                        mediainfo=mediainfo,
                        transferinfo=transferinfo,
                    )

                if self._notify:
                    # 发送消息汇总
                    media_list = (
                        self._medias.get(mediainfo.title_year + " " + file_meta.season)
                        or {}
                    )
                    if media_list:
                        media_files = media_list.get("files") or []
                        if media_files:
                            file_exists = False
                            for file in media_files:
                                if str(file_path) == file.get("path"):
                                    file_exists = True
                                    break
                            if not file_exists:
                                media_files.append(
                                    {
                                        "path": str(file_path),
                                        "mediainfo": mediainfo,
                                        "file_meta": file_meta,
                                        "transferinfo": transferinfo,
                                    }
                                )
                        else:
                            media_files = [
                                {
                                    "path": str(file_path),
                                    "mediainfo": mediainfo,
                                    "file_meta": file_meta,
                                    "transferinfo": transferinfo,
                                }
                            ]
                        media_list = {
                            "files": media_files,
                            "time": datetime.datetime.now(),
                        }
                    else:
                        media_list = {
                            "files": [
                                {
                                    "path": str(file_path),
                                    "mediainfo": mediainfo,
                                    "file_meta": file_meta,
                                    "transferinfo": transferinfo,
                                }
                            ],
                            "time": datetime.datetime.now(),
                        }
                    self._medias[mediainfo.title_year + " " + file_meta.season] = (
                        media_list
                    )

                if self._refresh:
                    # 广播事件
                    self.eventmanager.send_event(
                        EventType.TransferComplete,
                        {
                            "meta": file_meta,
                            "mediainfo": mediainfo,
                            "transferinfo": transferinfo,
                        },
                    )

                if self._softlink:
                    # 通知实时软链接生成
                    self.eventmanager.send_event(
                        EventType.PluginAction,
                        {
                            "file_path": str(transferinfo.target_item.path),
                            "action": "softlink_file",
                        },
                    )

                if self._strm:
                    # 通知Strm助手生成
                    self.eventmanager.send_event(
                        EventType.PluginAction,
                        {
                            "file_path": str(transferinfo.target_item.path),
                            "action": "cloudstrm_file",
                        },
                    )

                # 移动模式删除空目录
                if transfer_type == "move" and self._del_empty_dir:
                    for file_dir in file_path.parents:
                        if len(str(file_dir)) <= len(str(Path(mon_path))):
                            # 重要，删除到监控目录为止
                            break
                        files = SystemUtils.list_files(
                            file_dir, settings.RMT_MEDIAEXT + settings.DOWNLOAD_TMPEXT
                        )
                        if not files:
                            logger.warn(f"移动模式，删除空目录: {file_dir}")
                            shutil.rmtree(file_dir, ignore_errors=True)

                # 返回成功的文件
                return transferinfo, mediainfo, file_meta

        except Exception as e:
            logger.error(f"目录监控发生错误: {str(e)} - {traceback.format_exc()}")
            return

    def send_transfer_message(
        self,
        meta: MetaBase,
        mediainfo: MediaInfo,
        transferinfo: TransferInfo,
        season_episode: Optional[str] = None,
        username: Optional[str] = None,
    ):
        """
        发送入库成功的消息
        """
        msg_title = f"{mediainfo.title_year} {meta.season_episode if not season_episode else season_episode} 已入库"
        if (
            transferinfo.file_count == 1
            and bool(meta.title)
            and bool(transferinfo.file_list_new[0])
        ):  # 如果只有一个文件
            msg_str = f"🎬 文件名: {os.path.basename(transferinfo.file_list_new[0])}\n💾 大小: {transferinfo.total_size / 2**30 :.2f} GiB"
        else:
            msg_str = (
                f"共{transferinfo.file_count}个视频\n"
                f"💾 大小: {transferinfo.total_size / 2**30 :.2f} GiB"
            )
        if hasattr(mediainfo, "category") and bool(mediainfo.category):
            msg_str = (
                f"{msg_str}\n📺 分类: {mediainfo.type.value} - {mediainfo.category}"
            )
        else:
            msg_str = f"{msg_str}\n📺 分类: {mediainfo.type.value}"

        if hasattr(mediainfo, "title") and bool(mediainfo.title):
            msg_str = f"{msg_str}\n🇨🇳 中文片名: {mediainfo.title}"
        # 电影是title, release_date
        # 电视剧是name, first_air_date
        if (
            mediainfo.type == MediaType.MOVIE
            and hasattr(mediainfo, "original_title")
            and bool(mediainfo.original_title)
        ):
            msg_str = f"{msg_str}\n🇬🇧 原始片名: {mediainfo.original_title}"
        elif (
            mediainfo.type == MediaType.TV
            and hasattr(mediainfo, "original_name")
            and bool(mediainfo.original_name)
        ):
            msg_str = f"{msg_str}\n🇬🇧 原始片名: {mediainfo.original_name}"
        if hasattr(mediainfo, "original_language") and bool(
            mediainfo.original_language
        ):
            language_mapping = {
                "kw": "康沃尔语",
                "ff": "富拉语",
                "gn": "瓜拉尼语",
                "id": "印尼语",
                "lu": "卢巴-加丹加语",
                "nr": "恩德贝莱语",
                "os": "奥塞梯语",
                "ru": "俄语",
                "se": "北萨米语",
                "so": "索马里语",
                "es": "西班牙语",
                "sv": "瑞典语",
                "ta": "泰米尔语",
                "te": "泰卢固语",
                "tn": "茨瓦纳语",
                "uk": "乌克兰语",
                "uz": "乌兹别克语",
                "el": "希腊语",
                "co": "科西嘉语",
                "dv": "迪维希语",
                "kk": "哈萨克语",
                "ki": "基库尤语",
                "or": "奥里亚语",
                "si": "僧伽罗语",
                "st": "索托语",
                "sr": "塞尔维亚语",
                "ss": "斯瓦蒂语",
                "tr": "土耳其语",
                "wa": "瓦隆语",
                "cn": "粤语",
                "bi": "比斯拉马语",
                "cr": "克里语",
                "cy": "威尔士语",
                "eu": "巴斯克语",
                "hz": "赫雷罗语",
                "ho": "希里莫图语",
                "ka": "格鲁吉亚语",
                "kr": "卡努里语",
                "km": "高棉语",
                "kj": "宽亚玛语",
                "to": "汤加语",
                "vi": "越南语",
                "zu": "祖鲁语",
                "zh": "中文",
                "ps": "普什图语",
                "mk": "马其顿语",
                "ae": "阿维斯陀语",
                "az": "阿塞拜疆语",
                "ba": "巴什基尔语",
                "sh": "塞尔维亚-克罗地亚语",
                "lv": "拉脱维亚语",
                "lt": "立陶宛语",
                "ms": "马来语",
                "rm": "罗曼什语",
                "as": "阿萨姆语",
                "gd": "盖尔语",
                "ja": "日语",
                "ko": "韩语",
                "ku": "库尔德语",
                "mo": "摩尔多瓦语",
                "mn": "蒙古语",
                "nb": "书面挪威语",
                "om": "奥罗莫语",
                "pi": "巴利语",
                "sq": "阿尔巴尼亚语",
                "vo": "沃拉普克语",
                "bo": "藏语",
                "da": "丹麦语",
                "kl": "格陵兰语",
                "kn": "卡纳达语",
                "nl": "荷兰语",
                "nn": "新挪威语",
                "sa": "梵语",
                "am": "阿姆哈拉语",
                "hy": "亚美尼亚语",
                "bs": "波斯尼亚语",
                "hr": "克罗地亚语",
                "mh": "马绍尔语",
                "mg": "马拉加斯语",
                "ne": "尼泊尔语",
                "su": "巽他语",
                "ts": "聪加语",
                "ug": "维吾尔语",
                "cs": "捷克语",
                "jv": "爪哇语",
                "ro": "罗马尼亚语",
                "sm": "萨摩亚语",
                "tg": "塔吉克语",
                "wo": "沃洛夫语",
                "br": "布列塔尼语",
                "fr": "法语",
                "ga": "爱尔兰语",
                "ht": "海地克里奥尔语",
                "kv": "科米语",
                "mi": "毛利语",
                "th": "泰语",
                "xx": "无语言",
                "af": "南非荷兰语",
                "av": "阿瓦尔语",
                "bm": "班巴拉语",
                "ca": "加泰罗尼亚语",
                "ce": "车臣语",
                "de": "德语",
                "gv": "马恩语",
                "rw": "卢旺达语",
                "ky": "吉尔吉斯语",
                "ln": "林加拉语",
                "sn": "绍纳语",
                "yi": "意第绪语",
                "be": "白俄罗斯语",
                "cu": "教会斯拉夫语",
                "dz": "宗喀语",
                "eo": "世界语",
                "fi": "芬兰语",
                "fy": "弗里西语",
                "ie": "西方国际语",
                "ia": "国际语",
                "it": "意大利语",
                "ng": "恩敦加语",
                "pa": "旁遮普语",
                "pt": "葡萄牙语",
                "rn": "隆迪语",
                "fa": "波斯语",
                "ch": "查莫罗语",
                "cv": "楚瓦什语",
                "en": "英语",
                "hu": "匈牙利语",
                "ii": "彝语",
                "kg": "刚果语",
                "li": "林堡语",
                "ml": "马拉雅拉姆语",
                "nv": "纳瓦霍语",
                "ny": "齐切瓦语",
                "sg": "桑戈语",
                "tw": "契维语",
                "ab": "阿布哈兹语",
                "ar": "阿拉伯语",
                "ee": "埃维语",
                "fo": "法罗语",
                "ik": "伊努皮克语",
                "ks": "克什米尔语",
                "lb": "卢森堡语",
                "nd": "北恩德贝莱语",
                "oc": "奥克语",
                "sk": "斯洛伐克语",
                "tt": "鞑靼语",
                "ve": "文达语",
                "ay": "艾马拉语",
                "fj": "斐济语",
                "gu": "古吉拉特语",
                "io": "伊多语",
                "lo": "老挝语",
                "la": "拉丁语",
                "no": "挪威语",
                "oj": "奥吉布瓦语",
                "pl": "波兰语",
                "qu": "克丘亚语",
                "sl": "斯洛文尼亚语",
                "sc": "萨丁尼亚语",
                "sw": "斯瓦希里语",
                "tl": "他加禄语",
                "ur": "乌尔都语",
                "bg": "保加利亚语",
                "hi": "印地语",
                "yo": "约鲁巴语",
                "ak": "阿坎语",
                "an": "阿拉贡语",
                "bn": "孟加拉语",
                "et": "爱沙尼亚语",
                "gl": "加利西亚语",
                "ha": "豪萨语",
                "ig": "伊博语",
                "iu": "因纽特语",
                "lg": "卢干达语",
                "mr": "马拉地语",
                "mt": "马耳他语",
                "my": "缅甸语",
                "na": "瑙鲁语",
                "sd": "信德语",
                "xh": "科萨语",
                "za": "壮语",
                "aa": "阿法尔语",
                "is": "冰岛语",
                "ty": "塔希提语",
                "ti": "提格利尼亚语",
                "tk": "土库曼语",
                "he": "希伯来语",
            }

            msg_str = f"{msg_str}\n🗣 原始语言: {language_mapping.get(mediainfo.original_language, mediainfo.original_language)}"
        # 电影才有mediainfo.release_date?
        if (
            mediainfo.type == MediaType.MOVIE
            and hasattr(mediainfo, "release_date")
            and bool(mediainfo.release_date)
        ):
            msg_str = f"{msg_str}\n📅 首播日期: {mediainfo.release_date}"
        # 电视剧才有first_air_date?
        elif (
            mediainfo.type == MediaType.TV
            and hasattr(mediainfo, "first_air_date")
            and bool(mediainfo.first_air_date)
        ):
            msg_str = f"{msg_str}\n📅 首播日期: {mediainfo.first_air_date}"

        if mediainfo.type == MediaType.TV and bool(
            mediainfo.tmdb_info["last_air_date"]
        ):
            msg_str = (
                f"{msg_str}\n📅 最后播出日期: {mediainfo.tmdb_info['last_air_date']}"
            )
        if hasattr(mediainfo, "status") and bool(mediainfo.status):
            status_translation = {
                "Returning Series": "回归系列",
                "Ended": "已完结",
                "In Production": "制作中",
                "Canceled": "已取消",
                "Planned": "计划中",
                "Released": "已发布",
            }

            msg_str = f"{msg_str}\n✅ 完结状态: {status_translation[mediainfo.status] if mediainfo.status in status_translation else '未知状态'}"
        if hasattr(mediainfo, "vote_average") and bool(mediainfo.vote_average):
            msg_str = f"{msg_str}\n⭐ 观众评分: {mediainfo.vote_average}"
        if hasattr(mediainfo, "genres") and bool(mediainfo.genres):
            genres = ", ".join(genre["name"] for genre in mediainfo.genres)
            msg_str = f"{msg_str}\n🎭 类型: {genres}"
        if hasattr(mediainfo, "overview") and bool(mediainfo.overview):
            msg_str = f"{msg_str}\n📝 简介: {mediainfo.overview}"
        if bool(transferinfo.message):
            msg_str = f"{msg_str}\n以下文件处理失败: \n{transferinfo.message}"
        # 发送
        self.chainbase.post_message(
            Notification(
                mtype=NotificationType.Organize,
                title=msg_title,
                text=msg_str,
                image=mediainfo.get_message_image(),
                username=username,
                link=mediainfo.detail_link,
            )
        )

    def send_msg(self):
        """
        定时检查是否有媒体处理完，发送统一消息
        """
        if not self._medias or not self._medias.keys():
            return

        # 遍历检查是否已刮削完，发送消息
        for medis_title_year_season in list(self._medias.keys()):
            media_list = self._medias.get(medis_title_year_season)
            logger.info(f"开始处理媒体 {medis_title_year_season} 消息")

            if not media_list:
                continue

            # 获取最后更新时间
            last_update_time = media_list.get("time")
            media_files = media_list.get("files")
            if not last_update_time or not media_files:
                continue

            transferinfo = media_files[0].get("transferinfo")
            file_meta = media_files[0].get("file_meta")
            mediainfo = media_files[0].get("mediainfo")
            # 判断剧集或者电影最后更新时间距现在是已超过300秒，发送消息
            if (datetime.datetime.now() - last_update_time).total_seconds() > int(
                self._interval
            ) or mediainfo.type == MediaType.MOVIE:
                # 发送通知
                if self._notify:

                    # 汇总处理文件总大小
                    total_size = 0
                    file_count = 0

                    # 剧集汇总
                    episodes = []
                    for file in media_files:
                        transferinfo = file.get("transferinfo")
                        total_size += transferinfo.total_size
                        file_count += 1

                        file_meta = file.get("file_meta")
                        if file_meta and file_meta.begin_episode:
                            episodes.append(file_meta.begin_episode)

                    transferinfo.total_size = total_size
                    # 汇总处理文件数量
                    transferinfo.file_count = file_count

                    # 剧集季集信息 S01 E01-E04 || S01 E01、E02、E04
                    season_episode = None
                    # 处理文件多，说明是剧集，显示季入库消息
                    if mediainfo.type == MediaType.TV:
                        # 季集文本
                        season_episode = (
                            f"{file_meta.season} {StringUtils.format_ep(episodes)}"
                        )
                    # 发送消息
                    try:
                        self.send_transfer_message(
                            meta=file_meta,
                            mediainfo=mediainfo,
                            transferinfo=transferinfo,
                            season_episode=season_episode,
                        )
                    except Exception as e:
                        logger.error(
                            f"发送消息失败: {str(e)}, traceback={traceback.format_exc()}"
                        )
                        del self._medias[medis_title_year_season]
                # 发送完消息，移出key
                del self._medias[medis_title_year_season]
                continue

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
        [{
            "id": "服务ID",
            "name": "服务名称",
            "trigger": "触发器: cron/interval/date/CronTrigger.from_crontab()",
            "func": self.xxx,
            "kwargs": {} # 定时器参数
        }]
        """
        if self._enabled:
            return [
                {
                    "id": "autoTransfer",
                    "name": "类似v1的目录监控，可定期整理文件",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self.main,
                    "kwargs": {},
                }
            ]
        return []

    def __get_alert_props(self) -> Tuple[str, str, str]:
        """
        根据插件的状态获取对应的标签文本、颜色和样式。

        Args:
            plugin_state (str): 插件的运行状态，可能的值包括 "running", "finished", "failed"。

        Returns:
            Tuple[str, str, str]: 返回状态标签、颜色和样式。
        """
        plugin_state = self.get_data(key="plugin_state")
        plugin_state_time = self.get_data(key="plugin_state_time")
        # 定义默认的状态、颜色和样式
        status_label = ""
        alert_type = "info"  # 默认颜色
        alert_variant = "tonal"  # 默认样式

        if plugin_state == "running":
            status_label = f"插件目前正在运行，开始运行时间为 {plugin_state_time}"
            alert_type = "primary"  # 运行中状态，显示为紫色
            alert_variant = "filled"  # 填充样式
        elif plugin_state == "finished":
            status_label = (
                f"插件上次成功运行，运行完成于 {plugin_state_time}，当前没有在运行"
            )
            alert_type = "success"  # 成功状态，显示为绿色
            alert_variant = "filled"
        elif plugin_state == "failed":
            status_label = f"上次运行失败于 {plugin_state_time}，当前没有在运行"
            alert_type = "error"  # 失败状态，显示为红色
            alert_variant = "filled"
        elif plugin_state == "toolong":
            status_label = "上次运行时间长于30分钟"
            alert_type = "warning"  # 黄色
            alert_variant = "filled"
        else:
            status_label = "插件运行状态未知(运行一次即可更新状态)"
            alert_type = "warning"  # 黄色
            alert_variant = "filled"

        return status_label, alert_type, alert_variant

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:

        # 获取插件运行状态对应的标签、颜色和样式
        status_label, alert_type, alert_variant = self.__get_alert_props()

        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VForm",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": alert_type,
                                            "variant": alert_variant,
                                            "text": status_label,
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
                                                    "hint": "开启后将按照执行周期定期运行",
                                                    "persistent-hint": True,
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
                                                    "model": "notify",
                                                    "label": "发送通知",
                                                    "hint": "整理完成后发送通知",
                                                    "persistent-hint": True,
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
                                                    "model": "refresh",
                                                    "label": "刷新媒体库",
                                                    "hint": "每个文件整理完成后通知媒体库扫描",
                                                    "persistent-hint": True,
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
                                                    "model": "reset_plunin_data",
                                                    "label": "清空上次运行状态",
                                                    "hint": "手动清空上次运行状态，一般用不到，是调试插件时，直接停止主函数导致本插件运行状态没有更新才用的",
                                                    "persistent-hint": True,
                                                },
                                            }
                                        ],
                                    },
                                ],
                                
                            },
                            {
                                "component": "VForm",
                                "content": [
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
                                                            "model": "history",
                                                            "label": "存储历史记录",
                                                            "hint": "开启后会将整理记录储存到'媒体整理'",
                                                            "persistent-hint": True,
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
                                                            "model": "scrape",
                                                            "label": "是否刮削",
                                                            "hint": "每处理完一行监控目录，就刮削一次对应的图片和nfo文件",
                                                            "persistent-hint": True,
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
                                                            "model": "category",
                                                            "label": "是否二级分类",
                                                            "hint": "开与关的区别就是'媒体库'-'电视剧'-'国产剧'-'甄嬛传'和'媒体库'-'电视剧'-'甄嬛传'",
                                                            "persistent-hint": True,
                                                        },
                                                    }
                                                ],
                                            },
                                        ],
                                    }
                                ],
                            },
                            {
                                "component": "VForm",
                                "content": [
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
                                                            "model": "del_empty_dir",
                                                            "label": "删除空目录",
                                                            "hint": "移动完成后删除空目录，推荐关闭，此开关仅在转移方式为移动时有效",
                                                            "persistent-hint": True,
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
                                                            "model": "softlink",
                                                            "label": "软链接",
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
                                                            "model": "strm",
                                                            "label": "联动Strm生成",
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
                                                            "hint": "不论插件是否启用都立即运行一次",
                                                            "persistent-hint": True,
                                                        },
                                                    }
                                                ],
                                            },
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
                            {
                                "component": "VCol",
                                "props": {"cols": 3, "md": 3},
                                "content": [
                                    {
                                        "component": "VCronField",
                                        "props": {
                                            "model": "cron",
                                            "label": "执行周期",
                                            "placeholder": "*/10 * * * *",
                                            "hint": "使用cron表达式定期执行，推荐 */10 * * * *",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 3, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "size",
                                            "label": "最低整理大小(MiB)",
                                            "placeholder": "0",
                                            "hint": "默认0, 单位MiB, 只能输入数字",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 3, "md": 3},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "transfer_type",
                                            "label": "转移方式",
                                            "items": [
                                                {"title": "移动", "value": "move"},
                                                {"title": "复制", "value": "copy"},
                                                {"title": "硬链接", "value": "link"},
                                                {
                                                    "title": "软链接",
                                                    "value": "softlink",
                                                },
                                                {
                                                    "title": "Rclone复制",
                                                    "value": "rclone_copy",
                                                },
                                                {
                                                    "title": "Rclone移动",
                                                    "value": "rclone_move",
                                                },
                                            ],
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 3, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "interval",
                                            "label": "入库消息延迟(秒)",
                                            "placeholder": "300",
                                            "hint": "默认300, 单位秒, 只能输入数字",
                                            "persistent-hint": True,
                                        },
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
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "downloaders_limit_enabled",
                                            "label": "开启下载器限速",
                                            "hint": "开启后，在移动或复制文件时会限制qb下载速度，默认关闭",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "multiple": True,
                                            "chips": True,
                                            "clearable": True,
                                            "model": "downloaders",
                                            "label": "选择转移时要限速的下载器",
                                            "items": [
                                                *[
                                                    {
                                                        "title": config.name,
                                                        "value": config.name,
                                                    }
                                                    for config in self.downloader_helper.get_configs().values()
                                                    if config.type == "qbittorrent"
                                                ],
                                            ],
                                            "hint": "列表中只会有qb",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "downloaderSpeedLimit",
                                            "label": "转移时下载器限速(KiB/s)",
                                            "placeholder": "0或留空不限速",
                                            "hint": "默认0, 单位KiB/s, 只能输入数字, 推荐1",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
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
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12},
                                        "content": [
                                            {
                                                "component": "VTextarea",
                                                "props": {
                                                    "model": "monitor_dirs",
                                                    "label": "监控目录",
                                                    "rows": 7,
                                                    "auto-grow": "{{ monitor_dirs.length > 0 }}",
                                                    "placeholder": "每一行一个目录，支持以下几种配置方式，转移方式支持 move、copy、link、softlink、rclone_copy、rclone_move：\n"
                                                    "监控目录:转移目的目录\n"
                                                    "监控目录:转移目的目录#转移方式\n"
                                                    "例如:\n/Downloads/电影/:/Library/电影/\n/Downloads/电视剧/:/Library/电视剧/\n/mnt/手动备份/电影/:/Library/手动备份/电影/#copy",
                                                    "hint": "如果'监控目录'中的视频在'设定'-'储存&目录'中的'资源目录'或其子目录中，则插件设置的目的目录无效，会自动使用'设定'中的'媒体库目录'",
                                                    "persistent-hint": True,
                                                },
                                            }
                                        ],
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
                                "props": {
                                    "cols": 12,
                                },
                                "content": [
                                    {
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "exclude_keywords",
                                            "label": "排除关键词",
                                            "rows": 1,
                                            "auto-grow": "{{ monitor_dirs.length > 0 }}",
                                            "placeholder": "正则, 区分大小写, 一行一个正则表达式",
                                            "hint": "正则, 区分大小写, 一行一个正则表达式",
                                            "persistent-hint": True,
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
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VRow",
                                        "content": [
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12, "md": 6},
                                                "content": [
                                                    {
                                                        "component": "VSwitch",
                                                        "props": {
                                                            "model": "move_failed_files",
                                                            "label": "移动失败文件",
                                                            "hint": "当转移失败时移动文件，如'未识别到媒体信息'、'媒体库存在同名文件'、'未识别到文件集数'",
                                                            "persistent-hint": True,
                                                        },
                                                    }
                                                ],
                                            },
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12, "md": 6},
                                                "content": [
                                                    {
                                                        "component": "VSwitch",
                                                        "props": {
                                                            "model": "move_excluded_files",
                                                            "label": "移动匹配 屏蔽词/关键字 的文件",
                                                            "hint": "当命中过滤规则时移动文件",
                                                            "persistent-hint": True,
                                                        },
                                                    }
                                                ],
                                            },
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
                                "props": {
                                    "cols": 12,
                                },
                                "content": [
                                    {
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "pathAfterMoveFailure",
                                            "label": "移动到的路径",
                                            "rows": 1,
                                            "auto-grow": "{{ monitor_dirs.length > 0 }}",
                                            "placeholder": "如 /mnt/failed",
                                            "hint": "移动方式，当整理失败或命中关键词后，将文件移动到此路径(会根据失败原因和原目录结构将文件移动到此处)，只能有一个路径，留空或'转移方式'不是'移动'或不满足上面两个开关的条件均不会移动。",
                                            "persistent-hint": True,
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
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": "1.入库消息延迟默认300s，如网络较慢可酌情调大，有助于发送统一入库消息。\n2.源目录与目的目录设置一致，则默认使用目录设置配置。否则可在源目录后拼接@覆盖方式（默认never覆盖方式）。\n3.开启软链接/Strm会在监控转移后联动【实时软链接】/【云盘Strm[助手]】插件生成软链接/Strm（只处理媒体文件，不处理刮削文件）。\n4.启用此插件后，可将'设定'-'存储&目录'-'目录'-'自动整理'改为'不整理'或'手动整理'\n5.'转移时下载器限速'只在移动(或复制)时生效，他会在每次移动(或复制)前，限制qb下载速度，转移完成后再恢复限速前的速度\n6.'是否二级分类'与'设定'-'储存&目录'-'媒体库目录'-'按类别分类'开关冲突时，以'设定'中的为准\n7.前3排开关，前2排推荐全部打开，第3排推荐全部关闭\n\n此插件由thsrite的目录监控插件修改而得\n本意是为了做类似v1的定时整理，因我只用本地移动，故也不知软/硬链、Strm之类的是否可用",
                                            "style": {
                                                "white-space": "pre-line",
                                                "word-wrap": "break-word",
                                                "height": "auto",
                                                "max-height": "320px",
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
                                "props": {
                                    "cols": 12,
                                },
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": "排除关键词推荐使用下面9行(一行一个):\n```\nSpecial Ending Movie\n\\[((TV|BD|\\bBlu-ray\\b)?\\s*CM\\s*\\d{2,3})\\]\n\\[Teaser.*?\\]\n\\[PV.*?\\]\n\\[NC[OPED]+.*?\\]\n\\[S\\d+\\s+Recap(\\s+\\d+)?\\]\n\\b(CDs|SPs|Scans|Bonus|映像特典|特典CD|/mv)\\b\n\\b(NC)?(Disc|SP|片头|OP|片尾|ED|PV|CM|MENU|EDPV|SongSpot|BDSpot)(\\d{0,2}|_ALL)\\b\n(?i)\\b(sample|preview|menu|special)\\b\n```\n排除bdmv再加入下面2行:\n```\n(?i)\\d+\\.(m2ts|mpls)$\n(?i)\\.bdmv$\n```\n",
                                            "style": {
                                                "white-space": "pre-line",
                                                "word-wrap": "break-word",
                                                "height": "auto",
                                                "max-height": "500px",
                                                "overflow-y": "auto",
                                            },
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "notify": False,
            "onlyonce": False,
            "history": False,
            "scrape": False,
            "category": False,
            "refresh": True,
            "reset_plunin_data": False,
            "softlink": False,
            "strm": False,
            "transfer_type": "move",
            "monitor_dirs": "",
            "exclude_keywords": "",
            "interval": 300,
            "cron": "*/10 * * * *",
            "size": 0,
            "del_empty_dir": False,
            "downloaderSpeedLimit": 0,
            "downloaders": "不限速",
            "pathAfterMoveFailure": None,
            "move_failed_files": True,
            "move_excluded_files": True,
            "downloaders_limit_enabled": False,
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
