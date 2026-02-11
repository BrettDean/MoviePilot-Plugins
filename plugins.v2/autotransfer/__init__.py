import traceback
import threading
import shutil
import re
import pytz
import os
import datetime
import time
import fcntl
from typing import List, Tuple, Dict, Any, Optional
from pathlib import Path
from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.background import BackgroundScheduler
from app.utils.system import SystemUtils
from app.utils.string import StringUtils
from app.schemas.types import EventType, MediaType, SystemConfigKey
from app.plugins import _PluginBase
from app.modules.filemanager import FileManagerModule
from app.log import logger
from app.helper.mediaserver import MediaServerHelper
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
    Notification,
    NotificationType,
    TransferDirectoryConf,
    TransferInfo,
    RefreshMediaItem,
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
    plugin_version = "1.0.47"
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
    _stop_transfer = False
    _history = False
    _scrape = False
    _category = False
    _refresh = False
    _refresh_modified = False
    _mediaservers = None
    _delay = 10
    mediaserver_helper = None
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
    # 文件锁文件描述符
    _lock_fd = None
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
        self.mediaserver_helper = MediaServerHelper()
        # 初始化文件锁路径
        self._lock_file_path = os.path.join(
            settings.TEMP_PATH,
            f"autotransfer_{self.plugin_name}.lock"
        )
        # 清空配置
        self._dirconf = {}
        self._transferconf = {}
        self._overwrite_mode = {}

        # 读取配置
        if config:
            self._enabled = config.get("enabled")
            self._notify = config.get("notify")
            self._onlyonce = config.get("onlyonce")
            self._stop_transfer = config.get("stop_transfer")
            self._history = config.get("history")
            self._scrape = config.get("scrape")
            self._category = config.get("category")
            self._refresh = config.get("refresh")
            self._refresh_modified = config.get("refresh_modified")
            self._mediaservers = config.get("mediaservers") or []
            self._delay = config.get("delay") or 10
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

        # 重置插件运行数据
        if bool(self._reset_plunin_data):
            self.__runResetPlunindata()
            self._reset_plunin_data = False
            self.__update_config()
            logger.info("重置插件运行数据成功")

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

            # 停止当前运行
            if self._stop_transfer:
                logger.info("停止本次运行")
                result = self.stop_transfer()
                # 关闭停止开关
                self._stop_transfer = False
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
                "stop_transfer": self._stop_transfer,
                "history": self._history,
                "scrape": self._scrape,
                "category": self._category,
                "refresh": self._refresh,
                "refresh_modified": self._refresh_modified,
                "mediaservers": self._mediaservers,
                "delay": self._delay,
                "reset_plunin_data": self._reset_plunin_data,
                "transfer_type": self._transfer_type,
                "monitor_dirs": self._monitor_dirs,
                "exclude_keywords": self._exclude_keywords,
                "interval": self._interval,
                "cron": self._cron,
                "size": self._size,
                "softlink": self._softlink,
                "strm": self._strm,
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

    def _apply_downloader_speed_limit(self, log_message: str = None):
        """
        应用下载器限速
        :param log_message: 日志消息
        :return: 是否成功限速
        """
        if not self._downloaders_limit_enabled or self._downloaderSpeedLimit == 0:
            return False
        
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
                if log_message:
                    logger.info(log_message)
                else:
                    logger.info(
                        f"下载器限速成功设置为 {self._downloaderSpeedLimit} KiB/s"
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
                return is_download_speed_limited
            else:
                logger.info(
                    f"不用设置下载器限速，当前下载器限速为 {download_limit_current_val} KiB/s 大于或等于设定值 {self._downloaderSpeedLimit} KiB/s"
                )
                return False
        except Exception as e:
            logger.error(
                f"下载器限速失败，请检查下载器 {', '.join(self._downloaders)} 的连通性，本次整理将跳过下载器限速"
            )
            logger.debug(
                f"下载器限速失败: {str(e)}, traceback={traceback.format_exc()}"
            )
            self.save_data(key="is_download_speed_limited", value=False)
            return False

    def moveFailedFilesToPath(self, fail_reason, src):
        """
        转移失败的文件到指定的路径

        :param fail_reason: 失败的原因
        :param src: 需要转移的文件路径
        """
        # 应用下载器限速
        self._apply_downloader_speed_limit()

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
        self._recover_downloader_speed_limit()

    def _acquire_lock(self) -> bool:
        """
        获取文件锁
        :return: 是否成功获取锁
        """
        try:
            # 尝试创建并锁定文件
            self._lock_fd = os.open(self._lock_file_path, os.O_CREAT | os.O_WRONLY)
            fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            logger.info(f"成功获取文件锁: {self._lock_file_path}")
            return True
        except BlockingIOError:
            logger.warning(
                f"文件锁已被占用，插件已在运行中，跳过本次运行"
            )
            return False
        except Exception as e:
            logger.error(f"获取文件锁失败: {e}")
            return False

    def _release_lock(self):
        """
        释放文件锁
        """
        if self._lock_fd is not None:
            try:
                fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
                os.close(self._lock_fd)
                self._lock_fd = None
                logger.info(f"已释放文件锁: {self._lock_file_path}")
            except Exception as e:
                logger.error(f"释放文件锁失败: {e}")
        
        if self._lock_file_path and os.path.exists(self._lock_file_path):
            try:
                os.remove(self._lock_file_path)
            except Exception as e:
                logger.error(f"删除锁文件失败: {e}")

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
        
        # 处理进度数据
        if value == "finished":
            # 正常完成，清理进度显示数据
            self.del_data(key="transfer_progress")
        elif value == "failed":
            # 失败，清理进度显示数据
            self.del_data(key="transfer_progress")
        elif value == "toolong":
            # 运行时间过长，保留进度数据
            pass
        elif value == "running":
            # 开始运行，初始化进度
            pass
        elif value == "stopped":
            # 手动停止，清理进度显示数据
            self.del_data(key="transfer_progress")

    def _set_stop_flag(self):
        """
        设置停止标志
        """
        self.save_data(key="stop_flag", value=True)
        logger.info("已设置停止标志，将在当前文件/刮削完成后停止")

    def _check_stop_flag(self) -> bool:
        """
        检查是否需要停止
        :return: 是否需要停止
        """
        stop_flag = self.get_data(key="stop_flag")
        if stop_flag:
            logger.info("检测到停止标志，将停止后续处理")
            return True
        return False

    def _clear_stop_flag(self):
        """
        清除停止标志
        """
        self.del_data(key="stop_flag")

    def __runResetPlunindata(self):
        """
        重置插件数据
        """
        self.del_data(key="plugin_state")
        self.del_data(key="plugin_state_time")
        self.del_data(key="download_limit_current_val")
        self.del_data(key="is_download_speed_limited")
        self.del_data(key="transfer_progress")

    def _get_file_key(self, file_path: str) -> str:
        """
        获取文件的唯一标识key
        :param file_path: 文件路径
        :return: 文件key
        """
        return f"file:{file_path}"

    def _init_file_status(self, file_path: str):
        """
        初始化文件状态
        :param file_path: 文件路径
        """
        file_key = self._get_file_key(file_path)
        self.save_data(
            key=file_key,
            value={
                "status": "pending",
                "file_path": file_path,
                "create_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "start_time": None,
                "end_time": None,
                "target_path": None,
                "transfer_type": None,
                "error_message": None
            }
        )

    def _update_file_status(self, file_path: str, status: str, **kwargs):
        """
        更新文件状态
        :param file_path: 文件路径
        :param status: 状态 (pending, processing, completed, failed)
        :param kwargs: 其他更新字段
        """
        file_key = self._get_file_key(file_path)
        file_status = self.get_data(key=file_key)
        
        if file_status:
            file_status["status"] = status
            file_status.update(kwargs)
            if status == "processing" and not kwargs.get("start_time"):
                file_status["start_time"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if status in ["completed", "failed"] and not kwargs.get("end_time"):
                file_status["end_time"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.save_data(key=file_key, value=file_status)

    def _get_file_status(self, file_path: str) -> Optional[Dict]:
        """
        获取文件状态
        :param file_path: 文件路径
        :return: 文件状态信息
        """
        file_key = self._get_file_key(file_path)
        return self.get_data(key=file_key)

    def _get_all_file_statuses(self, file_paths: List[str]) -> Dict[str, Dict]:
        """
        批量获取文件状态
        :param file_paths: 文件路径列表
        :return: 文件路径到状态的映射
        """
        all_data = self.plugindata.get_data_all("autoTransfer")
        status_map = {}
        for data in all_data:
            if data.key.startswith("file:"):
                file_path = data.key[5:]
                status_map[file_path] = data.value
        return {path: status_map.get(path) for path in file_paths}

    def _recover_interrupted_files(self) -> List[str]:
        """
        恢复中断的文件（状态为processing的文件）
        :return: 中断的文件路径列表
        """
        all_data = self.plugindata.get_data_all("autoTransfer")
        interrupted_files = []
        
        for data in all_data:
            if data.key.startswith("file:") and data.value.get("status") == "processing":
                file_path = data.key[5:]
                interrupted_files.append(file_path)
                logger.warning(f"发现中断的文件: {file_path}")
        
        return interrupted_files

    def _cleanup_old_file_records(self, days: int = 30):
        """
        清理旧的已完成文件记录
        :param days: 保留天数
        """
        cutoff_time = datetime.datetime.now() - datetime.timedelta(days=days)
        all_data = self.plugindata.get_data_all("autoTransfer")
        cleaned_count = 0
        
        for data in all_data:
            if data.key.startswith("file:") and data.value.get("status") == "completed":
                end_time_str = data.value.get("end_time")
                if end_time_str:
                    try:
                        end_time = datetime.datetime.strptime(end_time_str, "%Y-%m-%d %H:%M:%S")
                        if end_time < cutoff_time:
                            self.del_data(key=data.key)
                            cleaned_count += 1
                    except Exception as e:
                        logger.debug(f"解析文件记录时间失败: {data.key}, {e}")
        
        if cleaned_count > 0:
            logger.info(f"清理了 {cleaned_count} 个旧的已完成文件记录")

    def _get_scrape_key(self, scrape_path: str) -> str:
        """
        获取刮削路径的唯一标识key
        :param scrape_path: 刮削路径
        :return: 刮削key
        """
        return f"scrape:{scrape_path}"

    def _init_scrape_status(self, scrape_path: str):
        """
        初始化刮削状态
        :param scrape_path: 刮削路径
        """
        scrape_key = self._get_scrape_key(scrape_path)
        
        # 检查是否已存在
        existing_status = self.get_data(key=scrape_key)
        if existing_status:
            logger.debug(f"刮削状态已存在: {scrape_path}")
            return
        
        self.save_data(
            key=scrape_key,
            value={
                "status": "pending",
                "scrape_path": scrape_path,
                "create_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "start_time": None,
                "end_time": None,
                "error_message": None
            }
        )

    def _update_scrape_status(self, scrape_path: str, status: str, **kwargs):
        """
        更新刮削状态
        :param scrape_path: 刮削路径
        :param status: 状态 (pending, processing, completed, failed)
        :param kwargs: 其他更新字段
        """
        scrape_key = self._get_scrape_key(scrape_path)
        scrape_status = self.get_data(key=scrape_key)
        
        # 如果状态不存在，创建新的状态记录
        if not scrape_status:
            scrape_status = {
                "status": status,
                "scrape_path": scrape_path,
                "create_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "start_time": None,
                "end_time": None,
                "error_message": None
            }
        else:
            # 更新现有状态
            scrape_status["status"] = status
            scrape_status.update(kwargs)
        
        # 设置时间戳
        if status == "processing" and not kwargs.get("start_time"):
            scrape_status["start_time"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if status in ["completed", "failed"] and not kwargs.get("end_time"):
            scrape_status["end_time"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 保存状态
        self.save_data(key=scrape_key, value=scrape_status)

    def _get_scrape_status(self, scrape_path: str) -> Optional[Dict]:
        """
        获取刮削状态
        :param scrape_path: 刮削路径
        :return: 刮削状态信息
        """
        scrape_key = self._get_scrape_key(scrape_path)
        return self.get_data(key=scrape_key)

    def _get_all_scrape_statuses(self, scrape_paths: List[str]) -> Dict[str, Dict]:
        """
        批量获取刮削状态
        :param scrape_paths: 刮削路径列表
        :return: 刮削路径到状态的映射
        """
        all_data = self.plugindata.get_data_all("autoTransfer")
        status_map = {}
        for data in all_data:
            if data.key.startswith("scrape:"):
                scrape_path = data.key[7:]
                status_map[scrape_path] = data.value
        return {path: status_map.get(path) for path in scrape_paths}

    def _recover_interrupted_scrapes(self) -> List[str]:
        """
        恢复中断的刮削（状态为processing的刮削路径）
        :return: 中断的刮削路径列表
        """
        all_data = self.plugindata.get_data_all("autoTransfer")
        interrupted_scrapes = []
        
        for data in all_data:
            if data.key.startswith("scrape:") and data.value.get("status") == "processing":
                scrape_path = data.key[7:]
                interrupted_scrapes.append(scrape_path)
                logger.warning(f"发现中断的刮削路径: {scrape_path}")
        
        return interrupted_scrapes

    def _process_interrupted_scrapes(self, interrupted_scrapes: List[str]):
        """
        处理中断的刮削路径
        :param interrupted_scrapes: 中断的刮削路径列表
        """
        if not interrupted_scrapes:
            return
        
        logger.info(f"开始处理 {len(interrupted_scrapes)} 个中断的刮削路径")
        
        for scrape_path in interrupted_scrapes:
            try:
                # 获取刮削状态
                scrape_status = self._get_scrape_status(scrape_path)
                if not scrape_status:
                    logger.warning(f"未找到刮削状态: {scrape_path}")
                    continue
                
                # 更新刮削状态为处理中
                self._update_scrape_status(scrape_path, "processing")
                
                logger.info(f"重新处理刮削路径: {scrape_path}")
                
                # 重新获取 mediainfo 和 file_meta（通过重新识别媒体）
                try:
                    # 获取文件元数据
                    file_meta = self.chainbase.get_file_item(Path(scrape_path))
                    if file_meta:
                        # 识别媒体信息
                        mediainfo = self.chain.recognize_media(meta=file_meta)
                        
                        # 发送刮削请求
                        self.mediaChain.scrape_metadata(
                            fileitem=Path(scrape_path),
                            meta=file_meta,
                            mediainfo=mediainfo,
                        )
                    else:
                        logger.warning(f"无法获取文件元数据: {scrape_path}")
                except Exception as e:
                    logger.warning(f"重新识别媒体信息失败: {scrape_path}, {e}")
                
                # 发送刮削请求后立即标记为完成
                self._update_scrape_status(scrape_path, "completed")
                logger.info(f"刮削路径处理完成: {scrape_path}")
            except Exception as e:
                logger.error(f"处理中断刮削路径 {scrape_path} 失败: {e}")
                # 即使失败也标记为完成（因为已经发送了刮削请求）
                self._update_scrape_status(
                    scrape_path, "completed", error_message=str(e)
                )

    def _cleanup_old_scrape_records(self, days: int = 30):
        """
        清理旧的已完成刮削记录
        :param days: 保留天数
        """
        cutoff_time = datetime.datetime.now() - datetime.timedelta(days=days)
        all_data = self.plugindata.get_data_all("autoTransfer")
        cleaned_count = 0
        
        for data in all_data:
            if data.key.startswith("scrape:") and data.value.get("status") == "completed":
                end_time_str = data.value.get("end_time")
                if end_time_str:
                    try:
                        end_time = datetime.datetime.strptime(end_time_str, "%Y-%m-%d %H:%M:%S")
                        if end_time < cutoff_time:
                            self.del_data(key=data.key)
                            cleaned_count += 1
                    except Exception as e:
                        logger.debug(f"解析刮削记录时间失败: {data.key}, {e}")
        
        if cleaned_count > 0:
            logger.info(f"清理了 {cleaned_count} 个旧的已完成刮削记录")

    @property
    def service_infos(self) -> Optional[Dict[str, ServiceInfo]]:
        """
        服务信息
        """
        if not self._mediaservers:
            logger.warning("尚未配置媒体服务器，请检查配置")
            return None

        services = self.mediaserver_helper.get_services(name_filters=self._mediaservers)
        if not services:
            logger.warning("获取媒体服务器实例失败，请检查配置")
            return None

        active_services = {}
        for service_name, service_info in services.items():
            if service_info.instance.is_inactive():
                logger.warning(f"媒体服务器 {service_name} 未连接，请检查配置")
            else:
                active_services[service_name] = service_info

        if not active_services:
            logger.warning("没有已连接的媒体服务器，请检查配置")
            return None

        return active_services

    def main(self):
        """
        立即运行一次
        """
        # 尝试获取文件锁
        if not self._acquire_lock():
            return
        
        try:
            self.__update_plugin_state("running")

            logger.info(f"==========插件{self.plugin_name} v{self.plugin_version} 开始运行==========")

            # 清除旧的停止标志
            self._clear_stop_flag()

            # 恢复中断的文件
            interrupted_files = self._recover_interrupted_files()
            if interrupted_files:
                logger.info(f"发现 {len(interrupted_files)} 个中断的文件，将重新处理")

            # 恢复中断的刮削
            interrupted_scrapes = self._recover_interrupted_scrapes()
            if interrupted_scrapes:
                logger.info(f"发现 {len(interrupted_scrapes)} 个中断的刮削，将重新处理")
                # 处理中断的刮削
                self._process_interrupted_scrapes(interrupted_scrapes)

            # 清理旧的已完成记录
            self._cleanup_old_file_records(days=30)
            self._cleanup_old_scrape_records(days=30)

            # 遍历所有目录
            total_dirs = len(self._dirconf)
            dirs = list(self._dirconf.keys())
            
            for dir_idx, mon_path in enumerate(dirs, start=1):
                # 检查是否需要停止
                if self._check_stop_flag():
                    logger.info("检测到停止标志，停止目录处理")
                    self.__update_plugin_state("stopped")
                    break
                
                logger.info(f"开始处理目录({dir_idx}/{total_dirs}): {mon_path} ...")
                
                # 保存目录处理进度（用于状态显示）
                self.save_data(key="transfer_progress", value={
                    "status": "processing_dir",
                    "dir_idx": dir_idx,
                    "total_dirs": total_dirs,
                    "current_dir": mon_path
                })
                
                # 获取文件列表
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
                total_files = len(list_files)
                
                # 批量获取文件状态
                file_paths = [str(f) for f in list_files]
                file_statuses = self._get_all_file_statuses(file_paths)
                
                # 遍历目录下所有文件
                for file_idx, file_path in enumerate(list_files, start=1):
                    # 检查是否需要停止
                    if self._check_stop_flag():
                        logger.info("检测到停止标志，停止文件处理")
                        break
                    
                    file_path_str = str(file_path)
                    file_status = file_statuses.get(file_path_str)
                    
                    # # 跳过已完成的文件
                    # if file_status and file_status.get("status") == "completed":
                    #     logger.debug(f"文件 {file_path} 已处理完成，跳过")
                    #     continue
                    
                    logger.info(
                        f"开始处理文件({file_idx}/{total_files}) ({file_path.stat().st_size / 2**30:.2f} GiB): {file_path}"
                    )
                    
                    # 保存文件处理进度（用于状态显示）
                    self.save_data(key="transfer_progress", value={
                        "status": "processing_file",
                        "dir_idx": dir_idx,
                        "total_dirs": total_dirs,
                        "current_dir": mon_path,
                        "file_idx": file_idx,
                        "total_files": total_files,
                        "current_file": file_path_str,
                        "file_size": file_path.stat().st_size
                    })
                    
                    # 初始化或更新文件状态为处理中
                    if not file_status:
                        self._init_file_status(file_path_str)
                    self._update_file_status(file_path_str, "processing")
                    
                    try:
                        transfer_result = self.__handle_file(
                            event_path=file_path_str, mon_path=mon_path
                        )
                        # 如果返回值是 None，则跳过
                        if transfer_result is None:
                            logger.debug(f"文件 {file_path} 不用刮削")
                            self._update_file_status(file_path_str, "completed")
                            continue

                        transferinfo, mediainfo, file_meta = transfer_result
                        unique_key = Path(transferinfo.target_diritem.path)
                        # 统一路径格式，确保末尾有 /
                        scrape_path = str(unique_key).rstrip('/') + '/'

                        # 初始化刮削状态
                        self._init_scrape_status(scrape_path)

                        # 存储不重复的项
                        if unique_key not in unique_items:
                            unique_items[unique_key] = (transferinfo, mediainfo, file_meta)
                        
                        # 标记文件为已完成
                        self._update_file_status(
                            file_path_str,
                            "completed",
                            target_path=str(transferinfo.target_diritem.path),
                            transfer_type=self._transfer_type
                        )
                    except Exception as e:
                        logger.error(f"处理文件 {file_path} 失败: {e}")
                        self._update_file_status(
                            file_path_str,
                            "failed",
                            error_message=str(e)
                        )

                # 批量处理刮削
                if self._scrape and unique_items:
                    # 检查是否需要停止
                    if self._check_stop_flag():
                        logger.info("检测到停止标志，跳过刮削处理")
                    else:
                        # 过滤出需要刮削的项目（状态为 pending）
                        items_to_scrape = []
                        for unique_key, item in unique_items.items():
                            scrape_path = str(unique_key)
                            scrape_status = self._get_scrape_status(scrape_path)
                            if not scrape_status or scrape_status.get("status") != "completed":
                                items_to_scrape.append(item)
                        
                        if items_to_scrape:
                            logger.info(f"开始处理刮削，共 {len(items_to_scrape)} 个目录需要刮削")
                            self._batch_scrape(items_to_scrape)

                # 批量处理媒体库刷新
                if self._refresh and unique_items:
                    # 检查是否需要停止
                    if self._check_stop_flag():
                        logger.info("检测到停止标志，跳过媒体库刷新")
                    else:
                        self._batch_refresh_media(unique_items.values())
                elif self._refresh_modified and unique_items:
                    # 检查是否需要停止
                    if self._check_stop_flag():
                        logger.info("检测到停止标志，跳过媒体库刷新")
                    else:
                        self._batch_refresh_media_modified(unique_items.values())

            # 检查是否因为停止标志而停止
            if self._check_stop_flag():
                logger.info("检测到停止标志，停止插件运行")
                self.__update_plugin_state("stopped")
            else:
                logger.info("目录内所有文件整理完成！")
                self.__update_plugin_state("finished")

        except Exception as e:
            logger.error(
                f"插件{self.plugin_name} V{self.plugin_version} 运行失败，错误信息:{e}，traceback={traceback.format_exc()}"
            )
            self.__update_plugin_state("failed")
        finally:
            # 清除停止标志
            self._clear_stop_flag()
            # 释放文件锁
            self._release_lock()
            
    def _batch_scrape(self, items):
        """
        批量处理刮削
        :param items: 待刮削的项目列表
        """
        max_retries = 3  # 最大重试次数
        for transferinfo, mediainfo, file_meta in items:
            # 检查是否需要停止
            if self._check_stop_flag():
                logger.info("检测到停止标志，停止刮削处理")
                break
            
            scrape_path = str(transferinfo.target_diritem.path)
            retry_count = 1
            while retry_count <= max_retries:
                try:
                    logger.info(
                        f"开始刮削目录: {scrape_path}"
                    )
                    # 更新刮削状态为处理中
                    self._update_scrape_status(scrape_path, "processing")
                    
                    # 发送刮削请求
                    self.mediaChain.scrape_metadata(
                        fileitem=transferinfo.target_diritem,
                        meta=file_meta,
                        mediainfo=mediainfo,
                    )
                    
                    # 发送刮削请求后立即标记为完成
                    self._update_scrape_status(scrape_path, "completed")
                    logger.debug(
                        f"刮削目录成功: {scrape_path}"
                    )
                    break  # 成功后跳出循环
                except Exception as e:
                    error_msg = f"目录第 {retry_count}/{max_retries} 次刮削失败: {scrape_path} ,错误信息: {e}"
                    logger.warning(error_msg)
                    # 即使失败也标记为完成（因为已经发送了刮削请求）
                    if retry_count == max_retries:
                        self._update_scrape_status(
                            scrape_path, "completed", error_message=error_msg
                        )
                    time.sleep(3)
                    retry_count += 1
                    continue  # 重试
                    
    def _batch_refresh_media(self, items):
        """
        批量处理媒体库刷新
        :param items: 待刷新的项目列表
        """
        for transferinfo, mediainfo, file_meta in items:
            # 检查是否需要停止
            if self._check_stop_flag():
                logger.info("检测到停止标志，停止媒体库刷新")
                break
            
            try:
                self.eventmanager.send_event(
                    EventType.TransferComplete,
                    {
                        "meta": file_meta,
                        "mediainfo": mediainfo,
                        "transferinfo": transferinfo,
                    },
                )
                logger.info(
                    f"成功通知媒体库刷新: {transferinfo.target_diritem.path}"
                )
            except Exception as e:
                logger.error(
                    f"通知媒体库刷新失败: {transferinfo.target_diritem.path} ,错误信息: {e}"
                )
                
    def _batch_refresh_media_modified(self, items):
        """
        批量处理媒体库刷新（修改版）
        :param items: 待刷新的项目列表
        """
        for transferinfo, mediainfo, file_meta in items:
            # 检查是否需要停止
            if self._check_stop_flag():
                logger.info("检测到停止标志，停止媒体库刷新")
                break
            
            try:
                self._refresh_lib_modified(transferinfo, mediainfo)
                logger.info(
                    f"成功通知媒体库刷新: {transferinfo.target_diritem.path}"
                )
            except Exception as e:
                logger.error(
                    f"通知媒体库刷新失败: {transferinfo.target_diritem.path} ,错误信息: {e}"
                )

    def _refresh_lib_modified(self, transferinfo, mediainfo):
        """
        发送通知消息
        """
        # if not self._enabled:
        #     return

        # event_info: dict = event.event_data
        # if not event_info:
        #     return

        # 刷新媒体库
        if not self.service_infos:
            return

        if self._delay:
            logger.info(f"延迟 {self._delay} 秒后刷新媒体库... ")
            time.sleep(float(self._delay))

        # 入库数据
        # transferinfo: TransferInfo = event_info.get("transferinfo")
        if (
            not transferinfo
            or not transferinfo.target_diritem
            or not transferinfo.target_diritem.path
        ):
            return

        # mediainfo: MediaInfo = event_info.get("mediainfo")
        items = [
            RefreshMediaItem(
                title=mediainfo.title,
                year=mediainfo.year,
                type=mediainfo.type,
                category=mediainfo.category,
                target_path=Path(transferinfo.target_diritem.path),
            )
        ]

        for name, service in self.service_infos.items():
            if service.type == "plex":
                from app.db import ScopedSession
                from sqlalchemy import Column, Integer, String, Text
                from sqlalchemy.ext.declarative import declarative_base
                import json

                Base = declarative_base()

                class SystemConfig(Base):
                    __tablename__ = "systemconfig"

                    id = Column(Integer, primary_key=True, autoincrement=True)
                    key = Column(String(255), nullable=False)
                    value = Column(Text, nullable=True)

                try:
                    db = ScopedSession()
                    # 查询 key = "MediaServers" 的记录
                    record = (
                        db.query(SystemConfig)
                        .filter(SystemConfig.key == "MediaServers")
                        .first()
                    )
                finally:
                    db.close()

                media_conf = None
                if record:
                    try:
                        media_servers = json.loads(record.value)
                        for item in media_servers:
                            if (
                                item["name"] == service.name
                                and item["type"] == service.type
                            ):
                                media_conf = item
                                if not media_conf:
                                    logger.error(
                                        f"请检查媒体服务器 {service.name} 的配置！"
                                    )
                                    return
                                break
                    except Exception as e:
                        logger.error("JSON 解析失败:", e)

                from .plex.plex import Plex as class_plex

                if hasattr(class_plex, "refresh_library_by_items_modified"):

                    plex_instance = class_plex(
                        host=media_conf["config"]["host"],
                        token=media_conf["config"]["token"],
                        play_host=media_conf["config"]["play_host"],
                        sync_libraries=media_conf["sync_libraries"],
                    )

                    plex_instance.refresh_library_by_items_modified(items)
                    # service.instance.refresh_library_by_items(items)
                elif hasattr(service.instance, "refresh_root_library"):
                    # FIXME Jellyfin未找到刷新单个项目的API
                    service.instance.refresh_root_library()
                else:
                    logger.warning(f"{name} 不支持刷新")
            else:  # 如果不是plex就按照原来的的刷新流程
                if hasattr(service.instance, "refresh_library_by_items"):
                    service.instance.refresh_library_by_items(items)
                elif hasattr(service.instance, "refresh_root_library"):
                    # FIXME Jellyfin未找到刷新单个项目的API
                    service.instance.refresh_root_library()
                else:
                    logger.warning(f"{name} 不支持刷新")

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
            from .res import language_mapping

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
        try:
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
        except Exception as e:
            logger.error(f"发送消息失败: {str(e)}, traceback={traceback.format_exc()}")

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
                    finally:
                        # 无论发送成功与否，都移出已处理的媒体项
                        if medis_title_year_season in self._medias:
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
                    "name": "autoTransfer定期整理文件",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self.main,
                    "kwargs": {},
                }
            ]
        return []
        
    def _handle_error(self, error_type: str, error_message: str, file_item=None, file_meta=None, transfer_type=None):
        """
        统一错误处理
        :param error_type: 错误类型
        :param error_message: 错误消息
        :param file_item: 文件项
        :param file_meta: 文件元数据
        :param transfer_type: 转移方式
        """
        logger.error(f"{error_type}: {error_message}")
        
        # 新增转移失败历史记录
        if file_item and file_meta and transfer_type:
            try:
                self.transferhis.add_fail(
                    fileitem=file_item, mode=transfer_type, meta=file_meta
                )
            except Exception as e:
                logger.error(f"添加转移失败历史记录失败: {str(e)}")
        
        # 发送通知
        if self._notify and file_item:
            try:
                self.post_message(
                    mtype=NotificationType.Manual,
                    title=f"{file_item.path} {error_type}，无法入库！\n",
                    text=f"错误信息: {error_message}"
                )
            except Exception as e:
                logger.error(f"发送通知失败: {str(e)}")
        
        # 转移失败文件到指定目录
        if (
            file_item and self._pathAfterMoveFailure is not None
            and self._transfer_type == "move"
            and self._move_failed_files
        ):
            try:
                self.moveFailedFilesToPath(error_type, file_item.path)
            except Exception as e:
                logger.error(f"转移失败文件失败: {str(e)}")

    def _check_file_history(self, event_path: str, file_path: Path) -> bool:
        """
        检查文件历史记录
        :param event_path: 事件文件路径
        :param file_path: 文件路径
        :return: 是否继续处理
        """
        # 检查文件是否已处理过
        transfer_history = self.transferhis.get_by_src(event_path)
        if transfer_history:
            logger.info(f"文件已处理过: {event_path}")
            return False
        return True

    def _check_file_filter(self, event_path: str, file_path: Path) -> bool:
        """
        检查文件过滤
        :param event_path: 事件文件路径
        :param file_path: 文件路径
        :return: 是否继续处理
        """
        # 回收站及隐藏的文件不处理
        if (
            event_path.find("/@Recycle/") != -1
            or event_path.find("/#recycle/") != -1
            or event_path.find("/.") != -1
            or event_path.find("/@eaDir") != -1
        ):
            logger.debug(f"{event_path} 是回收站或隐藏的文件")
            return False

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
                    return False

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
                    return False

        # 不是媒体文件不处理
        if file_path.suffix.lower() not in [
            ext.lower() for ext in settings.RMT_MEDIAEXT
        ]:
            logger.debug(f"{event_path} 不是媒体文件")
            return False

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
                return False
        return True

    def _check_file_size(self, file_path: Path) -> bool:
        """
        检查文件大小
        :param file_path: 文件路径
        :return: 是否继续处理
        """
        if (
            self._size
            and float(self._size) > 0
            and file_path.stat().st_size < float(self._size) * 1024**3
        ):
            logger.info(f"{file_path} 文件大小小于监控文件大小，不处理")
            return False
        return True

    def _get_file_meta(self, file_path: Path, mon_path: str) -> Optional[MetaInfoPath]:
        """
        获取文件元数据
        :param file_path: 文件路径
        :param mon_path: 监控目录
        :return: 文件元数据
        """
        # 元数据
        file_meta = MetaInfoPath(file_path)
        if not file_meta.name:
            logger.error(f"{file_path.name} 无法识别有效信息")
            return None
            
        # 通过文件路径从历史下载记录中获取tmdbid和type
        # 先通过文件路径来查
        get_by_path_result = None
        
        # 构建查询路径列表
        query_paths = [str(file_path)]
        
        # 添加父目录到查询路径列表，最多3级
        if str(file_path.parent) != mon_path:
            parent_path = str(file_path.parent)
            for _ in range(3):
                if parent_path == mon_path:
                    break
                query_paths.append(parent_path)
                parent_path = str(Path(parent_path).parent)
        
        # 批量查询下载历史记录
        for path in query_paths:
            get_by_path_result = self.downloadhis.get_by_path(path)
            if get_by_path_result:
                logger.info(
                    f"通过路径 {path} 从历史下载记录中获取到tmdbid={get_by_path_result.tmdbid}，type={get_by_path_result.type}"
                )
                file_meta = self.__update_file_meta(
                    file_path=str(file_path),
                    file_meta=file_meta,
                    get_by_path_result=get_by_path_result,
                )
                break
        
        if not get_by_path_result:
            logger.info(
                f"未从历史下载记录中获取到 {str(file_path)} 的tmdbid和type，只能走正常识别流程"
            )
        
        return file_meta

    def _get_transfer_config(self, mon_path: str) -> Tuple[Optional[Path], Optional[str]]:
        """
        查询转移配置
        :param mon_path: 监控目录
        :return: (目标目录, 转移方式)
        """
        # 查询转移目的目录
        target: Path = self._dirconf.get(mon_path)
        # 查询转移方式
        transfer_type = self._transferconf.get(mon_path)
        return target, transfer_type

    def _get_file_item(self, file_path: Path) -> Optional[Any]:
        """
        获取文件项
        :param file_path: 文件路径
        :return: 文件项
        """
        try:
            # 查找这个文件项
            file_item = self.storagechain.get_file_item(
                storage="local", path=file_path
            )
            if not file_item:
                logger.warn(f"{file_path.name} 未找到对应的文件")
                return None
            return file_item
        except Exception as e:
            logger.error(f"获取文件项失败: {str(e)}, traceback={traceback.format_exc()}")
            return None

    def _get_media_info(self, file_item: Any, file_meta: MetaInfoPath, transfer_type: str) -> Optional[MediaInfo]:
        """
        识别媒体信息
        :param file_item: 文件项
        :param file_meta: 文件元数据
        :param transfer_type: 转移方式
        :return: 媒体信息
        """
        # 识别媒体信息
        mediainfo: MediaInfo = self.chain.recognize_media(meta=file_meta)
        if not mediainfo:
            logger.warn(f"未识别到媒体信息，路径: {file_item.path}")
            # 新增转移失败历史记录
            self.transferhis.add_fail(
                fileitem=file_item, mode=transfer_type, meta=file_meta
            )
            if self._notify:
                self.post_message(
                    mtype=NotificationType.Manual,
                    title=f"{file_item.path} 未识别到媒体信息，无法入库！\n",
                )
            # 转移失败文件到指定目录
            if (
                self._pathAfterMoveFailure is not None
                and self._transfer_type == "move"
                and self._move_failed_files
            ):
                self.moveFailedFilesToPath("未识别到媒体信息", file_item.path)
            return None
            
        # 如果未开启新增已入库媒体是否跟随TMDB信息变化则根据tmdbid查询之前的title
        if not settings.SCRAP_FOLLOW_TMDB:
            transfer_history = self.transferhis.get_by_type_tmdbid(
                tmdbid=mediainfo.tmdb_id, mtype=mediainfo.type.value
            )
            if transfer_history:
                mediainfo.title = transfer_history.title
        logger.info(
            f"{file_item.path} 识别为: {mediainfo.type.value} {mediainfo.title_year}"
        )
        return mediainfo

    def _get_episodes_info(self, mediainfo: MediaInfo, file_meta: MetaInfoPath) -> Optional[Any]:
        """
        获取剧集信息
        :param mediainfo: 媒体信息
        :param file_meta: 文件元数据
        :return: 剧集信息
        """
        if mediainfo.type == MediaType.TV:
            return self.tmdbchain.tmdb_episodes(
                tmdbid=mediainfo.tmdb_id,
                season=(
                    1
                    if file_meta.begin_season is None
                    else file_meta.begin_season
                ),
            )
        return None

    def _get_target_dir(self, mediainfo: MediaInfo, mon_path: str, target: Path, transfer_type: str) -> Optional[TransferDirectoryConf]:
        """
        获取目标目录
        :param mediainfo: 媒体信息
        :param mon_path: 监控目录
        :param target: 目标目录
        :param transfer_type: 转移方式
        :return: 目标目录配置
        """
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
            return None
        return target_dir

    def _handle_downloader_speed_limit(self, file_item: Any, target_dir: TransferDirectoryConf):
        """
        处理下载器限速
        :param file_item: 文件项
        :param target_dir: 目标目录配置
        """
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
                # 应用下载器限速
                log_message = f"下载器限速 - {', '.join(self._downloaders)}，下载速度限制为 {self._downloaderSpeedLimit} KiB/s，因正在移动或复制文件{file_item.path}"
                self._apply_downloader_speed_limit(log_message)
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
            logger.info("下载器限速未开启")

    def _transfer_file(self, file_item: Any, file_meta: MetaInfoPath, mediainfo: MediaInfo, target_dir: TransferDirectoryConf, episodes_info: Optional[Any]) -> Optional[TransferInfo]:
        """
        转移文件
        :param file_item: 文件项
        :param file_meta: 文件元数据
        :param mediainfo: 媒体信息
        :param target_dir: 目标目录配置
        :param episodes_info: 剧集信息
        :return: 转移信息
        """
        try:
            # 转移文件
            transferinfo: TransferInfo = self.chain.transfer(
                fileitem=file_item,
                meta=file_meta,
                mediainfo=mediainfo,
                target_directory=target_dir,
                episodes_info=episodes_info,
            )
            return transferinfo
        except Exception as e:
            logger.error(f"文件转移失败: {str(e)}, traceback={traceback.format_exc()}")
            return None

    def _recover_downloader_speed_limit(self):
        """
        恢复下载器限速
        """
        if self._downloaders_limit_enabled and self.get_data(
            key="is_download_speed_limited"
        ):
            try:
                download_limit_current_val = self.get_data(key="download_limit_current_val")
                recover_download_limit_success = self.set_download_limit(
                    download_limit_current_val
                )
                if recover_download_limit_success:
                    logger.info("取消下载器限速成功")
                    # 更新数据库中的限速状态为False
                    self.save_data(key="is_download_speed_limited", value=False)
                else:
                    logger.error("取消下载器限速失败")
            except Exception as e:
                logger.error(f"恢复下载器限速失败: {str(e)}")

    def _handle_transfer_failure(self, file_item: Any, file_path: Path, transfer_type: str, file_meta: MetaInfoPath, mediainfo: MediaInfo, transferinfo: TransferInfo):
        """
        处理转移失败
        :param file_item: 文件项
        :param file_path: 文件路径
        :param transfer_type: 转移方式
        :param file_meta: 文件元数据
        :param mediainfo: 媒体信息
        :param transferinfo: 转移信息
        """
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

    def _add_to_media_list(self, mediainfo: MediaInfo, file_meta: MetaInfoPath, file_path: Path, transferinfo: TransferInfo):
        """
        添加到媒体列表
        :param mediainfo: 媒体信息
        :param file_meta: 文件元数据
        :param file_path: 文件路径
        :param transferinfo: 转移信息
        """
        media_key = mediainfo.title_year + " " + file_meta.season
        media_list = self._medias.get(media_key) or {}
        
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
        self._medias[media_key] = media_list

    def _delete_empty_dirs(self, file_path: Path, mon_path: str):
        """
        删除空目录
        :param file_path: 文件路径
        :param mon_path: 监控目录
        """
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

    def _handle_transfer_success(self, file_item: Any, file_path: Path, transfer_type: str, file_meta: MetaInfoPath, mediainfo: MediaInfo, transferinfo: TransferInfo, mon_path: str):
        """
        处理转移成功
        :param file_item: 文件项
        :param file_path: 文件路径
        :param transfer_type: 转移方式
        :param file_meta: 文件元数据
        :param mediainfo: 媒体信息
        :param transferinfo: 转移信息
        :param mon_path: 监控目录
        """
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
            self._add_to_media_list(mediainfo, file_meta, file_path, transferinfo)

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
            self._delete_empty_dirs(file_path, mon_path)

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
                # 检查文件历史
                if not self._check_file_history(event_path, file_path):
                    return
                    
                # 检查文件过滤
                if not self._check_file_filter(event_path, file_path):
                    return
                    
                # 检查文件大小
                if not self._check_file_size(file_path):
                    return
                    
                # 获取文件元数据
                file_meta = self._get_file_meta(file_path, mon_path)
                if not file_meta:
                    return
                    
                # 查询转移配置
                target, transfer_type = self._get_transfer_config(mon_path)
                if not target:
                    return
                    
                # 获取文件项
                file_item = self._get_file_item(file_path)
                if not file_item:
                    return
                    
                # 识别媒体信息
                mediainfo = self._get_media_info(file_item, file_meta, transfer_type)
                if not mediainfo:
                    return
                    
                # 获取剧集信息
                episodes_info = self._get_episodes_info(mediainfo, file_meta)
                
                # 获取目标目录
                target_dir = self._get_target_dir(mediainfo, mon_path, target, transfer_type)
                if not target_dir:
                    return
                    
                # 处理下载器限速
                self._handle_downloader_speed_limit(file_item, target_dir)
                
                # 转移文件
                transferinfo = self._transfer_file(file_item, file_meta, mediainfo, target_dir, episodes_info)
                if not transferinfo:
                    logger.error("文件转移模块运行失败")
                    return
                    
                # 恢复下载器限速
                self._recover_downloader_speed_limit()
                
                # 处理转移失败
                if not transferinfo.success:
                    self._handle_transfer_failure(file_item, file_path, transfer_type, file_meta, mediainfo, transferinfo)
                    return
                    
                # 处理转移成功
                self._handle_transfer_success(file_item, file_path, transfer_type, file_meta, mediainfo, transferinfo, mon_path)
                
                # 返回成功的文件
                return transferinfo, mediainfo, file_meta

        except Exception as e:
            logger.error(f"目录监控发生错误: {str(e)} - {traceback.format_exc()}")
            return

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
            # 获取进度信息
            progress_data = self.get_data(key="transfer_progress")
            if progress_data:
                if progress_data.get("status") == "processing_dir":
                    status_label += f"\n正在处理目录({progress_data.get('dir_idx')}/{progress_data.get('total_dirs')}): {progress_data.get('current_dir')}"
                elif progress_data.get("status") == "processing_file":
                    status_label += f"\n正在处理目录({progress_data.get('dir_idx')}/{progress_data.get('total_dirs')}): {progress_data.get('current_dir')}"
                    file_size = progress_data.get('file_size', 0) / 2**30
                    status_label += f"\n正在处理文件({progress_data.get('file_idx')}/{progress_data.get('total_files')}) ({file_size:.2f} GiB): {progress_data.get('current_file')}"
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
            # 计算实际运行时间
            if plugin_state_time:
                try:
                    start_time = datetime.datetime.strptime(plugin_state_time, "%Y-%m-%d %H:%M:%S")
                    run_minutes = (datetime.datetime.now() - start_time).total_seconds() / 60
                    status_label = f"还没跑完，已连续运行时间 {run_minutes:.0f} 分钟"
                except:
                    status_label = "还没跑完，已连续运行时间长于30分钟"
            else:
                status_label = "还没跑完，已连续运行时间长于30分钟"
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
                                                    "hint": "整理完成后发送通知，推荐开",
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
                                                    "model": "EmptyPlaceholder",
                                                    "label": "EmptyPlaceholder",
                                                    "hint": "EmptyPlaceholder",
                                                    "persistent-hint": True,
                                                    "style": "visibility: hidden",
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
                                                    "hint": "手动清空上次运行状态，一般用不到，是调试插件时，直接停止主函数导致本插件运行状态没有更新才用的，推荐关",
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
                                                            "hint": "开启后会将整理记录储存到'媒体整理'，推荐开",
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
                                                            "hint": "每处理完一行监控目录，就刮削一次对应的图片和nfo文件，推荐开",
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
                                                            "hint": "开与关的区别就是'媒体库'-'电视剧'-'国产剧'-'甄嬛传'和'媒体库'-'电视剧'-'甄嬛传'，推荐开",
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
                                                            "model": "stop_transfer",
                                                            "label": "停止本次运行",
                                                            "hint": "停止当前正在运行的整理任务，当前文件/刮削会继续完成，下一个文件/刮削会留到下次运行时再处理",
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
                                                            "hint": "移动完成后删除空目录，推荐关闭，此开关仅在转移方式为移动时有效，推荐关",
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
                                                            "hint": "不论插件是否启用都立即运行一次(即手动整理一次)",
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
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "refresh",
                                            "label": "刷新媒体库",
                                            "hint": "广播整理完成事件，让插件'媒体库服务器刷新'通知媒体库刷新，推荐开",
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
                                            "model": "refresh_modified",
                                            "label": "刷新媒体库修改版",
                                            "hint": "修改plex刷新的路径，不广播完成事件了，直接把广播后的代码搬过来微调了一下",
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
                                        "component": "VSelect",
                                        "props": {
                                            "multiple": True,
                                            "chips": True,
                                            "clearable": True,
                                            "model": "mediaservers",
                                            "label": "媒体服务器",
                                            "hint": "刷新媒体库修改版使用的媒体服务器",
                                            "persistent-hint": True,
                                            "items": [
                                                {
                                                    "title": config.name,
                                                    "value": config.name,
                                                }
                                                for config in self.mediaserver_helper.get_configs().values()
                                            ],
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "delay",
                                            "label": "延迟时间（秒）",
                                            "placeholder": "10",
                                            "hint": "延迟特定秒后刷新媒体库，默认10",
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
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "text",
                                            "text": "两个开关二选一即可。'媒体服务器'和'延迟时间'的设置只和'刷新媒体库修改版'有关。两个开关区别：比如原版刷新媒体库的逻辑是入库了 '/Library/电视剧/欧美剧/黑镜 (2011)' 以后就让 plex 扫描 '/Library/电视剧/欧美剧/'，而修改版则是让 plex 扫描 '/Library/电视剧/欧美剧/黑镜 (2011)/'，从扫描欧美剧下的所有文件夹变为只扫描黑镜，大幅减少工作量。如果媒体服务器不是plex，不管选哪个都是走原来的逻辑",
                                            "density": "compact",
                                            "style": "font-size: 13px; color: #666;",
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
                                            "hint": "开启后，在移动或复制文件时会限制qb下载速度，完成后恢复原(限)速，默认关闭",
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
                                                    "rows": 10,
                                                    "auto-grow": True,
                                                    "placeholder": "转移方式支持 move、copy、link、softlink、rclone_copy、rclone_move\n"
                                                    "覆盖方式支持: always(总是覆盖同名文件)、size(存在时大覆盖小)、never(存在不覆盖)、latest(仅保留最新版本)\n"
                                                    "一行一个目录，支持以下几种配置方式:\n"
                                                    "监控目录:目的目录\n"
                                                    "监控目录:目的目录#转移方式\n"
                                                    "监控目录:目的目录#转移方式@覆盖方式\n"
                                                    "例如:\n"
                                                    "/Downloads/电影/:/Library/电影/\n"
                                                    "/Downloads/电视剧/:/Library/电视剧/#copy\n"
                                                    "/mnt/手动备份/电影/:/Library/手动备份/电影/#move@always",
                                                    "hint": "①转移方式支持 move、copy、link、softlink、rclone_copy、rclone_move。"
                                                    "②覆盖方式支持: always(总是覆盖同名文件)、size(存在时大覆盖小)、never(存在不覆盖)、latest(仅保留最新版本)。"
                                                    "③例: /mnt/手动备份/电影/:/Library/手动备份/电影/#move@always   其中#move和@always可省略，通过插件上方统一配置。"
                                                    "④如果'监控目录'中的视频在'设定'-'储存&目录'中的'资源目录中'或其子目录中，则插件这边的对应设置无效，会优先使用'设定'中的配置。",
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
                                            "auto-grow": True,
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
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "pathAfterMoveFailure",
                                            "label": "移动到的路径",
                                            "rows": 1,
                                            "placeholder": "如 /mnt/failed",
                                            "hint": "移动方式，当整理失败或命中关键词后，将文件移动到此路径(会根据失败原因和原目录结构将文件移动到此处)，只能有一个路径，留空或'转移方式'不是'移动'或不满足上面两个开关的条件均不会移动。",
                                            "persistent-hint": True,
                                            "auto-grow": True,
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
                                            "text": "1.入库消息延迟默认300s，如网络较慢可酌情调大，有助于发送统一入库消息。\n2.源目录与目的目录设置一致，则默认使用目录设置配置。否则可在源目录后拼接@覆盖方式（默认never覆盖方式）。\n3.开启软链接/Strm会在监控转移后联动【实时软链接】/【云盘Strm[助手]】插件生成软链接/Strm（只处理媒体文件，不处理刮削文件）。\n4.启用此插件后，可将'设定'-'存储&目录'-'目录'-'自动整理'改为'不整理'或'手动整理'\n5.'转移时下载器限速'只在移动(或复制)时生效，他会在每次移动(或复制)前，限制qb下载速度，转移完成后再恢复限速前的速度\n6.'是否二级分类'与'设定'-'储存&目录'-'媒体库目录'-'按类别分类'开关冲突时，以'设定'中的为准\n\n此插件由thsrite的目录监控插件修改而得\n本意是为了做类似v1的定时整理，因我只用本地移动，故也不知软/硬链、Strm之类的是否可用",
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
            "stop_transfer": False,
            "history": False,
            "scrape": False,
            "category": False,
            "refresh": False,
            "refresh_modified": False,
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
            "downloaders": "",
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
        
        # 清理文件锁
        self._release_lock()

    def stop_transfer(self):
        """
        手动停止文件整理
        """
        self._set_stop_flag()
        logger.info("已发送停止指令，将在当前文件/刮削完成后停止")
        return {"success": True, "message": "停止指令已发送，将在当前文件/刮削完成后停止"}
