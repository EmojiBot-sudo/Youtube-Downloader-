import os
import re
import sys
import time
import shutil
import threading
import requests
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

from PySide6.QtCore import Qt, QThread, Signal, QSettings, QTimer
from PySide6.QtGui import QPixmap
from PySide6.QtWidgets import (
    QApplication, QWidget, QTabWidget,
    QVBoxLayout, QHBoxLayout, QGridLayout, QFormLayout,
    QLabel, QLineEdit, QPushButton, QFileDialog,
    QProgressBar, QMessageBox, QComboBox, QGroupBox, QSpinBox,
    QCheckBox, QSystemTrayIcon, QStyle, QDialog, QTextEdit
)

import yt_dlp


# -------------------------
# Helpers
# -------------------------

def sanitize_filename(name: str) -> str:
    name = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name[:180] if len(name) > 180 else name


def bytes_to_human(n: int | None) -> str:
    if not n or n <= 0:
        return "Unknown"
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(n)
    i = 0
    while size >= 1024 and i < len(units) - 1:
        size /= 1024
        i += 1
    return f"{size:.2f} {units[i]}"


def best_thumbnail_url(info: dict) -> str | None:
    thumbs = info.get("thumbnails") or []
    best = None
    best_area = -1
    for t in thumbs:
        url = t.get("url")
        w = t.get("width") or 0
        h = t.get("height") or 0
        area = w * h
        if url and area > best_area:
            best = url
            best_area = area
    return best or info.get("thumbnail")


def estimate_size_best_effort(info: dict) -> int | None:
    size = info.get("filesize") or info.get("filesize_approx")
    if size:
        return int(size)
    formats = info.get("formats") or []
    best = None
    for f in formats:
        fs = f.get("filesize") or f.get("filesize_approx")
        if not fs:
            continue
        fs = int(fs)
        if best is None or fs > best:
            best = fs
    return best


def entry_to_url(entry: dict) -> str | None:
    if not entry:
        return None
    if entry.get("webpage_url"):
        return entry["webpage_url"]
    u = entry.get("url")
    if not u:
        return None
    if u.startswith("http://") or u.startswith("https://"):
        return u
    if len(u) == 11:
        return f"https://www.youtube.com/watch?v={u}"
    return u


def build_quality_combo() -> QComboBox:
    combo = QComboBox()
    combo.addItem("Best (Windows-friendly, usually MP4/H.264)", "best_compat")  # safer default
    combo.addItem("Best (max quality – fast, may need VLC)", "best_fast")
    combo.addItem("Best (max quality – re-encode to MP4/H.264, slow)", "best_win")
    combo.addItem("Up to 1080p (fast)", "1080")
    combo.addItem("Up to 720p (fast)", "720")
    combo.addItem("Up to 480p (fast)", "480")
    return combo


# -------------------------
# Theme (Modern UI)
# -------------------------

def modern_stylesheet(theme: str) -> str:
    if theme == "dark":
        BG = "#0b0f16"
        SURFACE = "#111827"
        SURFACE2 = "#0f172a"
        BORDER = "#243041"
        TEXT = "#e5e7eb"
        MUTED = "#9ca3af"
        INPUT = "#0b1220"
        INPUT_BORDER = "#2b3a52"
        ACCENT = "#7c3aed"
        ACCENT_HOVER = "#6d28d9"
        DANGER = "#ef4444"
        TAB_BG = "#0d1423"
        GOOD = "#22c55e"
        WARN = "#f59e0b"
    else:
        BG = "#f6f7fb"
        SURFACE = "#ffffff"
        SURFACE2 = "#fbfbfe"
        BORDER = "#e5e7eb"
        TEXT = "#111827"
        MUTED = "#6b7280"
        INPUT = "#ffffff"
        INPUT_BORDER = "#d1d5db"
        ACCENT = "#7c3aed"
        ACCENT_HOVER = "#6d28d9"
        DANGER = "#dc2626"
        TAB_BG = "#ffffff"
        GOOD = "#16a34a"
        WARN = "#d97706"

    return f"""
    * {{
        font-family: "Segoe UI", "Inter", "Arial";
        font-size: 13px;
        color: {TEXT};
    }}
    QWidget {{ background: {BG}; }}

    QLabel#HeaderTitle {{ font-size: 22px; font-weight: 800; }}
    QLabel#HeaderSub {{ color: {MUTED}; font-size: 12px; }}
    QLabel#Hint {{ color: {MUTED}; font-size: 12px; }}
    QLabel#Good {{ color: {GOOD}; font-weight: 700; }}
    QLabel#Warn {{ color: {WARN}; font-weight: 700; }}

    QTabWidget::pane {{
        border: 1px solid {BORDER};
        border-radius: 16px;
        background: {SURFACE};
        top: -1px;
    }}
    QTabBar::tab {{
        background: {TAB_BG};
        border: 1px solid {BORDER};
        padding: 10px 14px;
        margin-right: 6px;
        border-top-left-radius: 12px;
        border-top-right-radius: 12px;
        color: {MUTED};
        font-weight: 650;
        min-width: 110px;
    }}
    QTabBar::tab:selected {{
        background: {SURFACE};
        color: {TEXT};
        border-bottom-color: {SURFACE};
    }}
    QTabBar::tab:hover {{ color: {TEXT}; }}

    QGroupBox {{
        background: {SURFACE};
        border: 1px solid {BORDER};
        border-radius: 16px;
        margin-top: 12px;
        padding: 12px;
        font-weight: 700;
    }}
    QGroupBox::title {{
        subcontrol-origin: margin;
        left: 14px;
        padding: 0 6px;
        color: {TEXT};
    }}

    QLineEdit, QComboBox, QSpinBox {{
        background: {INPUT};
        border: 1px solid {INPUT_BORDER};
        border-radius: 12px;
        padding: 10px 12px;
        selection-background-color: {ACCENT};
    }}
    QLineEdit:focus, QComboBox:focus, QSpinBox:focus {{
        border: 1px solid {ACCENT};
    }}

    QPushButton {{
        background: {SURFACE2};
        border: 1px solid {BORDER};
        border-radius: 12px;
        padding: 10px 14px;
        font-weight: 700;
    }}
    QPushButton:hover {{ border-color: {ACCENT}; }}
    QPushButton:disabled {{
        color: {MUTED};
        border-color: {BORDER};
        background: {SURFACE};
    }}

    QPushButton#Primary {{
        background: {ACCENT};
        border: 1px solid {ACCENT};
        color: white;
    }}
    QPushButton#Primary:hover {{
        background: {ACCENT_HOVER};
        border-color: {ACCENT_HOVER};
    }}

    QPushButton#Danger {{
        background: transparent;
        border: 1px solid {DANGER};
        color: {DANGER};
    }}
    QPushButton#Danger:hover {{ background: rgba(239,68,68,0.10); }}

    QProgressBar {{
        border: 1px solid {BORDER};
        border-radius: 10px;
        background: {SURFACE};
        text-align: center;
        height: 18px;
        color: {MUTED};
        font-weight: 650;
    }}
    QProgressBar::chunk {{
        border-radius: 10px;
        background: {ACCENT};
    }}

    QLabel#Thumb {{
        background: {SURFACE2};
        border: 1px solid {BORDER};
        border-radius: 14px;
        color: {MUTED};
    }}

    QTextEdit {{
        background: {INPUT};
        border: 1px solid {INPUT_BORDER};
        border-radius: 12px;
        padding: 10px;
        font-family: Consolas, "Courier New", monospace;
        font-size: 12px;
    }}
    """


# -------------------------
# Persistent Settings
# -------------------------

class AppPrefs:
    ORG = "ColourBand"
    APP = "YTDownloaderReleaseReady"

    def __init__(self):
        self.qs = QSettings(self.ORG, self.APP)

    def get(self, key: str, default):
        v = self.qs.value(key, default)
        if isinstance(default, bool):
            if isinstance(v, str):
                return v.lower() in ("1", "true", "yes", "on")
            return bool(v)
        if isinstance(default, int):
            try:
                return int(v)
            except Exception:
                return default
        return v

    def set(self, key: str, value):
        self.qs.setValue(key, value)

    def theme(self) -> str:
        t = str(self.get("appearance/theme", "dark")).lower().strip()
        return t if t in ("light", "dark") else "dark"

    def default_video_folder(self) -> str:
        return self.get("folders/video", os.path.join(os.path.expanduser("~"), "Downloads"))

    def default_audio_folder(self) -> str:
        return self.get("folders/audio", os.path.join(os.path.expanduser("~"), "Downloads"))

    # Safe default: Windows-friendly MP4/avc1/m4a preference.
    def default_quality_mode(self) -> str:
        return self.get("video/quality_mode", "best_compat")

    def default_parallel_videos(self) -> int:
        return self.get("threads/parallel_videos", 2)

    def default_parallel_fragments(self) -> int:
        return self.get("threads/parallel_fragments", 4)

    def skip_existing(self) -> bool:
        return self.get("behavior/skip_existing", True)

    def notifications(self) -> bool:
        return self.get("behavior/notifications", True)

    def default_audio_format(self) -> str:
        return self.get("audio/format", "mp3")

    def default_audio_bitrate(self) -> int:
        return self.get("audio/mp3_bitrate", 192)

    def ffmpeg_path(self) -> str:
        return self.get("ffmpeg/path", "")

    def debug_logging(self) -> bool:
        return self.get("debug/enabled", True)  # keep ON for first release; helps bug reports


# -------------------------
# Logging (copyable)
# -------------------------

class LogBuffer:
    def __init__(self, max_lines: int = 2000):
        self.max_lines = max_lines
        self._lines: list[str] = []
        self._lock = threading.Lock()

    def add(self, line: str):
        line = line.rstrip("\n")
        if not line:
            return
        with self._lock:
            self._lines.append(line)
            if len(self._lines) > self.max_lines:
                self._lines = self._lines[-self.max_lines:]

    def dump(self) -> str:
        with self._lock:
            return "\n".join(self._lines)

    def clear(self):
        with self._lock:
            self._lines.clear()


class YDLLogger:
    """
    yt-dlp logger interface.
    """
    def __init__(self, buf: LogBuffer, emit_line=None):
        self.buf = buf
        self.emit_line = emit_line  # callable(str)

    def _log(self, prefix: str, msg: str):
        line = f"{prefix} {msg}".strip()
        self.buf.add(line)
        if self.emit_line:
            try:
                self.emit_line(line)
            except Exception:
                pass

    def debug(self, msg):
        self._log("[debug]", str(msg))

    def info(self, msg):
        self._log("[info]", str(msg))

    def warning(self, msg):
        self._log("[warn]", str(msg))

    def error(self, msg):
        self._log("[error]", str(msg))


class LogDialog(QDialog):
    def __init__(self, parent: QWidget, title: str, log_text: str):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.resize(840, 520)

        root = QVBoxLayout(self)
        root.setSpacing(10)

        self.text = QTextEdit()
        self.text.setReadOnly(True)
        self.text.setPlainText(log_text)
        root.addWidget(self.text, 1)

        row = QHBoxLayout()
        self.copy_btn = QPushButton("Copy Debug Log")
        self.close_btn = QPushButton("Close")
        row.addWidget(self.copy_btn)
        row.addStretch(1)
        row.addWidget(self.close_btn)
        root.addLayout(row)

        self.copy_btn.clicked.connect(self.copy)
        self.close_btn.clicked.connect(self.accept)

    def copy(self):
        QApplication.clipboard().setText(self.text.toPlainText())
        QMessageBox.information(self, "Copied", "Debug log copied to clipboard.")


# -------------------------
# FFmpeg detection
# -------------------------

def normalize_ffmpeg_location(path: str) -> str:
    """
    yt-dlp accepts either:
    - directory containing ffmpeg(.exe)
    - direct path to ffmpeg(.exe)
    """
    p = (path or "").strip().strip('"')
    if not p:
        return ""
    p = os.path.expanduser(p)
    p = os.path.abspath(p)
    return p


def ffmpeg_detected(ffmpeg_location: str) -> tuple[bool, str]:
    """
    Returns (ok, message). Message is a short human-readable status.
    """
    loc = normalize_ffmpeg_location(ffmpeg_location)

    # If user provided an explicit location
    if loc:
        if os.path.isdir(loc):
            exe = "ffmpeg.exe" if os.name == "nt" else "ffmpeg"
            probe = os.path.join(loc, exe)
            if os.path.exists(probe):
                return True, f"FFmpeg found in: {loc}"
            return False, f"Folder set, but {exe} not found in it."
        else:
            # file path
            if os.path.exists(loc) and os.path.isfile(loc):
                return True, f"FFmpeg found at: {loc}"
            return False, "FFmpeg path set, but file not found."

    # Otherwise: PATH
    which = shutil.which("ffmpeg")
    if which:
        return True, f"FFmpeg found in PATH: {which}"

    # Common Windows locations (best-effort)
    if os.name == "nt":
        guesses = [
            r"C:\ffmpeg\bin\ffmpeg.exe",
            r"C:\Program Files\ffmpeg\bin\ffmpeg.exe",
            r"C:\Program Files (x86)\ffmpeg\bin\ffmpeg.exe",
        ]
        for g in guesses:
            if os.path.exists(g):
                return True, f"FFmpeg found at: {g}"

    return False, "FFmpeg not detected (set it in Settings or add to PATH)."


# -------------------------
# Workers
# -------------------------

class InfoWorker(QThread):
    info_ready = Signal(dict)
    error = Signal(str)

    def __init__(self, url: str, allow_playlist: bool):
        super().__init__()
        self.url = url.strip()
        self.allow_playlist = allow_playlist

    def run(self):
        try:
            opts = {"quiet": True, "no_warnings": True}
            opts["noplaylist"] = not self.allow_playlist
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(self.url, download=False)
            self.info_ready.emit(info)
        except Exception as e:
            self.error.emit(str(e))


@dataclass
class DownloadOptions:
    url: str
    out_dir: str
    kind: str  # "single", "playlist", "channel"

    mode: str | None = None

    audio_only: bool = False
    audio_format: str = "mp3"
    audio_bitrate_kbps: int = 192

    start: int | None = None
    end: int | None = None

    parallel_videos: int = 1
    parallel_fragments: int = 1

    skip_existing: bool = True

    ffmpeg_location: str = ""
    debug_enabled: bool = True


class DownloadWorker(QThread):
    progress = Signal(int)
    status = Signal(str)
    busy = Signal(bool)        # True => postprocessing/merging/encoding spinner + timer in UI
    done = Signal(str)
    error = Signal(str)

    log_line = Signal(str)     # stream log lines to UI buffer if wanted

    def __init__(self, opts: DownloadOptions, logbuf: LogBuffer):
        super().__init__()
        self.opts = opts
        self.logbuf = logbuf
        self._cancel = threading.Event()

    def request_cancel(self):
        self._cancel.set()

    def _common_behavior_opts(self) -> dict:
        d = {
            "continuedl": True,
        }
        if self.opts.skip_existing:
            d["nooverwrites"] = True
        return d

    # -------- Video --------
    def _video_format_and_reencode(self, mode: str) -> tuple[str, bool]:
        if mode in ("best_fast", "best_win"):
            return "bv*+ba/best", (mode == "best_win")

        if mode == "best_compat":
            # Prefer MP4/H.264 + M4A so Windows default players behave better.
            return (
                "best[ext=mp4]/"
                "bv*[vcodec^=avc1][ext=mp4]+ba[acodec^=mp4a][ext=m4a]/"
                "bv*[vcodec^=avc1]+ba[acodec^=mp4a]/"
                "best",
                False
            )

        h = int(mode)
        return f"bv*[height<={h}]+ba/best[height<={h}]/best", False

    def _make_logger(self):
        if not self.opts.debug_enabled:
            return None
        return YDLLogger(self.logbuf, emit_line=lambda ln: self.log_line.emit(ln))

    def _ffmpeg_location_opt(self) -> dict:
        loc = normalize_ffmpeg_location(self.opts.ffmpeg_location)
        if loc:
            return {"ffmpeg_location": loc}
        return {}

    def _make_video_ydl_opts(self, outtmpl: str, noplaylist: bool) -> dict:
        if not self.opts.mode:
            raise RuntimeError("Pick a quality mode.")

        fmt, reencode = self._video_format_and_reencode(self.opts.mode)
        logger = self._make_logger()

        def hook(d):
            if self._cancel.is_set():
                raise yt_dlp.utils.DownloadError("Cancelled by user.")
            st = d.get("status")
            if st == "downloading":
                self.busy.emit(False)
                total = d.get("total_bytes") or d.get("total_bytes_estimate")
                downloaded = d.get("downloaded_bytes") or 0
                if total:
                    pct = int(downloaded * 100 / total)
                    self.progress.emit(max(0, min(100, pct)))
                self.status.emit("Downloading…")
            elif st == "finished":
                self.progress.emit(100)
                self.busy.emit(True)
                self.status.emit("Post-processing (FFmpeg)…")
            elif st == "postprocessing":
                self.busy.emit(True)
                self.status.emit("Encoding for Windows compatibility…" if reencode else "Merging/remuxing (FFmpeg)…")

        ydl_opts = {
            "format": fmt,
            "outtmpl": outtmpl,
            "quiet": True,
            "no_warnings": True,
            "progress_hooks": [hook],
            "merge_output_format": "mp4",
            "noplaylist": noplaylist,
            "concurrent_fragment_downloads": max(1, int(self.opts.parallel_fragments)),
            "logger": logger,
        }
        ydl_opts.update(self._common_behavior_opts())
        ydl_opts.update(self._ffmpeg_location_opt())

        if not reencode:
            ydl_opts["postprocessors"] = [{"key": "FFmpegVideoRemuxer", "preferedformat": "mp4"}]
        else:
            ydl_opts["postprocessors"] = [{"key": "FFmpegVideoConvertor", "preferedformat": "mp4"}]
            ydl_opts["postprocessor_args"] = [
                "-c:v", "libx264",
                "-preset", "veryfast",
                "-c:a", "aac",
                "-b:a", "192k",
            ]
        return ydl_opts

    # -------- Audio --------
    def _make_audio_ydl_opts(self, outtmpl: str, noplaylist: bool) -> dict:
        logger = self._make_logger()

        def hook(d):
            if self._cancel.is_set():
                raise yt_dlp.utils.DownloadError("Cancelled by user.")
            st = d.get("status")
            if st == "downloading":
                self.busy.emit(False)
                total = d.get("total_bytes") or d.get("total_bytes_estimate")
                downloaded = d.get("downloaded_bytes") or 0
                if total:
                    pct = int(downloaded * 100 / total)
                    self.progress.emit(max(0, min(100, pct)))
                self.status.emit("Downloading audio…")
            elif st == "finished":
                self.progress.emit(100)
                self.busy.emit(True)
                self.status.emit("Post-processing audio (FFmpeg)…")
            elif st == "postprocessing":
                self.busy.emit(True)
                self.status.emit("Converting/extracting audio (FFmpeg)…")

        ydl_opts = {
            "format": "bestaudio/best",
            "outtmpl": outtmpl,
            "quiet": True,
            "no_warnings": True,
            "progress_hooks": [hook],
            "noplaylist": noplaylist,
            "concurrent_fragment_downloads": max(1, int(self.opts.parallel_fragments)),
            "logger": logger,
        }
        ydl_opts.update(self._common_behavior_opts())
        ydl_opts.update(self._ffmpeg_location_opt())

        audio_fmt = (self.opts.audio_format or "mp3").lower().strip()
        if audio_fmt not in ("mp3", "m4a", "opus"):
            audio_fmt = "mp3"

        pp = {"key": "FFmpegExtractAudio", "preferredcodec": audio_fmt}
        if audio_fmt == "mp3":
            pp["preferredquality"] = str(max(64, min(320, int(self.opts.audio_bitrate_kbps))))
        ydl_opts["postprocessors"] = [pp]
        return ydl_opts

    # -------- Multi --------
    def _download_one(self, video_url: str, outtmpl: str):
        if self._cancel.is_set():
            raise yt_dlp.utils.DownloadError("Cancelled by user.")
        if self.opts.audio_only:
            ydl_opts = self._make_audio_ydl_opts(outtmpl=outtmpl, noplaylist=True)
        else:
            ydl_opts = self._make_video_ydl_opts(outtmpl=outtmpl, noplaylist=True)
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])

    def _extract_flat_entries(self, url: str) -> list[str]:
        extract_opts = {
            "quiet": True,
            "no_warnings": True,
            "extract_flat": "in_playlist",
            "skip_download": True,
            "noplaylist": False,
        }
        extract_opts.update(self._ffmpeg_location_opt())

        with yt_dlp.YoutubeDL(extract_opts) as ydl:
            info = ydl.extract_info(url, download=False)

        entries = info.get("entries") or []
        urls = []
        for e in entries:
            u = entry_to_url(e)
            if u:
                urls.append(u)
        return urls

    def run(self):
        try:
            self.logbuf.clear()
            self.logbuf.add(f"[info] yt-dlp version: {yt_dlp.version.__version__}")
            self.logbuf.add(f"[info] python: {sys.version}")
            self.logbuf.add(f"[info] platform: {sys.platform}")

            url = self.opts.url.strip()
            out_dir = self.opts.out_dir.strip()

            if not url:
                raise RuntimeError("Please paste a URL.")
            if not out_dir or not os.path.isdir(out_dir):
                raise RuntimeError("Please choose a valid output folder.")

            ok, msg = ffmpeg_detected(self.opts.ffmpeg_location)
            self.logbuf.add(f"[info] ffmpeg check: {msg}")
            if not ok:
                raise RuntimeError(
                    "FFmpeg not detected.\n\n"
                    "Go to Settings → FFmpeg and set the path, OR install FFmpeg and add it to PATH."
                )

            if self.opts.kind == "single":
                outtmpl = os.path.join(out_dir, "%(title)s.%(ext)s")
            elif self.opts.kind == "playlist":
                outtmpl = os.path.join(out_dir, "%(playlist_title)s", "%(playlist_index)03d - %(title)s.%(ext)s")
            else:
                outtmpl = os.path.join(out_dir, "%(uploader)s", "%(upload_date)s - %(title)s.%(ext)s")

            self.status.emit("Preparing…")
            self.progress.emit(0)
            self.busy.emit(False)

            # Single
            if self.opts.kind == "single":
                if self.opts.audio_only:
                    ydl_opts = self._make_audio_ydl_opts(outtmpl=outtmpl, noplaylist=True)
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        info = ydl.extract_info(url, download=False)
                        title = sanitize_filename(info.get("title") or "audio")
                        ext = (self.opts.audio_format or "mp3").lower()
                        final_guess = os.path.join(out_dir, f"{title}.{ext}")
                        self.status.emit(f"Starting: {title}")
                        ydl.download([url])
                else:
                    ydl_opts = self._make_video_ydl_opts(outtmpl=outtmpl, noplaylist=True)
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        info = ydl.extract_info(url, download=False)
                        title = sanitize_filename(info.get("title") or "video")
                        final_guess = os.path.join(out_dir, f"{title}.mp4")
                        self.status.emit(f"Starting: {title}")
                        ydl.download([url])

                self.busy.emit(False)
                self.status.emit("Done ✅")
                self.done.emit(final_guess)
                return

            # Playlist / channel
            self.status.emit("Fetching list…")
            video_urls = self._extract_flat_entries(url)

            start = self.opts.start or 1
            end = self.opts.end
            start_idx = max(0, start - 1)
            video_urls = video_urls[start_idx:(end if end else None)]
            total = len(video_urls)

            if total == 0:
                raise RuntimeError("No videos found (or list is private/restricted).")

            workers = min(max(1, int(self.opts.parallel_videos)), 8)
            completed = 0

            self.status.emit(f"Downloading {total} item(s) with {workers} threads…")
            self.progress.emit(0)

            def update_progress():
                self.progress.emit(int((completed / total) * 100))

            with ThreadPoolExecutor(max_workers=workers) as ex:
                futures = []
                for vurl in video_urls:
                    if self._cancel.is_set():
                        break
                    futures.append(ex.submit(self._download_one, vurl, outtmpl))

                # If cancelled, try to cancel pending futures
                if self._cancel.is_set():
                    for f in futures:
                        f.cancel()
                    raise yt_dlp.utils.DownloadError("Cancelled by user.")

                for fut in as_completed(futures):
                    if self._cancel.is_set():
                        for f in futures:
                            f.cancel()
                        raise yt_dlp.utils.DownloadError("Cancelled by user.")
                    fut.result()
                    completed += 1
                    update_progress()
                    self.status.emit(f"Completed {completed}/{total}")

            self.busy.emit(False)
            self.status.emit("Done ✅")
            self.done.emit(out_dir)

        except Exception as e:
            self.busy.emit(False)
            self.error.emit(str(e))


# -------------------------
# UI Tabs
# -------------------------

class SingleTab(QWidget):
    def __init__(self, pick_folder_cb):
        super().__init__()
        self.pick_folder_cb = pick_folder_cb
        self.info_worker = None

        root = QVBoxLayout(self)
        root.setContentsMargins(18, 18, 18, 18)
        root.setSpacing(12)

        url_card = QGroupBox("Video URL")
        url_layout = QVBoxLayout(url_card)
        self.url = QLineEdit()
        self.url.setPlaceholderText("Paste or drag a YouTube video URL…")
        url_layout.addWidget(self.url)
        root.addWidget(url_card)

        preview = QGroupBox("Preview")
        grid = QGridLayout(preview)
        grid.setHorizontalSpacing(14)
        grid.setVerticalSpacing(10)

        self.thumb = QLabel("Thumbnail")
        self.thumb.setObjectName("Thumb")
        self.thumb.setAlignment(Qt.AlignCenter)
        self.thumb.setFixedSize(300, 170)

        self.title_lbl = QLabel("Title: —")
        self.title_lbl.setWordWrap(True)
        self.size_lbl = QLabel("Estimated size: —")
        self.size_lbl.setObjectName("Hint")

        grid.addWidget(self.thumb, 0, 0, 3, 1)
        grid.addWidget(self.title_lbl, 0, 1)
        grid.addWidget(self.size_lbl, 1, 1)
        grid.setColumnStretch(1, 1)
        root.addWidget(preview)

        opts = QGroupBox("Options")
        form = QFormLayout(opts)
        self.quality = build_quality_combo()
        self.frags = QSpinBox()
        self.frags.setRange(1, 16)
        self.frags.setValue(4)

        out_row = QWidget()
        out_h = QHBoxLayout(out_row)
        out_h.setContentsMargins(0, 0, 0, 0)
        self.out = QLineEdit()
        self.out.setPlaceholderText("Output folder…")
        browse = QPushButton("Browse")
        browse.clicked.connect(lambda: self.out.setText(self.pick_folder_cb()))
        out_h.addWidget(self.out, 1)
        out_h.addWidget(browse)

        form.addRow("Quality mode:", self.quality)
        form.addRow("Parallel fragments:", self.frags)
        form.addRow("Save to:", out_row)
        root.addWidget(opts)

        root.addStretch(1)

    def load_preview(self, url: str):
        url = url.strip()
        if not url:
            return

        self.title_lbl.setText("Title: Loading…")
        self.size_lbl.setText("Estimated size: Loading…")
        self.thumb.setText("Loading…")
        self.thumb.setPixmap(QPixmap())

        if self.info_worker and self.info_worker.isRunning():
            self.info_worker.quit()
            self.info_worker.wait(200)

        self.info_worker = InfoWorker(url, allow_playlist=False)
        self.info_worker.info_ready.connect(self._show_info)
        self.info_worker.error.connect(self._show_err)
        self.info_worker.start()

    def _show_err(self, _msg: str):
        self.title_lbl.setText("Title: —")
        self.size_lbl.setText("Estimated size: —")
        self.thumb.setText("No preview")

    def _show_info(self, info: dict):
        self.title_lbl.setText(f"Title: {info.get('title') or '—'}")
        est = estimate_size_best_effort(info)
        self.size_lbl.setText(f"Estimated size: {bytes_to_human(est)}")

        turl = best_thumbnail_url(info)
        if turl:
            try:
                r = requests.get(turl, timeout=10)
                r.raise_for_status()
                pix = QPixmap()
                pix.loadFromData(r.content)
                if not pix.isNull():
                    self.thumb.setPixmap(pix.scaled(self.thumb.size(), Qt.KeepAspectRatio, Qt.SmoothTransformation))
                else:
                    self.thumb.setText("Thumbnail unavailable")
            except Exception:
                self.thumb.setText("Thumbnail unavailable")
        else:
            self.thumb.setText("Thumbnail unavailable")


class ListTab(QWidget):
    def __init__(self, kind: str, pick_folder_cb):
        super().__init__()
        self.kind = kind
        self.pick_folder_cb = pick_folder_cb
        self.info_worker = None

        root = QVBoxLayout(self)
        root.setContentsMargins(18, 18, 18, 18)
        root.setSpacing(12)

        label = "Playlist URL" if kind == "playlist" else "Channel URL"
        url_card = QGroupBox(label)
        url_layout = QVBoxLayout(url_card)
        self.url = QLineEdit()
        self.url.setPlaceholderText("Paste or drag a URL…")
        url_layout.addWidget(self.url)
        root.addWidget(url_card)

        info = QGroupBox("Info")
        info_layout = QVBoxLayout(info)
        self.title_lbl = QLabel("Title: —")
        self.count_lbl = QLabel("Items: —")
        self.count_lbl.setObjectName("Hint")
        info_layout.addWidget(self.title_lbl)
        info_layout.addWidget(self.count_lbl)
        root.addWidget(info)

        opts = QGroupBox("Options")
        form = QFormLayout(opts)

        self.quality = build_quality_combo()
        self.threads = QSpinBox()
        self.threads.setRange(1, 8)
        self.threads.setValue(2)

        self.frags = QSpinBox()
        self.frags.setRange(1, 16)
        self.frags.setValue(4)

        range_row = QWidget()
        r = QHBoxLayout(range_row)
        r.setContentsMargins(0, 0, 0, 0)
        self.start = QSpinBox()
        self.start.setRange(1, 999999)
        self.start.setValue(1)
        self.end = QSpinBox()
        self.end.setRange(0, 999999)
        self.end.setValue(0)
        r.addWidget(QLabel("Start"))
        r.addWidget(self.start)
        r.addSpacing(10)
        r.addWidget(QLabel("End"))
        r.addWidget(self.end)
        r.addStretch(1)

        out_row = QWidget()
        out_h = QHBoxLayout(out_row)
        out_h.setContentsMargins(0, 0, 0, 0)
        self.out = QLineEdit()
        self.out.setPlaceholderText("Output folder…")
        browse = QPushButton("Browse")
        browse.clicked.connect(lambda: self.out.setText(self.pick_folder_cb()))
        out_h.addWidget(self.out, 1)
        out_h.addWidget(browse)

        form.addRow("Quality mode:", self.quality)
        form.addRow("Parallel videos:", self.threads)
        form.addRow("Parallel fragments:", self.frags)
        form.addRow("Range:", range_row)
        form.addRow("Save to:", out_row)
        root.addWidget(opts)

        root.addStretch(1)

    def load_info(self, url: str):
        url = url.strip()
        if not url:
            return

        self.title_lbl.setText("Title: Loading…")
        self.count_lbl.setText("Items: Loading…")

        if self.info_worker and self.info_worker.isRunning():
            self.info_worker.quit()
            self.info_worker.wait(200)

        self.info_worker = InfoWorker(url, allow_playlist=True)
        self.info_worker.info_ready.connect(self._show_info)
        self.info_worker.error.connect(self._show_err)
        self.info_worker.start()

    def _show_err(self, _msg: str):
        self.title_lbl.setText("Title: —")
        self.count_lbl.setText("Items: —")

    def _show_info(self, info: dict):
        title = info.get("title") or info.get("uploader") or "—"
        self.title_lbl.setText(f"Title: {title}")
        entries = info.get("entries")
        if isinstance(entries, list):
            self.count_lbl.setText(f"Items: {len(entries)} (count may be limited)")
        else:
            self.count_lbl.setText("Items: —")


class AudioTab(QWidget):
    def __init__(self, pick_folder_cb):
        super().__init__()
        self.pick_folder_cb = pick_folder_cb
        self.info_worker = None

        root = QVBoxLayout(self)
        root.setContentsMargins(18, 18, 18, 18)
        root.setSpacing(12)

        url_card = QGroupBox("Audio URL (video / playlist / channel)")
        url_layout = QVBoxLayout(url_card)
        self.url = QLineEdit()
        self.url.setPlaceholderText("Paste or drag a URL…")
        url_layout.addWidget(self.url)
        root.addWidget(url_card)

        info = QGroupBox("Info")
        info_layout = QVBoxLayout(info)
        self.title_lbl = QLabel("Title: —")
        self.count_lbl = QLabel("Items: —")
        self.count_lbl.setObjectName("Hint")
        info_layout.addWidget(self.title_lbl)
        info_layout.addWidget(self.count_lbl)
        root.addWidget(info)

        opts = QGroupBox("Options")
        form = QFormLayout(opts)

        self.format = QComboBox()
        self.format.addItem("MP3", "mp3")
        self.format.addItem("M4A (AAC)", "m4a")
        self.format.addItem("Opus", "opus")

        self.bitrate = QComboBox()
        for b in (128, 160, 192, 256, 320):
            self.bitrate.addItem(f"{b} kbps", str(b))
        self.bitrate.setCurrentText("192 kbps")

        self.threads = QSpinBox()
        self.threads.setRange(1, 8)
        self.threads.setValue(2)

        self.frags = QSpinBox()
        self.frags.setRange(1, 16)
        self.frags.setValue(4)

        range_row = QWidget()
        r = QHBoxLayout(range_row)
        r.setContentsMargins(0, 0, 0, 0)
        self.start = QSpinBox()
        self.start.setRange(1, 999999)
        self.start.setValue(1)
        self.end = QSpinBox()
        self.end.setRange(0, 999999)
        self.end.setValue(0)
        r.addWidget(QLabel("Start"))
        r.addWidget(self.start)
        r.addSpacing(10)
        r.addWidget(QLabel("End"))
        r.addWidget(self.end)
        r.addStretch(1)

        out_row = QWidget()
        out_h = QHBoxLayout(out_row)
        out_h.setContentsMargins(0, 0, 0, 0)
        self.out = QLineEdit()
        self.out.setPlaceholderText("Output folder…")
        browse = QPushButton("Browse")
        browse.clicked.connect(lambda: self.out.setText(self.pick_folder_cb()))
        out_h.addWidget(self.out, 1)
        out_h.addWidget(browse)

        self.format.currentIndexChanged.connect(self._sync_bitrate_enabled)
        self._sync_bitrate_enabled()

        form.addRow("Audio format:", self.format)
        form.addRow("MP3 bitrate:", self.bitrate)
        form.addRow("Parallel videos:", self.threads)
        form.addRow("Parallel fragments:", self.frags)
        form.addRow("Range:", range_row)
        form.addRow("Save to:", out_row)
        root.addWidget(opts)

        root.addStretch(1)

    def _sync_bitrate_enabled(self):
        self.bitrate.setEnabled(self.format.currentData() == "mp3")

    def load_info(self, url: str):
        url = url.strip()
        if not url:
            return

        self.title_lbl.setText("Title: Loading…")
        self.count_lbl.setText("Items: Loading…")

        if self.info_worker and self.info_worker.isRunning():
            self.info_worker.quit()
            self.info_worker.wait(200)

        self.info_worker = InfoWorker(url, allow_playlist=True)
        self.info_worker.info_ready.connect(self._show_info)
        self.info_worker.error.connect(self._show_err)
        self.info_worker.start()

    def _show_err(self, _msg: str):
        self.title_lbl.setText("Title: —")
        self.count_lbl.setText("Items: —")

    def _show_info(self, info: dict):
        title = info.get("title") or info.get("uploader") or "—"
        self.title_lbl.setText(f"Title: {title}")
        entries = info.get("entries")
        if isinstance(entries, list):
            self.count_lbl.setText(f"Items: {len(entries)} (count may be limited)")
        else:
            self.count_lbl.setText("Items: 1 (single video)")


class SettingsTab(QWidget):
    settings_changed = Signal()

    def __init__(self, prefs: AppPrefs, pick_folder_cb):
        super().__init__()
        self.prefs = prefs
        self.pick_folder_cb = pick_folder_cb

        root = QVBoxLayout(self)
        root.setContentsMargins(18, 18, 18, 18)
        root.setSpacing(12)

        appearance = QGroupBox("Appearance")
        aform = QFormLayout(appearance)
        self.theme = QComboBox()
        self.theme.addItem("Dark", "dark")
        self.theme.addItem("Light", "light")
        aform.addRow("Theme:", self.theme)
        root.addWidget(appearance)

        ff = QGroupBox("FFmpeg")
        fform = QFormLayout(ff)

        self.ffmpeg_path = QLineEdit()
        self.ffmpeg_path.setPlaceholderText("Optional: path to ffmpeg.exe or folder containing it (recommended on Windows)")

        ff_row = QWidget()
        h = QHBoxLayout(ff_row)
        h.setContentsMargins(0, 0, 0, 0)
        self.ff_browse = QPushButton("Browse…")
        self.ff_clear = QPushButton("Clear")
        h.addWidget(self.ffmpeg_path, 1)
        h.addWidget(self.ff_browse)
        h.addWidget(self.ff_clear)

        self.ff_status = QLabel("FFmpeg status: —")
        self.ff_status.setObjectName("Hint")

        fform.addRow("FFmpeg location:", ff_row)
        fform.addRow(self.ff_status)
        root.addWidget(ff)

        defaults = QGroupBox("Defaults")
        form = QFormLayout(defaults)

        self.video_folder = QLineEdit()
        self.audio_folder = QLineEdit()

        def folder_row(target: QLineEdit):
            w = QWidget()
            hh = QHBoxLayout(w)
            hh.setContentsMargins(0, 0, 0, 0)
            btn = QPushButton("Browse")
            btn.clicked.connect(lambda: self._pick_folder_into(target))
            hh.addWidget(target, 1)
            hh.addWidget(btn)
            return w

        self.default_quality = build_quality_combo()

        self.default_audio_format = QComboBox()
        self.default_audio_format.addItem("MP3", "mp3")
        self.default_audio_format.addItem("M4A (AAC)", "m4a")
        self.default_audio_format.addItem("Opus", "opus")

        self.default_mp3_bitrate = QComboBox()
        for b in (128, 160, 192, 256, 320):
            self.default_mp3_bitrate.addItem(f"{b} kbps", str(b))

        self.parallel_videos = QSpinBox()
        self.parallel_videos.setRange(1, 8)

        self.parallel_fragments = QSpinBox()
        self.parallel_fragments.setRange(1, 16)

        self.skip_existing = QCheckBox("Skip existing files (don’t re-download)")
        self.notifications = QCheckBox("Enable notifications")
        self.debug_logging = QCheckBox("Enable debug logging (recommended for GitHub issues)")

        form.addRow("Default video folder:", folder_row(self.video_folder))
        form.addRow("Default audio folder:", folder_row(self.audio_folder))
        form.addRow("Default video quality:", self.default_quality)
        form.addRow("Default audio format:", self.default_audio_format)
        form.addRow("Default MP3 bitrate:", self.default_mp3_bitrate)
        form.addRow("Default parallel videos:", self.parallel_videos)
        form.addRow("Default parallel fragments:", self.parallel_fragments)
        form.addRow(self.skip_existing)
        form.addRow(self.notifications)
        form.addRow(self.debug_logging)

        root.addWidget(defaults)

        hint = QLabel("Changes save automatically.")
        hint.setObjectName("Hint")
        root.addWidget(hint)
        root.addStretch(1)

        self.ff_browse.clicked.connect(self._pick_ffmpeg)
        self.ff_clear.clicked.connect(lambda: self.ffmpeg_path.setText(""))

        self.load_from_prefs()
        self._bind_autosave()
        self._update_ffmpeg_status()

        self.ffmpeg_path.textChanged.connect(self._update_ffmpeg_status)

    def _pick_folder_into(self, target_edit: QLineEdit):
        folder = QFileDialog.getExistingDirectory(self, "Choose Folder")
        if folder:
            target_edit.setText(folder)

    def _pick_ffmpeg(self):
        if os.name == "nt":
            file_path, _ = QFileDialog.getOpenFileName(
                self, "Select ffmpeg.exe", "", "FFmpeg (ffmpeg.exe);;All Files (*)"
            )
            if file_path:
                self.ffmpeg_path.setText(file_path)
        else:
            # On mac/linux, it's often just in PATH, but allow selecting file anyway
            file_path, _ = QFileDialog.getOpenFileName(self, "Select ffmpeg", "", "All Files (*)")
            if file_path:
                self.ffmpeg_path.setText(file_path)

    def _update_ffmpeg_status(self):
        ok, msg = ffmpeg_detected(self.ffmpeg_path.text())
        if ok:
            self.ff_status.setText(f"FFmpeg status: ✅ {msg}")
            self.ff_status.setObjectName("Good")
        else:
            self.ff_status.setText(f"FFmpeg status: ⚠ {msg}")
            self.ff_status.setObjectName("Warn")
        # Force style refresh
        self.ff_status.style().unpolish(self.ff_status)
        self.ff_status.style().polish(self.ff_status)

    def load_from_prefs(self):
        t = self.prefs.theme()
        idx = self.theme.findData(t)
        if idx >= 0:
            self.theme.setCurrentIndex(idx)

        self.ffmpeg_path.setText(self.prefs.ffmpeg_path())

        self.video_folder.setText(self.prefs.default_video_folder())
        self.audio_folder.setText(self.prefs.default_audio_folder())

        q = self.prefs.default_quality_mode()
        i = self.default_quality.findData(q)
        if i >= 0:
            self.default_quality.setCurrentIndex(i)

        af = self.prefs.default_audio_format()
        i2 = self.default_audio_format.findData(af)
        if i2 >= 0:
            self.default_audio_format.setCurrentIndex(i2)

        br = str(self.prefs.default_audio_bitrate())
        i3 = self.default_mp3_bitrate.findData(br)
        if i3 >= 0:
            self.default_mp3_bitrate.setCurrentIndex(i3)

        self.parallel_videos.setValue(self.prefs.default_parallel_videos())
        self.parallel_fragments.setValue(self.prefs.default_parallel_fragments())
        self.skip_existing.setChecked(self.prefs.skip_existing())
        self.notifications.setChecked(self.prefs.notifications())
        self.debug_logging.setChecked(self.prefs.debug_logging())

    def _bind_autosave(self):
        def save_all():
            self.prefs.set("appearance/theme", self.theme.currentData())
            self.prefs.set("ffmpeg/path", self.ffmpeg_path.text().strip())
            self.prefs.set("folders/video", self.video_folder.text().strip())
            self.prefs.set("folders/audio", self.audio_folder.text().strip())
            self.prefs.set("video/quality_mode", self.default_quality.currentData())
            self.prefs.set("audio/format", self.default_audio_format.currentData())
            self.prefs.set("audio/mp3_bitrate", int(self.default_mp3_bitrate.currentData()))
            self.prefs.set("threads/parallel_videos", int(self.parallel_videos.value()))
            self.prefs.set("threads/parallel_fragments", int(self.parallel_fragments.value()))
            self.prefs.set("behavior/skip_existing", bool(self.skip_existing.isChecked()))
            self.prefs.set("behavior/notifications", bool(self.notifications.isChecked()))
            self.prefs.set("debug/enabled", bool(self.debug_logging.isChecked()))
            self.settings_changed.emit()

        self.theme.currentIndexChanged.connect(save_all)
        self.ffmpeg_path.textChanged.connect(save_all)
        self.video_folder.textChanged.connect(save_all)
        self.audio_folder.textChanged.connect(save_all)
        self.default_quality.currentIndexChanged.connect(save_all)
        self.default_audio_format.currentIndexChanged.connect(save_all)
        self.default_mp3_bitrate.currentIndexChanged.connect(save_all)
        self.parallel_videos.valueChanged.connect(save_all)
        self.parallel_fragments.valueChanged.connect(save_all)
        self.skip_existing.stateChanged.connect(save_all)
        self.notifications.stateChanged.connect(save_all)
        self.debug_logging.stateChanged.connect(save_all)


# -------------------------
# Main Window
# -------------------------

class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.prefs = AppPrefs()
        self.logbuf = LogBuffer(max_lines=2500)

        self.dl_worker: DownloadWorker | None = None

        self.setWindowTitle("YouTube Downloader")
        self.setMinimumWidth(980)
        self.setAcceptDrops(True)

        self.tray = None
        self._init_tray()

        # Busy timer (post-processing)
        self._busy_started = None
        self._busy_base_text = None
        self._busy_timer = QTimer(self)
        self._busy_timer.setInterval(500)
        self._busy_timer.timeout.connect(self._tick_busy)

        root = QVBoxLayout(self)
        root.setContentsMargins(18, 18, 18, 18)
        root.setSpacing(12)

        header = QWidget()
        header_l = QHBoxLayout(header)
        header_l.setContentsMargins(0, 0, 0, 0)

        titles = QVBoxLayout()
        h_title = QLabel("YouTube Downloader")
        h_title.setObjectName("HeaderTitle")
        h_sub = QLabel("Release-ready: FFmpeg detection • Debug log • Light/Dark • Skip existing")
        h_sub.setObjectName("HeaderSub")
        titles.addWidget(h_title)
        titles.addWidget(h_sub)
        header_l.addLayout(titles, 1)

        self.quick_theme = QComboBox()
        self.quick_theme.addItem("Dark", "dark")
        self.quick_theme.addItem("Light", "light")
        self.quick_theme.setFixedWidth(120)
        header_l.addWidget(self.quick_theme)

        root.addWidget(header)

        self.tabs = QTabWidget()
        root.addWidget(self.tabs, 1)

        self.single_tab = SingleTab(self.pick_folder)
        self.playlist_tab = ListTab("playlist", self.pick_folder)
        self.channel_tab = ListTab("channel", self.pick_folder)
        self.audio_tab = AudioTab(self.pick_folder)
        self.settings_tab = SettingsTab(self.prefs, self.pick_folder)

        self.tabs.addTab(self.single_tab, "Single")
        self.tabs.addTab(self.playlist_tab, "Playlist")
        self.tabs.addTab(self.channel_tab, "Channel")
        self.tabs.addTab(self.audio_tab, "Audio")
        self.tabs.addTab(self.settings_tab, "Settings")

        action = QGroupBox("Download")
        action_layout = QHBoxLayout(action)

        self.download_btn = QPushButton("Download")
        self.download_btn.setObjectName("Primary")

        self.cancel_btn = QPushButton("Cancel")
        self.cancel_btn.setObjectName("Danger")
        self.cancel_btn.setEnabled(False)

        self.logs_btn = QPushButton("View Debug Log")
        action_layout.addWidget(self.download_btn, 1)
        action_layout.addWidget(self.cancel_btn)
        action_layout.addWidget(self.logs_btn)

        root.addWidget(action)

        self.progress = QProgressBar()
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        root.addWidget(self.progress)

        self.status = QLabel("Idle.")
        self.status.setObjectName("Hint")
        self.status.setWordWrap(True)
        root.addWidget(self.status)

        # events
        self.download_btn.clicked.connect(self.start_download)
        self.cancel_btn.clicked.connect(self.cancel_download)
        self.logs_btn.clicked.connect(self.show_logs)

        self.single_tab.url.textChanged.connect(lambda: self.single_tab.load_preview(self.single_tab.url.text()))
        self.playlist_tab.url.textChanged.connect(lambda: self.playlist_tab.load_info(self.playlist_tab.url.text()))
        self.channel_tab.url.textChanged.connect(lambda: self.channel_tab.load_info(self.channel_tab.url.text()))
        self.audio_tab.url.textChanged.connect(lambda: self.audio_tab.load_info(self.audio_tab.url.text()))

        self.settings_tab.settings_changed.connect(self.apply_defaults_to_tabs)

        self.quick_theme.currentIndexChanged.connect(self._quick_theme_changed)

        # Apply theme + defaults
        self._apply_theme(self.prefs.theme(), sync_quick=True)
        self.apply_defaults_to_tabs()

        # Default ffmpeg warning on first open
        ok, msg = ffmpeg_detected(self.prefs.ffmpeg_path())
        if not ok:
            self.status.setText(f"⚠ {msg}  (Set it in Settings → FFmpeg)")

    def _init_tray(self):
        if not QSystemTrayIcon.isSystemTrayAvailable():
            return
        icon = QApplication.style().standardIcon(QStyle.SP_ComputerIcon)
        self.tray = QSystemTrayIcon(icon, self)
        self.tray.setToolTip("YouTube Downloader")
        self.tray.show()

    def notify(self, title: str, message: str):
        if not self.prefs.notifications():
            return
        if self.tray and self.tray.isVisible():
            self.tray.showMessage(title, message, QSystemTrayIcon.Information, 4500)

    def _apply_theme(self, theme: str, sync_quick: bool = False):
        theme = theme if theme in ("dark", "light") else "dark"
        QApplication.instance().setStyleSheet(modern_stylesheet(theme))
        if sync_quick:
            i = self.quick_theme.findData(theme)
            if i >= 0:
                self.quick_theme.setCurrentIndex(i)

    def _quick_theme_changed(self):
        t = self.quick_theme.currentData()
        self.prefs.set("appearance/theme", t)
        self._apply_theme(t)
        idx = self.settings_tab.theme.findData(t)
        if idx >= 0 and self.settings_tab.theme.currentIndex() != idx:
            self.settings_tab.theme.setCurrentIndex(idx)

    def apply_defaults_to_tabs(self):
        self._apply_theme(self.prefs.theme(), sync_quick=True)

        # folders
        self.single_tab.out.setText(self.prefs.default_video_folder())
        self.playlist_tab.out.setText(self.prefs.default_video_folder())
        self.channel_tab.out.setText(self.prefs.default_video_folder())
        self.audio_tab.out.setText(self.prefs.default_audio_folder())

        # quality
        q = self.prefs.default_quality_mode()
        for tab in (self.single_tab, self.playlist_tab, self.channel_tab):
            idx = tab.quality.findData(q)
            if idx >= 0:
                tab.quality.setCurrentIndex(idx)

        # threads/frags
        self.single_tab.frags.setValue(self.prefs.default_parallel_fragments())
        self.playlist_tab.threads.setValue(self.prefs.default_parallel_videos())
        self.playlist_tab.frags.setValue(self.prefs.default_parallel_fragments())
        self.channel_tab.threads.setValue(self.prefs.default_parallel_videos())
        self.channel_tab.frags.setValue(self.prefs.default_parallel_fragments())
        self.audio_tab.threads.setValue(self.prefs.default_parallel_videos())
        self.audio_tab.frags.setValue(self.prefs.default_parallel_fragments())

        # audio defaults
        af = self.prefs.default_audio_format()
        idx2 = self.audio_tab.format.findData(af)
        if idx2 >= 0:
            self.audio_tab.format.setCurrentIndex(idx2)

        br = str(self.prefs.default_audio_bitrate())
        idx3 = self.audio_tab.bitrate.findData(br)
        if idx3 >= 0:
            self.audio_tab.bitrate.setCurrentIndex(idx3)

    # drag/drop URL
    def dragEnterEvent(self, event):
        if event.mimeData().hasText():
            event.acceptProposedAction()

    def dropEvent(self, event):
        text = event.mimeData().text().strip()
        idx = self.tabs.currentIndex()
        if idx == 0:
            self.single_tab.url.setText(text)
        elif idx == 1:
            self.playlist_tab.url.setText(text)
        elif idx == 2:
            self.channel_tab.url.setText(text)
        elif idx == 3:
            self.audio_tab.url.setText(text)

    def pick_folder(self) -> str:
        folder = QFileDialog.getExistingDirectory(self, "Choose Output Folder")
        return folder or ""

    def set_busy(self, busy: bool):
        self.download_btn.setEnabled(not busy)
        self.cancel_btn.setEnabled(busy)
        self.tabs.setEnabled(not busy)

    def set_spinner(self, spinning: bool):
        if spinning:
            self.progress.setRange(0, 0)
        else:
            self.progress.setRange(0, 100)

    def _busy_start(self, base_text: str):
        self._busy_started = time.time()
        self._busy_base_text = base_text
        if not self._busy_timer.isActive():
            self._busy_timer.start()

    def _busy_stop(self):
        self._busy_started = None
        self._busy_base_text = None
        if self._busy_timer.isActive():
            self._busy_timer.stop()

    def _tick_busy(self):
        if self._busy_started is None or not self._busy_base_text:
            return
        elapsed = int(time.time() - self._busy_started)
        mm = elapsed // 60
        ss = elapsed % 60
        self.status.setText(f"{self._busy_base_text}  ({mm:02d}:{ss:02d})")

    def on_worker_busy(self, spinning: bool):
        self.set_spinner(spinning)
        if spinning:
            # status text will arrive separately; timer starts when we receive a post-processing status
            pass
        else:
            self._busy_stop()

    def on_worker_status(self, text: str):
        # If we're in post-processing mode, start timer with that base text
        if self.progress.maximum() == 0:  # indeterminate (spinner)
            if self._busy_started is None:
                self._busy_start(text)
            else:
                self._busy_base_text = text
        else:
            self.status.setText(text)

    def show_logs(self):
        dlg = LogDialog(self, "Debug Log", self.logbuf.dump() or "(No logs yet)")
        dlg.exec()

    def start_download(self):
        if self.dl_worker and self.dl_worker.isRunning():
            return

        tab = self.tabs.currentIndex()
        if tab == 4:
            QMessageBox.information(self, "Settings", "Go to a download tab to start a download.")
            return

        # Clear log for fresh run
        self.logbuf.clear()
        self.logbuf.add("[info] Starting download…")

        skip_existing = self.prefs.skip_existing()
        ff_loc = self.prefs.ffmpeg_path()
        debug_enabled = self.prefs.debug_logging()

        if tab == 0:
            opts = DownloadOptions(
                url=self.single_tab.url.text().strip(),
                out_dir=self.single_tab.out.text().strip(),
                kind="single",
                mode=self.single_tab.quality.currentData(),
                audio_only=False,
                parallel_videos=1,
                parallel_fragments=int(self.single_tab.frags.value()),
                skip_existing=skip_existing,
                ffmpeg_location=ff_loc,
                debug_enabled=debug_enabled,
            )
        elif tab == 1:
            end_val = int(self.playlist_tab.end.value())
            opts = DownloadOptions(
                url=self.playlist_tab.url.text().strip(),
                out_dir=self.playlist_tab.out.text().strip(),
                kind="playlist",
                mode=self.playlist_tab.quality.currentData(),
                audio_only=False,
                start=int(self.playlist_tab.start.value()),
                end=(end_val if end_val > 0 else None),
                parallel_videos=int(self.playlist_tab.threads.value()),
                parallel_fragments=int(self.playlist_tab.frags.value()),
                skip_existing=skip_existing,
                ffmpeg_location=ff_loc,
                debug_enabled=debug_enabled,
            )
        elif tab == 2:
            end_val = int(self.channel_tab.end.value())
            opts = DownloadOptions(
                url=self.channel_tab.url.text().strip(),
                out_dir=self.channel_tab.out.text().strip(),
                kind="channel",
                mode=self.channel_tab.quality.currentData(),
                audio_only=False,
                start=int(self.channel_tab.start.value()),
                end=(end_val if end_val > 0 else None),
                parallel_videos=int(self.channel_tab.threads.value()),
                parallel_fragments=int(self.channel_tab.frags.value()),
                skip_existing=skip_existing,
                ffmpeg_location=ff_loc,
                debug_enabled=debug_enabled,
            )
        else:
            end_val = int(self.audio_tab.end.value())
            url = self.audio_tab.url.text().strip()
            kind_guess = "single"
            if "list=" in url or "/channel/" in url or "/@" in url or "/c/" in url or "/user/" in url:
                kind_guess = "playlist"

            opts = DownloadOptions(
                url=url,
                out_dir=self.audio_tab.out.text().strip(),
                kind=kind_guess,
                audio_only=True,
                audio_format=self.audio_tab.format.currentData(),
                audio_bitrate_kbps=int(self.audio_tab.bitrate.currentData()),
                start=int(self.audio_tab.start.value()),
                end=(end_val if end_val > 0 else None),
                parallel_videos=int(self.audio_tab.threads.value()),
                parallel_fragments=int(self.audio_tab.frags.value()),
                skip_existing=skip_existing,
                ffmpeg_location=ff_loc,
                debug_enabled=debug_enabled,
            )

        self.set_busy(True)
        self._busy_stop()
        self.set_spinner(False)
        self.progress.setValue(0)
        self.status.setText("Starting…")

        self.dl_worker = DownloadWorker(opts, self.logbuf)
        self.dl_worker.progress.connect(self.progress.setValue)
        self.dl_worker.status.connect(self.on_worker_status)
        self.dl_worker.busy.connect(self.on_worker_busy)
        self.dl_worker.done.connect(self.on_done)
        self.dl_worker.error.connect(self.on_error)
        self.dl_worker.log_line.connect(lambda ln: None)  # already stored in logbuf; keep hook for future
        self.dl_worker.start()

    def cancel_download(self):
        if self.dl_worker and self.dl_worker.isRunning():
            self.dl_worker.request_cancel()
            # Clear, honest messaging: might finish current file
            self.status.setText("Cancelling… (stopping after current file if needed)")
            self.logbuf.add("[warn] Cancel requested by user.")

    def on_done(self, out_path: str):
        self.set_busy(False)
        self._busy_stop()
        self.set_spinner(False)
        self.status.setText("Finished ✅")
        self.notify("Download complete", f"Saved to: {out_path}")
        QMessageBox.information(self, "Done", f"Saved to:\n{out_path}")

    def on_error(self, msg: str):
        self.set_busy(False)
        self._busy_stop()
        self.set_spinner(False)

        self.status.setText("Error ❌")
        self.notify("Download failed", msg)

        # Show error with an easy “Copy log” option
        m = QMessageBox(self)
        m.setIcon(QMessageBox.Critical)
        m.setWindowTitle("Error")
        m.setText(msg)
        copy_btn = m.addButton("View / Copy Debug Log", QMessageBox.ActionRole)
        m.addButton("Close", QMessageBox.RejectRole)
        m.exec()

        if m.clickedButton() == copy_btn:
            self.show_logs()


def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")

    prefs = AppPrefs()
    app.setStyleSheet(modern_stylesheet(prefs.theme()))

    w = MainWindow()
    w.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()