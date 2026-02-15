"""
Icebreaker Console Output

Centralized, styled terminal output for the dbt-icebreaker adapter.
Uses the `rich` library for professional CLI presentation with
thread-safe output and a shared ASCII spinner.

Usage:
    from dbt.adapters.icebreaker.console import console

    console.info("Loaded 189 sources from manifest")
    console.success("Cached halo.partners_raw")
    console.warn("Could not sync table DDL")
    console.error("Snowflake connection failed")
    console.step("Transpiling SQL...")

    # Thread-safe spinner (works with concurrent dbt threads)
    with console.spinning("Downloading from Snowflake..."):
        cursor.execute(query)
"""

import os
import sys
import threading
import time
from contextlib import contextmanager
from typing import Optional

from rich.console import Console as RichConsole
from rich.panel import Panel
from rich.table import Table
from rich.theme import Theme


# ---------------------------------------------------------------------------
# Verbosity levels
# ---------------------------------------------------------------------------

class Verbosity:
    QUIET = 0    # Errors and final summary only
    NORMAL = 1   # Success, warnings, errors, summary (default)
    VERBOSE = 2  # Everything including transpilation and debug

    @staticmethod
    def from_env() -> int:
        val = os.environ.get("ICEBREAKER_VERBOSITY", "normal").lower().strip()
        return {
            "quiet": Verbosity.QUIET,
            "normal": Verbosity.NORMAL,
            "verbose": Verbosity.VERBOSE,
            "0": Verbosity.QUIET,
            "1": Verbosity.NORMAL,
            "2": Verbosity.VERBOSE,
        }.get(val, Verbosity.NORMAL)


# ---------------------------------------------------------------------------
# Theme
# ---------------------------------------------------------------------------

_THEME = Theme({
    "ib.success": "green",
    "ib.warn": "yellow",
    "ib.error": "bold red",
    "ib.step": "cyan",
    "ib.info": "dim",
    "ib.label": "bold",
    "ib.muted": "dim italic",
})


# ---------------------------------------------------------------------------
# Shared Spinner (thread-safe, supports multiple concurrent callers)
# ---------------------------------------------------------------------------

_SPIN_FRAMES = ['|', '/', '-', '\\']


class _SharedSpinner:
    """A single global spinner that multiple threads can register with.
    
    Only one daemon thread ever runs. Multiple threads calling register()
    increase a ref count; unregister() decreases it. The spinner shows
    the most recently registered message, or a count when >1 are active.
    
    All print methods clear the spinner line before printing so that
    regular output and the spinner never garble each other.
    """
    
    def __init__(self, print_lock: threading.Lock):
        self._print_lock = print_lock
        self._ops_lock = threading.Lock()
        self._active_ops: dict[int, str] = {}  # thread_id -> message
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._frame_idx = 0
        # Track the length of the last spinner line for clean clearing
        self._last_line_len = 0
    
    @property
    def is_active(self) -> bool:
        return self._running
    
    def register(self, message: str):
        """Register the calling thread with a spinner message."""
        with self._ops_lock:
            self._active_ops[threading.current_thread().ident] = message
            if not self._running:
                self._running = True
                self._frame_idx = 0
                self._thread = threading.Thread(target=self._animate, daemon=True)
                self._thread.start()
    
    def unregister(self):
        """Unregister the calling thread. Spinner stops when all done."""
        with self._ops_lock:
            self._active_ops.pop(threading.current_thread().ident, None)
            if not self._active_ops:
                self._running = False
    
    def clear_line(self):
        """Clear the spinner line. Called by print methods WHILE HOLDING print_lock."""
        if self._last_line_len > 0:
            sys.stdout.write('\r' + ' ' * self._last_line_len + '\r')
            sys.stdout.flush()
            self._last_line_len = 0
    
    def _animate(self):
        """Background animation loop."""
        while self._running:
            with self._ops_lock:
                count = len(self._active_ops)
                if count == 0:
                    break
                elif count == 1:
                    msg = list(self._active_ops.values())[0]
                else:
                    # Show count + one of the active messages
                    sample = list(self._active_ops.values())[0]
                    msg = f"{sample} (+{count - 1} more)"
            
            frame = _SPIN_FRAMES[self._frame_idx % 4]
            line = f'\r  {frame} {msg}'
            
            with self._print_lock:
                sys.stdout.write(line)
                sys.stdout.flush()
                self._last_line_len = len(line)
            
            self._frame_idx += 1
            time.sleep(0.15)
        
        # Final cleanup
        with self._print_lock:
            self.clear_line()


# ---------------------------------------------------------------------------
# Download Tracker (thread-safe progress for source caching)
# ---------------------------------------------------------------------------

class DownloadTracker:
    """Thread-safe tracker for source downloads across concurrent threads.
    
    Usage:
        tracker.start("HALO.CLAIMS_RAW")
        # ... download ...
        done, total = tracker.finish("HALO.CLAIMS_RAW")
        bar = console.progress_bar(done, total)
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._total = 0
        self._done = 0
        self._active: dict[str, float] = {}  # name -> start_time

    def start(self, name: str):
        """Register a source download starting."""
        with self._lock:
            self._total += 1
            self._active[name] = time.time()

    def finish(self, name: str) -> tuple[int, int]:
        """Mark a source download complete. Returns (done, total)."""
        with self._lock:
            self._done += 1
            self._active.pop(name, None)
            return self._done, self._total

    @property
    def summary(self) -> str:
        """Current progress summary."""
        with self._lock:
            return f"{self._done}/{self._total} sources"

    def reset(self):
        """Reset for a new run."""
        with self._lock:
            self._total = 0
            self._done = 0
            self._active.clear()


# ---------------------------------------------------------------------------
# IcebreakerConsole
# ---------------------------------------------------------------------------

class IcebreakerConsole:
    """Styled output for dbt-icebreaker with verbosity support."""

    def __init__(self) -> None:
        self._rich = RichConsole(theme=_THEME, highlight=False)
        self._verbosity = Verbosity.from_env()
        self._print_lock = threading.Lock()
        self._spinner = _SharedSpinner(self._print_lock)
        self._download_tracker = DownloadTracker()

    # -- internal -----------------------------------------------------------

    def _safe_print(self, text: str) -> None:
        """Print a line, clearing any active spinner first."""
        with self._print_lock:
            self._spinner.clear_line()
            self._rich.print(text)

    # -- public api ---------------------------------------------------------

    def info(self, msg: str) -> None:
        """Background/context message (dim). Shown at normal+ verbosity."""
        if self._verbosity >= Verbosity.NORMAL:
            self._safe_print(f"  [ib.info]{msg}[/]")

    def success(self, msg: str) -> None:
        """Completed action. Shown at normal+ verbosity."""
        if self._verbosity >= Verbosity.NORMAL:
            self._safe_print(f"  [ib.success]✓[/] {msg}")

    def warn(self, msg: str) -> None:
        """Non-fatal issue. Always shown (except quiet hides non-errors)."""
        if self._verbosity >= Verbosity.NORMAL:
            self._safe_print(f"  [ib.warn]![/] {msg}")

    def error(self, msg: str) -> None:
        """Failure. Always shown."""
        self._safe_print(f"  [ib.error]✗[/] {msg}")

    def step(self, msg: str) -> None:
        """In-progress action. Shown at verbose only."""
        if self._verbosity >= Verbosity.VERBOSE:
            self._safe_print(f"  [ib.step]›[/] {msg}")

    def debug(self, msg: str) -> None:
        """Debug-level detail. Shown at verbose only."""
        if self._verbosity >= Verbosity.VERBOSE:
            self._safe_print(f"  [ib.muted]{msg}[/]")

    # -- spinner ------------------------------------------------------------

    @contextmanager
    def spinning(self, message: str):
        """Context manager for a thread-safe ASCII spinner.
        
        Multiple threads can call this concurrently — they share a
        single global spinner daemon that cycles through | / - \\.
        
        Usage:
            with console.spinning("Downloading from Snowflake..."):
                cursor.execute(query)
        """
        if self._verbosity < Verbosity.NORMAL:
            yield
            return
        
        self._spinner.register(message)
        try:
            yield
        finally:
            self._spinner.unregister()

    # -- download tracker ---------------------------------------------------

    @property
    def download_tracker(self) -> DownloadTracker:
        return self._download_tracker

    # -- progress bar -------------------------------------------------------

    @staticmethod
    def progress_bar(current: int, total: int, width: int = 20) -> str:
        """Format a text progress bar.
        
        Returns: '[████████░░░░░░░░░░░░] 40%'
        """
        if total <= 0:
            return f"[{'░' * width}]  0%"
        
        pct = min(current / total, 1.0)
        filled = int(width * pct)
        empty = width - filled
        bar = '█' * filled + '░' * empty
        return f"[{bar}] {pct:>4.0%}"

    # -- structured output --------------------------------------------------

    def panel(self, content: str, title: str = "", border_style: str = "cyan") -> None:
        """Display a bordered panel. Always shown."""
        self._safe_print("")  # Ensure spinner is cleared
        with self._print_lock:
            self._rich.print(Panel(content, title=title, border_style=border_style, padding=(0, 1)))

    def table(self, title: str, columns: list[tuple[str, str]], rows: list[list[str]]) -> None:
        """
        Display a formatted table. Always shown.

        Args:
            title: Table title
            columns: list of (name, style) tuples
            rows: list of row data (list of strings)
        """
        tbl = Table(title=title, show_edge=False, pad_edge=False, box=None)
        for name, style in columns:
            tbl.add_column(name, style=style)
        for row in rows:
            tbl.add_row(*row)
        self._safe_print("")  # Ensure spinner is cleared
        with self._print_lock:
            self._rich.print(tbl)

    def summary_panel(
        self,
        title: str,
        stats: dict[str, str],
        footer: Optional[str] = None,
    ) -> None:
        """
        Display a summary panel with key-value stats.

        Args:
            title: Panel title
            stats: dict of label → value
            footer: Optional footer text
        """
        lines = []
        max_key = max(len(k) for k in stats) if stats else 0
        for key, value in stats.items():
            lines.append(f"  [ib.label]{key:<{max_key}}[/]  {value}")

        content = "\n".join(lines)
        if footer:
            content += f"\n\n  [ib.muted]{footer}[/]"

        self._safe_print("")  # Ensure spinner is cleared
        with self._print_lock:
            self._rich.print(Panel(
                content,
                title=f"[bold]{title}[/]",
                border_style="cyan",
                padding=(1, 1),
            ))

    # -- verbosity ----------------------------------------------------------

    @property
    def verbosity(self) -> int:
        return self._verbosity

    @verbosity.setter
    def verbosity(self, level: int) -> None:
        self._verbosity = level

    @property
    def is_verbose(self) -> bool:
        return self._verbosity >= Verbosity.VERBOSE

    @property
    def is_quiet(self) -> bool:
        return self._verbosity <= Verbosity.QUIET


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------

console = IcebreakerConsole()
