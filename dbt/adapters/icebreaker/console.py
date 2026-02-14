"""
Icebreaker Console Output

Centralized, styled terminal output for the dbt-icebreaker adapter.
Uses the `rich` library for professional CLI presentation.

Usage:
    from dbt.adapters.icebreaker.console import console

    console.info("Loaded 189 sources from manifest")
    console.success("Cached halo.partners_raw")
    console.warn("Could not sync table DDL")
    console.error("Snowflake connection failed")
    console.step("Transpiling SQL...")
"""

import os
from typing import Optional

from rich.console import Console as RichConsole
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
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
# IcebreakerConsole
# ---------------------------------------------------------------------------

class IcebreakerConsole:
    """Styled output for dbt-icebreaker with verbosity support."""

    def __init__(self) -> None:
        self._rich = RichConsole(theme=_THEME, highlight=False)
        self._verbosity = Verbosity.from_env()

    # -- public api ---------------------------------------------------------

    def info(self, msg: str) -> None:
        """Background/context message (dim). Shown at normal+ verbosity."""
        if self._verbosity >= Verbosity.NORMAL:
            self._rich.print(f"  [ib.info]{msg}[/]")

    def success(self, msg: str) -> None:
        """Completed action. Shown at normal+ verbosity."""
        if self._verbosity >= Verbosity.NORMAL:
            self._rich.print(f"  [ib.success]✓[/] {msg}")

    def warn(self, msg: str) -> None:
        """Non-fatal issue. Always shown (except quiet hides non-errors)."""
        if self._verbosity >= Verbosity.NORMAL:
            self._rich.print(f"  [ib.warn]![/] {msg}")

    def error(self, msg: str) -> None:
        """Failure. Always shown."""
        self._rich.print(f"  [ib.error]✗[/] {msg}")

    def step(self, msg: str) -> None:
        """In-progress action. Shown at verbose only."""
        if self._verbosity >= Verbosity.VERBOSE:
            self._rich.print(f"  [ib.step]›[/] {msg}")

    def debug(self, msg: str) -> None:
        """Debug-level detail. Shown at verbose only."""
        if self._verbosity >= Verbosity.VERBOSE:
            self._rich.print(f"  [ib.muted]{msg}[/]")

    # -- structured output --------------------------------------------------

    def panel(self, content: str, title: str = "", border_style: str = "cyan") -> None:
        """Display a bordered panel. Always shown."""
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
