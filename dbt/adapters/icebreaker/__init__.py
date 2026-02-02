from dbt.adapters.icebreaker.connections import IcebreakerConnectionManager
from dbt.adapters.icebreaker.connections import IcebreakerCredentials
from dbt.adapters.icebreaker.impl import IcebreakerAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import icebreaker

Plugin = AdapterPlugin(
    adapter=IcebreakerAdapter,
    credentials=IcebreakerCredentials,
    include_path=icebreaker.PACKAGE_PATH,
)

__all__ = [
    "Plugin",
    "IcebreakerAdapter",
    "IcebreakerConnectionManager",
    "IcebreakerCredentials",
]
