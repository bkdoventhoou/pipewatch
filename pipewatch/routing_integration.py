"""Helpers to wire AlertRouter into a PipelineWatcher at runtime."""

from typing import Callable, List

from pipewatch.alerts import Alert
from pipewatch.routing import AlertRouter, build_router_from_config
from pipewatch.watcher import PipelineWatcher


def attach_router_to_watcher(
    watcher: PipelineWatcher,
    router: AlertRouter,
) -> None:
    """Register a callback on *watcher* that routes every alert through *router*."""

    def _route_alert(alert: Alert) -> None:
        router.route(alert)

    watcher.on_alert(_route_alert)


def build_and_attach_router(
    watcher: PipelineWatcher,
    config: dict,
    handler_registry: dict,
) -> AlertRouter:
    """Build a router from config, register handlers from *handler_registry*,
    and attach it to *watcher*. Returns the configured router.

    Args:
        watcher: The PipelineWatcher instance to attach to.
        config: Full pipewatch config dict (must contain 'routing' key).
        handler_registry: Mapping of handler name -> callable(Alert).
    """
    router = build_router_from_config(config)
    for name, handler in handler_registry.items():
        router.register_handler(name, handler)
    attach_router_to_watcher(watcher, router)
    return router
