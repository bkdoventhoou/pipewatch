"""Tests for cli_watch entry point."""

import signal
from unittest.mock import MagicMock, patch, call
import pytest

from pipewatch.cli_watch import parse_args, main


def test_parse_args_defaults():
    args = parse_args([])
    assert args.config == "pipewatch.yaml"
    assert args.interval is None
    assert not args.verbose


def test_parse_args_custom():
    args = parse_args(["--config", "custom.yaml", "--interval", "30", "--verbose"])
    assert args.config == "custom.yaml"
    assert args.interval == 30.0
    assert args.verbose


@patch("pipewatch.cli_watch.PipelineScheduler")
@patch("pipewatch.cli_watch.PipelineWatcher")
@patch("pipewatch.cli_watch.build_handlers_from_config", return_value=[])
@patch("pipewatch.cli_watch.AlertDispatcher")
@patch("pipewatch.cli_watch.build_collector_from_config")
@patch("pipewatch.cli_watch.load_config")
def test_main_uses_config_interval(
    mock_load_config, mock_build_collector, mock_dispatcher,
    mock_handlers, mock_watcher, mock_scheduler
):
    mock_load_config.return_value = {"watch": {"interval_seconds": 45.0}}
    mock_build_collector.return_value = MagicMock()
    mock_dispatcher.return_value = MagicMock()

    fake_thread = MagicMock()
    fake_scheduler_instance = MagicMock()
    fake_scheduler_instance._thread = fake_thread
    mock_scheduler.return_value = fake_scheduler_instance

    main(["--config", "pipewatch.yaml"])

    mock_scheduler.assert_called_once_with(
        interval_seconds=45.0, task=mock_watcher.return_value.run_once
    )
    fake_scheduler_instance.start.assert_called_once()
    fake_thread.join.assert_called_once()


@patch("pipewatch.cli_watch.PipelineScheduler")
@patch("pipewatch.cli_watch.PipelineWatcher")
@patch("pipewatch.cli_watch.build_handlers_from_config", return_value=[])
@patch("pipewatch.cli_watch.AlertDispatcher")
@patch("pipewatch.cli_watch.build_collector_from_config")
@patch("pipewatch.cli_watch.load_config")
def test_main_cli_interval_overrides_config(
    mock_load_config, mock_build_collector, mock_dispatcher,
    mock_handlers, mock_watcher, mock_scheduler
):
    mock_load_config.return_value = {"watch": {"interval_seconds": 45.0}}
    mock_build_collector.return_value = MagicMock()
    mock_dispatcher.return_value = MagicMock()

    fake_thread = MagicMock()
    fake_scheduler_instance = MagicMock()
    fake_scheduler_instance._thread = fake_thread
    mock_scheduler.return_value = fake_scheduler_instance

    main(["--config", "pipewatch.yaml", "--interval", "15"])

    mock_scheduler.assert_called_once_with(
        interval_seconds=15.0, task=mock_watcher.return_value.run_once
    )
