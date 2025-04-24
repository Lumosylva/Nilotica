#!/usr/bin/env python
#  -*- coding: utf-8 -*-
import json
import logging
from datetime import datetime

# 增加 panic fatal 级别日志
PANIC = 60
logging.addLevelName(PANIC, "PANIC")
logging.addLevelName(logging.CRITICAL, "FATAL")

# ---------------------------------------------------------------------------
#   Logger
# ---------------------------------------------------------------------------
class Logger(logging.Logger):

    def debug(self, msg, *args, **kwargs):
        if self.isEnabledFor(logging.DEBUG):
            kwargs = self.kwargs_to_extra(**kwargs)
            self._log(logging.DEBUG, msg, args, **kwargs)

    def info(self, msg, *args, **kwargs):
        if self.isEnabledFor(logging.INFO):
            kwargs = self.kwargs_to_extra(**kwargs)
            self._log(logging.INFO, msg, args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        if self.isEnabledFor(logging.WARNING):
            kwargs = self.kwargs_to_extra(**kwargs)
            self._log(logging.WARNING, msg, args, **kwargs)

    def error(self, msg, *args, **kwargs):
        if self.isEnabledFor(logging.ERROR):
            kwargs = self.kwargs_to_extra(**kwargs)
            self._log(logging.ERROR, msg, args, **kwargs)

    def fatal(self, msg, *args, **kwargs):
        if self.isEnabledFor(logging.FATAL):
            kwargs = self.kwargs_to_extra(**kwargs)
            self._log(logging.FATAL, msg, args, **kwargs)

    critical = fatal

    def panic(self, message, *args, **kwargs):
        if self.isEnabledFor(PANIC):
            kwargs = self.kwargs_to_extra(**kwargs)
            self._log(PANIC, message, args, **kwargs)

    def log(self, level, msg, *args, **kwargs):
        if not isinstance(level, int):
            if logging.raiseExceptions:
                raise TypeError("level must be an integer")
            else:
                return
        if self.isEnabledFor(level):
            kwargs = self.kwargs_to_extra(**kwargs)
            self._log(level, msg, args, **kwargs)

    @staticmethod
    def kwargs_to_extra(**kwargs):
        """
        将 kwargs 除 ["exc_info", "extra", "stack_info", "stack_level"] 之外的参数添加到 extra 字段上，
        然后将 extra 的值放在 extra["extra"] 下
        """
        extra = kwargs.get("extra", {}).copy()
        for k in set(kwargs.keys()) - {"exc_info", "extra", "stack_info", "stack_level"}:
            extra[k] = kwargs[k]
            del kwargs[k]
        kwargs["extra"] = {"extra": extra}
        return kwargs


logging.setLoggerClass(klass=Logger)

# ---------------------------------------------------------------------------
#   JSONFormatter
# ---------------------------------------------------------------------------
_VALID_ATTRS_to_BUILTIN_ATTRS = {
    'created': "created",
    'file_name': 'filename',
    'func_name': 'funcName',
    'level_no': 'levelno',
    'line_no': 'lineno',
    'module': 'module',
    'msecs': 'msecs',
    'path_name': 'pathname',
    'process': 'process',
    'process_name': 'processName',
    'relative_created': 'relativeCreated',
    'thread': 'thread',
    'thread_name': 'threadName'
}

_VALID_ATTRS = set(_VALID_ATTRS_to_BUILTIN_ATTRS.keys())


class JSONFormatter(logging.Formatter):

    def __init__(self, log_keys: list = None):
        self.log_keys = (set(log_keys) & _VALID_ATTRS) if log_keys else set()

    def format(self, record):
        extra = self.extra_from_record(record)
        extra['msg'] = record.getMessage()
        extra['time'] = datetime.fromtimestamp(record.created).astimezone().isoformat()
        extra['level'] = self.get_levelname(record.levelno).lower()
        extra['name'] = record.name
        for k in self.log_keys:
            extra[k] = getattr(record, _VALID_ATTRS_to_BUILTIN_ATTRS[k])
        if record.exc_info:
            extra['exc_info'] = self.formatException(record.exc_info)
        if record.stack_info:
            extra['stack_info'] = record.stack_info
        return self.to_json(extra)

    def get_levelname(self, levelno):
        if isinstance(levelno, str):
            return levelno
        levelno = levelno // 10 * 10
        if levelno < 0:
            levelno = 0
        elif levelno > 60:
            levelno = 60
        return logging.getLevelName(levelno)

    def extra_from_record(self, record):
        return record.__dict__["extra"] if "extra" in record.__dict__ else {}

    def to_json(self, record):
        try:
            return json.dumps(record, ensure_ascii=False)
        except TypeError:
            return json.dumps(record, default=_json_serializable, ensure_ascii=False)


def _json_serializable(obj):
    try:
        return obj.__dict__
    except AttributeError:
        return str(obj)


# ---------------------------------------------------------------------------
#   LoggerAdapter
# ---------------------------------------------------------------------------
class LoggerAdapter(logging.LoggerAdapter):

    def __init__(self, logger, **kwargs):
        self.logger = logger
        self.extra = kwargs

    def process(self, msg, kwargs):
        if "extra" in kwargs:
            extra = self.extra.copy()
            extra.update(kwargs["extra"])
            kwargs["extra"] = extra
        else:
            kwargs["extra"] = self.extra
        return msg, kwargs

    def bind(self, **kwargs):
        extra = self.extra.copy()
        extra.update(kwargs)
        return LoggerAdapter(self.logger, **extra)


root = Logger(logging.WARNING)
root.name = 'root'
logging.Logger.root = root
logging.Logger.manager = logging.Manager(logging.Logger.root)


def getLogger(name=None):
    if name:
        return logging.Logger.manager.getLogger(name)
    else:
        return root


logging.getLogger = getLogger
