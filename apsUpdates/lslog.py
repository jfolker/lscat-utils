import os
import sys
import syslog
from datetime import datetime

__console_threshold = syslog.LOG_INFO

def __lsloglevel_str(value):
    if value == syslog.LOG_ERR:
        return 'ERROR'
    elif value == syslog.LOG_WARNING:
        return 'WARNING'
    elif value == syslog.LOG_NOTICE:
        return 'NOTICE'
    elif value == syslog.LOG_INFO:
        return 'INFO'
    elif value == syslog.LOG_DEBUG:
        return 'DEBUG'
    else:
        return 'UNSPECIFIED'


def __lslog_init():
    if os.environ.get('DEBUG'):
        __console_threshold = syslog.LOG_DEBUG

    ident = 'lscat %s' % (sys.argv[0])
    opts = syslog.LOG_NDELAY | syslog.LOG_PID | syslog.LOG_CONS
    syslog.openlog(ident=ident, logoption=opts, facility=syslog.LOG_USER)
    print('NOTICE: lslog initialized - ident=\"%s\"' % ident)


__lslog_init()


def __lslog_log(level, msg):
    if level <= __console_threshold:
        sys.stderr.write('%s %s: %s\n'
                         % (datetime.now(), __lsloglevel_str(level), msg))
    syslog.syslog(level, msg)


def error(msg: str):
    __lslog_log(syslog.LOG_ERR, msg)


def warning(msg: str):
    __lslog_log(syslog.LOG_WARNING, msg)


def notice(msg: str):
    __lslog_log(syslog.LOG_NOTICE, msg)


def info(msg: str):
    __lslog_log(syslog.LOG_INFO, msg)


def debug(msg: str):
    __lslog_log(syslog.LOG_DEBUG, msg)
