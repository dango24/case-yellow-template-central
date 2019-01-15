import logging
import logging.handlers
import os
import platform
import socket

from decorators import static_vars

def get_module_name(filename):
    return os.path.splitext(os.path.basename(filename))[0]

@static_vars(fh=None, ch=None, logPath=None)
def get_logger(logPath, loggerName=__name__, modulePath=None):
    if modulePath is not None:
        loggerName = get_module_name(modulePath)
    # Get logger
    logger = logging.getLogger(loggerName)
    if not len(logger.handlers):
        # Logger was just created, initialize
        logger.setLevel(logging.DEBUG)
        this = get_logger
        if this.logPath != logPath:
            this.logPath = logPath
            # Create a rotating file handler with a max file size of 2 MiB
            fh = logging.handlers.RotatingFileHandler(logPath, maxBytes=2097152, backupCount=4)
            # Create console handler to print to the console as well
            ch = logging.StreamHandler()
            # Create formatter
            formatter = logging.Formatter("%(asctime)s - %(name)s/%(module)s - %(levelname)s"
                                          " - %(funcName)s:%(lineno)d;%(threadName)s - %(message)s")
            # Set formatter on handlers
            fh.setFormatter(formatter)
            ch.setFormatter(formatter)
            this.fh = fh
            this.ch = ch
        # Add handlers to logger
        logger.addHandler(this.fh)
        logger.addHandler(this.ch)
        # Do not propagate
        logger.propagate = False

    return logger


def getConfigPath(self):
    workingDir = os.path.dirname(os.path.realpath(__file__))
    configPath = os.path.join(workingDir, "../config")
    return configPath

def get_log_path():
    LOG_NAME = "connections-agent.log"

    systype = platform.system()
    if systype == "Darwin":
        # On Mac, log in the user logs library
        user_subdir = "Library/Logs"
        logname = LOG_NAME
    else:
        # On linux, log in the home directory
        user_subdir = ""
        # Make the log hidden
        logname = "." + LOG_NAME

    homepath = os.path.expanduser('~')
    return os.path.join(homepath, user_subdir, logname)

@static_vars(logpath=None)
def user_logger(*args, **kwargs):
    this = user_logger
    if this.logpath is None:
        this.logpath = get_log_path()
    return get_logger(this.logpath, *args, **kwargs)

# Get the Hostname
def getHostname():
    logger = user_logger()
    hostname = ""
    try:
        # Will be escaped by requests
        hostname = socket.gethostname()
    except IOError as e:
        # Not sure if this will ever happen, but kept for safety
        logger.warning("An error has occurred while getting hostname. Error: {0}".format(e))
    return hostname
