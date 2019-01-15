var winston = require('winston');
var mkdirp = require('mkdirp');
var path = require('path');
var os = require('os');
var config = require("./config");
var requests = require("./requests");

const FILENAME = 'connections-client.log';

var logDirectory;
var logDirectoryExists;
try {
    // Configure directory to log files to
    switch (global.platform) {
        case "darwin":
            logDirectory = path.join(os.homedir(), 'Library/Logs/AmazonConnections');
            break;
        case "win32":
            logDirectory = path.join(process.env.APPDATA, 'AmazonConnections');
            break;
        case "linux":
            logDirectory = path.join(os.homedir(), '.acme/logs/AmazonConnections');
            break;
    }
    // Create directory if it doesn't already exist
    mkdirp.sync(logDirectory);
    logDirectoryExists = true;
} catch(e) {
    logDirectoryExists = false;
    var errorMsg = "Could not create log directory: " + logDirectory + ". " + e;
    console.error(errorMsg);
    requests.reportError(config.DISCO_APP_EXCEPTION, errorMsg);
}

/**
 * Use this to log events to rotating file (e.g. logger.info(message, metadata)).
 * This will also log to console in developer mode, or stderr if failed to create
 * log directory to write to.
 */
module.exports = function(isDeveloperMode) {

    var logger = new winston.Logger({
        emitErrs: false, // suppress so that we don't crash on logging errors
        exitOnError: false,
        transports: []
    });

    try {
        if (isDeveloperMode) {
            logger.add(winston.transports.Console, {
                colorize: true
            });
        } else if (!logDirectoryExists) {
            logger.add(winston.transports.Console, {
                stderrLevels: ['error', 'debug', 'info', 'warn']
            });
        }

        if (logDirectoryExists) {
            logger.add(require('winston-daily-rotate-file'), {
                filename: isDeveloperMode ? ('dev-' + FILENAME) : FILENAME,
                dirname: logDirectory,
                json: false,
                datePattern : 'YYYY-MM',  // rotate monthly
                maxsize: 1024 * 1024 * 10, // 10MB
                prettyPrint: function(meta) {
                    return os.EOL + '\t' + JSON.stringify(meta);
                }
            });
        }
    } catch(e) {
        var errorMsg = "Could not configure logger: " + e;
        console.error(errorMsg);
        requests.reportError(config.DISCO_APP_EXCEPTION, errorMsg);
    }

    return logger;
};
