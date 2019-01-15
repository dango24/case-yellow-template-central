var electron = require('electron');
var app = electron.app;  // Module to control application life.
var BrowserWindow = electron.BrowserWindow;  // Module to create native browser window.
var Tray = electron.Tray;
var ipc = electron.ipcMain;
var fs = require('fs');
var path = require('path');
var jade = require('jade');
var argv = require('minimist')(process.argv.slice(1));
var validator = require('validator');

// Close the app if an unknown error occurs
process.on('uncaughtException', handleUncaughtException);

var config = require("./modules/config");
var requests = require("./modules/requests");
var exceptions = require("./modules/exceptions");

var mainWindow = null;
var tray = null;

const usage = "usage: [--username | -u <value>] [--dev | -d] [--stage | -s <value>] [--pipeuuid | -p <value>]\n";
// Check for required arguments
var username = argv.username || argv.u;
var stage = argv.stage || argv.s;


if (!username) {
     let logMessage = "Username not found.";
     var events = requests.addEvent(null, config.ClientEventType.USERNAME_NOT_FOUND, config.ClientEventLevel.ERROR, logMessage);
     requests.reportClientEvents(events);
     process.stderr.write(usage);
     waitAndExit(1);
 }
config.setUsername(username);

var stageWarningMsg = "";
try {
    config.setStage(stage);
} catch(e) {
    if (e instanceof exceptions.IllegalArgumentException) {
        stageWarningMsg = e.message + ". Defaulting to prod.";
        // Default to prod stage if the provided stage is not recognized
        config.setStage(config.STAGES.PROD);
    } else {
        throw e;
    }
}
stage = config.getStage();

// Set the website domain globally
global.domain = config.WEBSITE_DOMAINS[stage];

// Check for optional arguments
var isDeveloperMode = Boolean(argv.dev) || Boolean(argv.d);
global.isDeveloperMode = isDeveloperMode;

// Configure logger
var logger = require("./modules/logger")(isDeveloperMode);
if (stageWarningMsg) {
    logger.warn(stageWarningMsg);
}
logger.info("Stage: " + config.getStage());
logger.info("Username: " + username);
logger.info("Base URL: " + config.WEBSITE_DOMAINS[config.getStage()]);

// Pipe name passed in by the display helper
let pipeName = argv.pipeuuid || argv.p;
if(pipeName && validator.isUUID(pipeName)) {
    pipeName = "ConnectionsDisplayHelperToApp-" + pipeName;
}
else {
    pipeName = undefined;
}

let hasPipe = pipeName !== undefined;
if(argv.nopipe) {
    hasPipe = true;
}

const onWindows = process.platform === 'win32';
const onMac = process.platform === 'darwin';

if (onWindows || onMac) {
  app.setAccessibilitySupportEnabled(true);
}

var pipeClient = null;
var whenPipeConnected = new Promise(function(resolve, reject) {
    if (!onWindows || !hasPipe) {
        // We are not on windows or no pipe was specified, resolve the promise right away
        resolve();
    }
    else {
        // Replace stdout with named pipe since it doesn't work on windows
        let net = require('net');

        // From: http://stackoverflow.com/a/32172145
        const pipePath = "\\\\.\\pipe\\" + pipeName;

        process.stderr.write("Attempting to connect to pipe " + pipeName + "\n");
        var stdwrite = process.stdout.write;
        pipeClient = net.connect(pipePath, function() {
            process.stderr.write("Pipe connected\n");
            // From: https://gist.github.com/pguillory/729616
            process.stdout.write = function(string, encoding, fd) {
                stdwrite.apply(process.stdout, arguments);
                pipeClient.write(string);
            };
            resolve();
        }).on('error', function(e) {
            // If we are in dev mode and couldn't connect to the pipe that's fine, otherwise exit
            // as the daemon won't be able to read the status and will respawn anyway
            const errorMsg = "Could not connect to pipe (error code: " + e.code + ")";
            var events = requests.addEvent(null, config.ClientEventType.PIPE_CONNECTION_ERROR, config.ClientEventLevel.ERROR, errorMsg);
            requests.reportClientEvents(events);

            if (isDeveloperMode) {
                const noPipeServer = e.code === "ENOENT";
                const timedOut = e.code === "ETIMEDOUT";
                if (!noPipeServer && !timedOut) {
                    reject();
                    // We weren't expecting this, rethrow
                    throw e;
                } else if (timedOut) {
                    process.stderr.write("The pipe connection timed out, likely another app " +
                        "is already running and connected. Try using a different pipe name.\n");
                }
                process.stderr.write(errorMsg + " but in dev mode so continuing\n");
                resolve();
            }
            else {
                reject();
                process.stderr.write(errorMsg + ", exiting\n");
                // For some reason electron gets in a bad state if you just call process.exit() and leaves
                // a zombie gui process with ~100k memory hanging forever.
                // Calling via setTimeout seems to avoid this problem. Even with the timeout as 1 ms it seems to work,
                // but I default to a full second in waitAndExit to be safe
                waitAndExit(1);
            }
        }).on('close', function() {
            // If the pipe is closed by the server (eg. the helper is killed) then we have to reset the
            // stdout write because if we don't and try to write to the pipe it will throw an exception
            // and prevent the app process from closing when postponing for example (window will close
            // but process still running)
            process.stdout.write = stdwrite;
        });
    }
});

// Quit when all windows are closed.
app.on('window-all-closed', function() {
    logger.info("event window-all-closed called");
    clearAppData(() => {
        logger.info("clear cache before quit");
    });
    app.quit();
});

function initApp() {

    const logMessage = "Main: Initiating application";
    logger.info(logMessage);
    var events = requests.addEvent(null, config.ClientEventType.DISCO_APP_INIT, config.ClientEventLevel.INFO, logMessage);

    const area = electron.screen.getPrimaryDisplay().workArea;
    // You need to round the coordinates as passing in floats for either position causes both of them to be ignored
    // and electron to use its default centering. This works fine except for multi-monitor linux where
    // it puts it on the left side of the second monitor, so we unfortunately have to do it manually
    // See: https://github.com/electron/electron/issues/3490
    var xPosition = Math.round(area.width / 2 - config.WINDOW_WIDTH / 2);
    var yPosition = Math.round(
        Math.max(0, area.height - config.WINDOW_HEIGHT - 100) / 2);
    // Offset the position from the work area start coordinates
    xPosition += area.x;
    yPosition += area.y;

    mainWindow = new BrowserWindow({
        "width": config.WINDOW_WIDTH,
        "height": config.WINDOW_HEIGHT,
        "x" : xPosition,
        "y" : yPosition,
        "resizable" : isDeveloperMode,
        "frame": false, //hide window controls
        "show-dev-tools": isDeveloperMode,
        "show": false,
        "minimizable": false,
        "title": "Amazon Connections",
        "icon": path.join(__dirname, 'resources/app-icon.png') // for Linux app icon
    });

    const windowSizeInfo = {
        "width": mainWindow.webContents.browserWindowOptions.width,
        "height": mainWindow.webContents.browserWindowOptions.height,
        "x" : mainWindow.webContents.browserWindowOptions.x,
        "y" : mainWindow.webContents.browserWindowOptions.y,
        "frame" :  mainWindow.webContents.browserWindowOptions.frame,
        isDeveloperMode : isDeveloperMode
    }

    requests.addEvent(events, config.ClientEventType.WINDOW_SIZE, config.ClientEventLevel.INFO, JSON.stringify(windowSizeInfo));
    requests.reportClientEvents(events);

    //Keep window visible on all Spaces
    mainWindow.setVisibleOnAllWorkspaces(true);

    configureTray(mainWindow);
    loadSurveyUI(mainWindow);

    mainWindow.on('closed', function() {
        mainWindow = null;
    });
}

var whenAppReady = new Promise(function(resolve, reject) {
    // App on ready doesn't fire if the whole file has run before the pipe promise is resolved,
    // so we need a promise for it too, see: https://github.com/atom/electron/issues/1726
    app.on('ready', resolve);
});

// Only run the main app once the pipe is ready
whenPipeConnected.then(function() {
    whenAppReady.then(initApp).catch(function(err) {
        handleUncaughtException(err);
    });
}).catch(function(err) {
    // failed to Connect to pipe
    logger.error('Failed to connect to pipe');
    reportErrorAndExit(config.PIPE_CONNECTION_ERROR, err);
});

/**
 * Set up tray for Windows to show balloon for system notification
 */
function configureTray(window) {
    if (onWindows) {
        tray = new Tray(path.join(__dirname, 'resources/tray-icon.png'));
        tray.setToolTip('Amazon Connections');

        ipc.on('display-balloon', function(event, arg) {
            tray.displayBalloon({
                icon: path.join(__dirname, 'resources/app-icon.png'),
                title: arg.title,
                content: arg.options.body
            });
            tray.on('balloon-click', function() {
                // Let renderer know balloon has been clicked
                event.sender.send('balloon-click');
            });
        });
    }
}

/**
 * Load the content of the survey UI
 */
function loadSurveyUI(window) {
    const session = mainWindow.webContents.session;
    //session.webRequest.onHeadersReceived((details, callback) => {
    //    callback({ responseHeaders: Object.assign({
            // TODO: Remove 'unsafe-inline' and 'data:' once new React page is live
            //       New CSP should look like this, without font-src specification needed:
            //           "default-src 'self' *.amazon.com" 
            //       font-src unsafe-inline and data: is included because CDW currently has a base64 encoded font inlined:
            //          https://code.amazon.com/packages/ContentDeliveryWebsiteService/blobs/c94629edc68b6f21eee78b76087ee0b911dd5f14/--/webapp/resources/disco/css/common.css#L4
            //       and default-src 'unsafe-inline' is because CDW has many inlined <style> tags
    //        "Content-Security-Policy": [ "default-src 'self' *.amazon.com 'unsafe-inline'" ],
    //        "Content-Security-Policy": [ "font-src 'self' *.amazon.com 'unsafe-inline' data:" ]
    //    }, details.responseHeaders)});
    //});
    logger.info("Pre Log");
    const logMessage = 'Main: Loading survey UI';
    logger.info(logMessage);
    var events = requests.addEvent(events, config.ClientEventType.LOAD_UI, config.ClientEventLevel.INFO, logMessage);
    logger.info("events added");

    session.allowNTLMCredentialsForDomains(config.ALLOW_NTLM_CREDENTIALS_DOMAINS);
    logger.info("session allow NTLMCreds success");
    const jadeFile = path.join(__dirname, "web-content", "ui-container.jade");
    logger.info("Jade File found");
    fs.readFile(jadeFile, 'utf8', function(err, data) {
        logger.info("JadeFile read");
        if (err) {
            logger.error('Failed to read jade file, error: ' + err + ', exiting.');
            requests.addEvent(events, config.ClientEventType.LOAD_UI_ERROR, config.ClientEventLevel.ERROR, err);
            requests.reportClientEvents(events);
            reportErrorAndExit(config.JADE_ERROR, err);
            return;
        }
        logger.info("after if");
        requests.reportClientEvents(events);
        logger.info("requests populated");
        const options = {
        // --- Options ---
            // Jade options, see: http://jade-lang.com/api/
            pretty: true,
            // filename is needed when using includes such as with style.css
            filename: jadeFile,
        // --- Locals ---
            // Our local variables to be used in the jade template. Unfortunately jade uses the
            // same options object for the locals, so we specify them here
            url: global.domain + config.APP_ROUTE,
            appCss: global.domain + config.APP_CSS_ROUTE
        };
        logger.info("options created");
        // Variables we are passing into jade to use in the markup
        let html =  jade.render(data, options);
        logger.info("html set");
        // Load the html string as if it were a url
        // From: https://github.com/atom/electron/issues/968
        html = 'data:text/html,' + encodeURIComponent(html);
        logger.info("html reset");
        window.loadURL(html);
    });
}

// Disconnect from pipe
app.on('will-quit', function() {
    logger.info("event will-quit called");
    clearAppData(() => {
        logger.info("app cache deleted");
    });

    if (onWindows && hasPipe) {
        pipeClient.end();
    }
});

// Logging
// TODO: Remove the event.returnValue after website changes for making these
//       logging calls to async from sync are deployed. This is only required
//       for sync ipc calls.
ipc.on('error', function(event, message, details) {
    logger.error(message, details || {});
    event.returnValue = 'success';
});

ipc.on('warn', function(event, message, details) {
    logger.warn(message, details || {});
    event.returnValue = 'success';
});

ipc.on('info', function(event, message, details) {
    logger.info(message, details || {});
    event.returnValue = 'success';
});

ipc.on('quit', function(event, arg) {
    waitAndQuit();
});

/**
 * clearAppData - Clear all app data (cookies, cache, etc.)
 *
 * @param  {function} callback Called when operation is done
 */
function clearAppData(callback) {
    const session = mainWindow.webContents.session;

    logger.info("deleting app cache");
    session.clearStorageData({}, () => { // this doesn't clear HTTP session cache
        session.clearCache(() => {
            logger.info("Cleared app data");
            callback();
        });
    });
    logger.info("deleting clientCertificate");
    session.clearAuthCache({type: "clientCertificate"});
    logger.info('deleting app cache done');
}

ipc.on('clear-app-data', function(event, arg) {
    clearAppData(() => {
        event.returnValue = 'success';
    });
});

var crashCallback = function() {
    logger.error('Render health check failed. Exiting...');

    // There are some cases where corrupted JS assets are written to cache
    // resulting in failure to load renderer. Clearing the cache usually
    // resolves this issue and user will receive question after next unlock.
    // See: https://issues.amazon.com/issues/Connections-2657.
    clearAppData(() => {
        const logMessage = "Disco app crashed. No heartbeat. The stage is: " + stage + " domain is: " + global.domain;
        var events = requests.addEvent(null, config.ClientEventType.HEALTH_CHECK_ERROR, config.ClientEventLevel.ERROR, logMessage);
        requests.reportClientEvents(events);


        reportErrorAndExit(config.HEALTH_CHECK_ERROR);
    });
};

app.on('certificate-error', function(event, webContents, url, error, certificate, callback) {

    logger.info("event:");
    logger.error(event);
    logger.info("certificate:");
    logger.error(certificate);
    logger.info("webContents:");
    logger.error(webContents);

    logger.error("Certificate Error, details: { error: { " + error + " }, url: { " + url + " } }");
    if (isDeveloperMode) {
        // Accept all certificates in renderer
        event.preventDefault();
        callback(true);
    }
});

// Close app if we don't hear from renderer process after some time
var safeCloseTimeout = setTimeout(crashCallback, config.HEALTH_CHECK_INITIAL_TIMEOUT);

// Perform health check on a repeated interval
ipc.on('health-check-pulse', function(event, arg) {
    clearTimeout(safeCloseTimeout);
    safeCloseTimeout = setTimeout(crashCallback, config.HEALTH_CHECK_TIMEOUT);
});

// This function is to try and avoid electron getting in a bad state and not properly exiting
// which I have seen when exiting right after a pipe connection failure.
// This is also used to ensure logs get flushed before exiting.
function waitAndExit(errorCode, waitTime) {
    waitAndRun(function() {
        process.exit(errorCode);
    }, waitTime);
}

// app.quit() is different than process.exit(0)
// See: https://github.com/electron/electron/blob/master/docs/api/app.md#appquit
function waitAndQuit(waitTime) {
    waitAndRun(function() {
        app.quit();
    }, waitTime);
}

function waitAndRun(callback, waitTime) {
    // Default value workaround, see: http://stackoverflow.com/a/894877
    waitTime = typeof waitTime !== 'undefined' ? waitTime : 1000;

    setTimeout(function() {
        callback();
    }, waitTime);
}

function reportErrorAndExit(eventName, errorDetails, additionalParams) {
    return requests.reportError(eventName, errorDetails, additionalParams)
        .finally(function() {
            waitAndExit(1);
        });
}

// Close the app if an unknown error occurs
function handleUncaughtException(error) {
    try {
        const hasLogger = typeof logger !== 'undefined' && logger;
        if (hasLogger) {
            logger.error('Main: uncaughtException', { error: error.toString(), stack: error.stack });
        } else {
            process.stderr.write(error.stack + "\n");
            process.stderr.write("Caught an unhandled exception: " + error + ", exiting.\n");
        }
        var events = requests.addEvent(null, config.ClientEventType.DISCO_APP_EXCEPTION, config.ClientEventLevel.ERROR);
        requests.reportClientEvents(events);
        requests.reportError(config.DISCO_APP_EXCEPTION, error)
            .catch(function(err) {
                // TODO Move this logging on report failure into the requests function. This will require
                // passing the logger into the requests module after it is loaded (perhaps with a setter).
                if (hasLogger) {
                    logger.error("ReportEvent of the exception failed.", { error: err.toString(), stack: err.stack });
                }
            })
            .finally(function() {
                waitAndExit(1);
            });
    } catch(e) {
        waitAndExit(1);
    }
}
