'use strict';

var app = require('electron').app;
var os = require("os");
var exceptions = require("./exceptions");

const DEFAULT_LANGUAGE = "en_US";
var preferredLanguage = app.getLocale();
if (preferredLanguage === "") {
    preferredLanguage = DEFAULT_LANGUAGE;
}

// Global variables
global.hostname = os.hostname();
global.clientIdentity = "DiscoApp";
global.electronClientIdentity = "DiscoApp_Electron";
global.preferredLanguage = preferredLanguage;
global.clientVersion = app.getVersion();
global.deviceType = "PC";
global.platform = os.platform();

switch (os.platform()) {
    case "darwin":
        global.deviceOS = "OSX " + os.release();
        break;
    case "win32":
        global.deviceOS = "Windows " + os.release();
        break;
    case "linux":
        global.deviceOS = "Linux " + os.release();
        break;
    default:
        global.deviceOS = os.platform() + " " + os.release();
}

// Window config
exports.WINDOW_HEIGHT = 670;
exports.WINDOW_WIDTH = 1000;

// Service configuration
// Note: integ technically is a domain not a stage, beta would be the stage
// TODO Replace "integ" with "beta"
exports.STAGES = {
    ALPHA : "alpha",
    INTEG : "integ",
    GAMMA : "gamma",
    PROD : "prod"
};

exports.ClientEventType = {
    USERNAME_NOT_FOUND : "username_not_found",
    PIPE_CONNECTION_ERROR : "pipe_connected_error",
    DISCO_APP_INIT: "disco_app_init",
    WINDOW_SIZE : "window_size",
    LOAD_UI : "load_ui",
    LOAD_UI_ERROR : "load_ui_error",
    HEALTH_CHECK_ERROR : "health_check_error",
    DISCO_APP_EXCEPTION : "disco_app_exception"
}

exports.ClientEventLevel = {
    TRACE : "trace",
    INFO : "info",
    WARN : "warn",
    ERROR : "error",
    FATAL : "fatal"
}

exports.APP_ROUTE = "s/survey-app?v=2";
exports.APP_CSS_ROUTE = "resources/disco/css/app.css";

exports.WEBSITE_DOMAINS = {
    // If your dev-desktop is not like this, manually changed it before test
    [exports.STAGES.ALPHA]  : "https://" + os.userInfo().username + ".aka.corp.amazon.com:8243/",
    [exports.STAGES.INTEG]  : "https://cdws-sln-pdx-d.integ.amazon.com/",
    [exports.STAGES.GAMMA] : "https://connections-cdw-gamma.aka.amazon.com/",
    [exports.STAGES.PROD]  : "https://connections-cdw-prod.aka.amazon.com/"
};
exports.VALID_STAGES_LIST = Object.keys(exports.WEBSITE_DOMAINS);

exports.SERVICE_DOMAINS = {
    [exports.STAGES.ALPHA] : "https://expresssurveyservice.integ.amazon.com/",
    [exports.STAGES.INTEG] : "https://expresssurveyservice.integ.amazon.com/",
    [exports.STAGES.GAMMA] : "https://expresssurveyservice-gamma-iad.amazon.com/",
    [exports.STAGES.PROD] : "https://expresssurveyservice-iad.amazon.com/"
};

exports.CLIENTMETRICS_DOMAIN = {
    [exports.STAGES.ALPHA] : "http://ccsm-zqf-pdx-d.integ.amazon.com/",
    [exports.STAGES.INTEG] : "http://ccsm-zqf-pdx-d.integ.amazon.com/",
    [exports.STAGES.GAMMA] : "https://ccsm-zqf-primary-na-pp-iad.iad.proxy.amazon.com/",
    [exports.STAGES.PROD] : "https://ccsm-zqf-primary-na-p-iad.iad.proxy.amazon.com/"
};

/*
 These are the domains for which ntlm/kerberos authentication is allowed.
 We can't just use global.domain for the connections website as it redirects to sentry.amazon.com if you do not have an
 active cookie. Could probably just allow them globally with *, but restricting to amazon domains doesn't hurt.
 See: https://electron.atom.io/docs/api/session/#sesallowntlmcredentialsfordomainsdomains
*/
exports.ALLOW_NTLM_CREDENTIALS_DOMAINS = "*.amazon.com";

// Health check config
exports.HEALTH_CHECK_INITIAL_TIMEOUT = 90000;
exports.HEALTH_CHECK_TIMEOUT = 5000;
exports.DISCO_APP_EXCEPTION = "DiscoAppException";
exports.HEALTH_CHECK_ERROR = "DiscoAppException:HealthCheckError";
exports.PIPE_CONNECTION_ERROR = "DiscoAppException:PipeConnectionError";
exports.JADE_ERROR = "DiscoAppException:JadeFileError";

var instanceConfig = {
    username: null,
    stage: null,
};

exports.getUsername = function() {
    return instanceConfig.username;
};
exports.setUsername = function(username) {
    instanceConfig.username = username;
};

exports.getStage = function() {
    return instanceConfig.stage;
};
exports.setStage = function(stage) {
    if (!exports.WEBSITE_DOMAINS.hasOwnProperty(stage)) {
        throw new exceptions.IllegalArgumentException("Invalid stage passed: " + stage + ", should be one of: "
            + exports.VALID_STAGES_LIST.join(', '));
    }
    instanceConfig.stage = stage;
};
