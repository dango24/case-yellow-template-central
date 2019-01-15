var requestPromise = require('request-promise');
var config = require('./config');
var os = require('os');

// add event to events list. para1, para2, para3, para4 must be a string
exports.addEvent = function(events, eventType, eventLevel, eventDetail, para1, para2, para3, para4) {
    if (!events) {
        events = [];
    }

    var time = new Date();
    var utcTimeInMs = getUTCTime(time)
    var localTimeInMs = getLocalTime(time, utcTimeInMs);
    var event = {
        localTime: Math.round(localTimeInMs / 1000),
        utcTime: Math.round(utcTimeInMs / 1000),
        type: eventType,
        level: eventLevel,
        description: eventDetail,
        parameter1: para1 || "",
        parameter2: para2 || "",
        parameter3: para3 || "",
        parameter4: para4 || ""
    }
    events.push(event);
    return events;
}

// call CCMS report user events
exports.reportClientEvents = function(events) {
    var stage = config.getStage();
    var username = config.getUsername();
    // Disco App set username before set stage, so we have to set stage as default value here.
    if (!username) {
        username = "NotFound";
        stage = config.STAGES.ALPHA;
    }
    if (!stage) {
        throw new Error("Stage was null or empty, cannot proceed with event report."
            + "Stage: " + stage);
    }
    var domain = config.CLIENTMETRICS_DOMAIN[stage];
    var api = "events";
    var url = domain + api;

    var params = {
        employeeLogon: username,
        clientIdentity : global.electronClientIdentity,
        clientDevice: global.deviceOS,
        clientVersion : global.clientVersion,
        hostname : os.hostname(),
        events: events
    };

    var options = {
        method: 'POST',
        uri: url,
        headers: {"Content-Type" : "application/json"},
        body: params,
        json : true,
        rejectUnauthorized: false
    };
    return requestPromise(options);
}

exports.reportEvent = function(eventName, additionalParams, errorDetails) {
    var stage = config.getStage();
    var username = config.getUsername();
    if (!stage || !username) {
        throw new Error("Stage or username was null or empty, cannot proceed with event report."
            + "Stage: " + stage + " . Username: " + username);
    }
    var domain = config.SERVICE_DOMAINS[stage];
    var api = "events/";
    var url = domain + api + username;
    additionalParams = additionalParams || "";
    errorDetails = errorDetails || "";
    var params = {
        eventName : eventName,
        clientIdentity : global.clientIdentity,
        hostname : os.hostname(),
        clientVersion : global.clientVersion,
        deviceType : global.deviceType,
        deviceOS : global.deviceOS,
        additionalParams : additionalParams.toString(),
        errorDetails : errorDetails.toString()
    };
    var options = {
        method: 'POST',
        uri: url,
        headers: {"Content-Type" : "application/json"},
        body: params,
        json : true,
        rejectUnauthorized: false
    };
    return requestPromise(options);
};

// Simply calls reportEvent with the order of errorDetails and additionalParams swapped.
exports.reportError = function(eventName, errorDetails, additionalParams) {
    return exports.reportEvent(eventName, additionalParams, errorDetails);
};

//.getTime() method would return us a UTC time, so we have to have more logical here to do
// the tranfer between UTC and local time
function getLocalTime(time, utc) {
     var offset = time.getTimezoneOffset();
     var localTimeInMs = utc + (-offset * 60 * 1000);
     return localTimeInMs;
}

function getUTCTime(time) {
    var utcTimeInMs = time.getTime();
    return utcTimeInMs;
}
