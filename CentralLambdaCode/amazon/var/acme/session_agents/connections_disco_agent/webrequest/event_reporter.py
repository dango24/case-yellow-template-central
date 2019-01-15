from xml.etree import ElementTree
from xml.etree.ElementTree import Element, SubElement
from xml.dom import minidom

from service_requester import ServiceRequester, RequestParamEnum

class EventEnum(object):
    ATTEMPTING_DISPLAY_APP_LAUNCH = "DisplayAppLaunchAttempt"
    DISPLAY_APP_SHUTDOWN_NORMAL = "DisplayAppShutdownNormal"
    DISPLAY_APP_SHUTDOWN_UNEXPECTED = "DisplayAppShutdownUnexpected"
    DISPLAY_APP_TIMED_OUT = "DisplayAppTimedOut"
    USER_PREFS_INIT_FAILED = "UserPrefsInitFailed"
    AGENT_NOT_INSTALLED = "AgentNotInstalled"

class EventParamEnum(RequestParamEnum):
    EVENT_NAME = "eventName"
    ADDITIONAL_PARAMS = "additionalParams"

class EventReporter(ServiceRequester):
    _MODULE = __file__

    REQUEST_URI = "events/"

    XML_ROOT = "ReportEventRequest"

    # Not used but shows format of request data
    XML_TEMPLATE = """
        <ReportEventRequest>
            <eventName>{eventName}</eventName>
            <clientIdentity>{clientIdentity}</clientIdentity>
            <hostname>{hostname}</hostname>
            <additionalParams>{additionalParams}</additionalParams>
        </ReportEventRequest>
    """

    def prettify(self, elem):
        """Return a pretty-printed XML string for the Element."""
        rough_string = ElementTree.tostring(elem)
        reparsed = minidom.parseString(rough_string)
        return reparsed.toprettyxml()

    def getXmlObj(self, eventName, additionalParams):
        baseParams = self._getBaseParams()
        eventParams = {EventParamEnum.EVENT_NAME: eventName,
                       EventParamEnum.ADDITIONAL_PARAMS: additionalParams}
        eventParams.update(baseParams)

        root = Element(EventReporter.XML_ROOT)
        for param, val in eventParams.iteritems():
            child = SubElement(root, param)
            child.text = val

        return root

    def reportEventSync(self, eventName, additionalParams=""):
        headers = {"ContentType": "application/xml"}

        root = self.getXmlObj(eventName, additionalParams)
        data = ElementTree.tostring(root)

        def onSuccess(response):
            self.logger.info("Event report succeeded. Event Name: {0}; Additional Params: {1}"
                             .format(eventName, additionalParams))
            return True

        self.logger.info("Submitting event report for: Event Name: {0}; Additional Params: {1}. Raw post xml: {2}"
                         .format(eventName, additionalParams, data))
        result = self.makeServiceCall(onSuccess, data=data, headers=headers, default=False)
        return result

    def reportEvent(self, *args, **kwargs):
        # Report events asynchronously so they don't block (since they are not crucial)
        self._initThread(self.reportEventSync, args, kwargs)
