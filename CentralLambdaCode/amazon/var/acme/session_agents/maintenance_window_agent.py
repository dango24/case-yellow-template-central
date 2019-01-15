import acme.agent as agent
import datetime
import logging
import pykarl.event
import acme
import os
import plistlib
import subprocess

class MaintenanceWindowAgent(agent.BaseAgent):

    def __init__(self, *args, **kwargs):
        """
        PyACME Agent which will handle the client side of ACME task:
         1.Send metric for maintenance window notification events
         2.Send metric for maintenance window enable/unable events
        """

        logger = logging.getLogger(self.logger_name)
        
        self.identifier = "MaintenanceWindowAgent"
        self.name = "MaintenanceWindowAgent"
        self.main_win_notification_event_type = "MaintenanceWindowUINotificationEvent"
        self.main_win_event_type = "MaintenanceWindowEvent"
        self.run_frequency = datetime.timedelta(minutes=60)
        self.run_frequency_skew = datetime.timedelta(minutes=10)
        self.triggers = agent.AGENT_TRIGGER_SCHEDULED

        self.maintenance_window_file_path = None
        self.current_user_home_dir = os.path.expanduser("~")

        self.maintenance_window_notification_shown = False
        self.maintenance_window_notification_clicked_learn_more = False
        self.show_maintenance_window_notification = False
        self.click_mw_learn_more = False

        self.maintenance_window_status = False
        self.maintenance_window_enabled = False

        self.state_keys.append("show_maintenance_window_notification")
        self.state_keys.append("click_mw_learn_more")
        self.state_keys.append("maintenance_window_status")

        # Define default value
        p = acme.platform.lower()
        if p == "os x" or p == "macos":
            self.platform = "macOS"
            self.maintenance_window_file_path = os.path.join(self.current_user_home_dir,
                                                       "Library/Preferences/com.amazon.acme.updates.plist")
        else:
            logger.warning("Platform: {}  is not supported".format(p))
            pass

        super(MaintenanceWindowAgent, self).__init__(name=self.name,
                                                identifier=self.identifier,
                                                *args, **kwargs)

    def execute(self, trigger=None, *args, **kwargs):
        """
        Our primary execution method. This method will be called by
        our scheduler or during events as registered by our triggers.
        """

        logger = logging.getLogger(self.logger_name)

        logger.info("{} Executing!".format(self.identifier))

        self.load_settings()  # this needs to be called every time the agent run
        self.load_state(self.state_path)
        self.read_data_file()

        enabled_status = True  # track status of maintenance window event,if it doesn't require to send event, status is true
        notification_status = True  #track status of notification event,if it doesn't require to send event, status is true

        if self.maintenance_window_enabled != self.maintenance_window_status:  # check whether the previous state and current status is different, if it is, it means to send event.
            evt = pykarl.event.Event(type=self.main_win_event_type, subject_area="ACME")
            dispatcher = pykarl.event.dispatcher
            if dispatcher.is_configured():
                evt.payload["platform"] = self.platform
                evt.payload["is_mw_enabled"] = self.maintenance_window_enabled
                evt.payload["mw_changed_date"] = datetime.datetime.strftime(datetime.datetime.utcnow(),'%Y-%m-%d %H:%M:%S')
                dispatcher.dispatch(evt)
                self.maintenance_window_status = self.maintenance_window_enabled
            else:
                enabled_status = False
                logger.error("Cannot send maintenance window event: KARL dispatcher is not configured!")

        if self.show_maintenance_window_notification and self.click_mw_learn_more:
            if enabled_status:
                self.last_execution_status = agent.AGENT_EXECUTION_STATUS_SUCCESS
            else:
                self.last_execution_status = agent.AGENT_EXECUTION_STATUS_ERROR
            logger.info("End execution because of sending notification event already.")
            return

        if (not self.show_maintenance_window_notification and self.maintenance_window_notification_shown) or (not self.click_mw_learn_more and self.maintenance_window_notification_clicked_learn_more): # check whether the notification or click more event has already sent.
            evt = pykarl.event.Event(type=self.main_win_notification_event_type, subject_area="ACME")
            dispatcher = pykarl.event.dispatcher
            if dispatcher.is_configured():
                evt.payload["platform"] = self.platform
                if not self.show_maintenance_window_notification:
                    evt.payload["notification_showed"] = True
                    self.show_maintenance_window_notification = True
                if not self.click_mw_learn_more and self.maintenance_window_notification_clicked_learn_more:
                    evt.payload["notification_showed"] = True
                    evt.payload["mw_showed"] = True
                    self.click_mw_learn_more = True
                dispatcher.dispatch(evt)
            else:
                notification_status = False
                logger.error("Cannot send maintenance window notification event: KARL dispatcher is not configured!")

        if enabled_status and notification_status:
            self.last_execution_status = agent.AGENT_EXECUTION_STATUS_SUCCESS
        else:
            self.last_execution_status = agent.AGENT_EXECUTION_STATUS_ERROR

        logger.info("{} Finished Executing!".format(self.identifier))

    def read_data_file(self):
        """
        Method to read ACME plist file and set agent variables
        """

        logger = logging.getLogger(self.logger_name)

        cmd = ["/usr/bin/plutil", "-convert", "xml1", "-o", "-", self.maintenance_window_file_path]

        try:
            file_string = subprocess.check_output(cmd)
            data = plistlib.readPlistFromString(file_string)
            try:
                self.maintenance_window_notification_shown = data["maintenanceWindowNotificationShown"]
                self.maintenance_window_notification_clicked_learn_more = data["maintenanceWindowNotificationClickedLearnMore"]
                self.maintenance_window_enabled = data["maintenanceWindowEnabled"]
            except KeyError as e:
                raise Exception("Expected key: {} missing from ACME plist file: {}.".format(e,self.maintenance_window_file_path))
        except Exception as e:
            raise Exception("Error happen when loading data from plist file: {}. ERROR: {}".format(self.maintenance_window_file_path, e))
