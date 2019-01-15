#!/usr/bin/python
# -*-coding :utf-8-*-

import os
import sys
import webbrowser
import logging
logging.basicConfig(filename=os.path.join(os.path.expanduser("~"), ".acme/state/popup.log"), format='%(asctime)-15s %(message)s')

sys.path.insert(0,"/System/Library/Frameworks/Python.framework/Versions/2.7/Extras/lib/python")
sys.path.insert(0,"/System/Library/Frameworks/Python.framework/Versions/2.7/Extras/lib/python/PyObjC")
sys.path.insert(0,"/usr/local/amazon/lib")


import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk, GdkPixbuf, GObject

sys.path.append("/usr/local/amazon/var/acme/session_agents/")
from amazon_enterprise_access_agent import AmazonEnterpriseAccessAgent

class AEAExtensionUbuntuNotificationWindow(Gtk.Window):
    GOOGLE_CHROME = "google-chrome"
    FIREFOX = "firefox"
    CHROMIUM = "chromium-browser"

    def __init__(self):
        Gtk.Window.__init__(self, title="Amazon Enterprise Access (AEA)")
        self.layout = Gtk.Layout()
        
        self.installed_browsers = []
        self.decision = True
        self.most_recent_profile_chrome = ""
        self.most_recent_profile_firefox = ""
	self.most_recent_profile_chromium = ""
        self.agent = AmazonEnterpriseAccessAgent()
        self.update()
        self.setup_ui()

    def update(self):
        """
        Method to update extension status
        """
        self.installed_browsers = []
        self.agent.load_settings(os.path.join("/usr/local/amazon/var/acme/manifests/session_agents/",
                                              self.agent.identifier,".json"))
        self.agent.read_aea_config_file()
        self.agent.get_popup_preference()

        if self.agent.check_if_chrome_installed():
            self.installed_browsers.append(self.GOOGLE_CHROME)
        if self.agent.check_if_firefox_installed():
            self.installed_browsers.append(self.FIREFOX)
	if self.agent.check_if_chromium_installed():
	    self.installed_browsers.append(self.CHROMIUM)
        self.most_recent_profile_chrome = self.agent.get_most_recent_profile_path_chrome(self.agent.chrome_state_file_path)
        self.most_recent_profile_firefox = self.agent.get_most_recent_profile_path_firefox(self.agent.firefox_profiles_file_path)
	self.most_recent_profile_chromium = self.agent.get_most_recent_profile_path_chromium(self.agent.chromium_state_file_path)

    def setup_ui(self):
        """
        Method to instantiate UI
        """
        self.set_resizable(False)
        self.move(150, 150)
        self.connect("destroy", lambda q: Gtk.main_quit())
        self.add(self.layout)
        self.setup_introduction_part(self.layout)
        self.setup_chrome_part(self.layout)
        self.setup_firefox_part(self.layout)
	self.setup_chromium_part(self.layout)

    def update_ui(self):
        """
        Method to update UI, called by GTK Timer
        Must return True for GTK Timer
        """
        self.update()
        self.update_chrome_part(self.layout)
        self.update_firefox_part(self.layout)
	self.update_chromium_part(self.layout)

        return True

    def setup_chrome_part(self, layout):
        """
        Method to instantiate Chrome UI section
        """
        installed_status = self.agent.check_chrome_extension_installation(
            self.agent.chrome_state_file_path,
            self.agent.chrome_ext_id)

        self.label_chrome_status = Gtk.Label()

        self.button_chrome = Gtk.Button()
        self.button_chrome.connect("clicked", self.chrome_clicked)
        self.button_chrome.set_size_request(160, 15)

        self.chrome_check_button = Gtk.CheckButton(label="Install automatically")
        self.chrome_check_button.connect("toggled", self.toggled_chrome)

        self.label_chrome = Gtk.Label()
        self.label_chrome.set_markup('<b>Chrome</b>')

        if self.agent.aea_chrome_ext_silent_install_user_enabled:
            self.chrome_check_button.set_active(True)
        else:
            self.chrome_check_button.set_active(False)

        layout.put(self.chrome_check_button, 50, 120)
        layout.put(self.button_chrome, 450, 115)
        layout.put(self.label_chrome_status, 55, 100)
        layout.put(self.label_chrome, 40, 80)
                
    def update_chrome_part(self, layout):
        """
        Method to update Chrome UI section
        """
        installed_status = self.agent.check_chrome_extension_installation(
            self.agent.chrome_state_file_path,
            self.agent.chrome_ext_id)

        if self.GOOGLE_CHROME in self.installed_browsers:
            if installed_status:
                self.label_chrome_status.set_markup("AEA Extension is installed")
                button_label_chrome = "Verify"
            else:
                self.label_chrome_status.set_markup("AEA Extension is not installed")
                button_label_chrome = "Install"

            if self.agent.aea_chrome_ext_silent_install_user_enabled:
                self.chrome_check_button.set_active(True)
            else:
                self.chrome_check_button.set_active(False)

            self.button_chrome.set_label(button_label_chrome)
            self.button_chrome.show()
            self.chrome_check_button.show()
        else:
            self.label_chrome_status.set_markup("<span color='red'>Chrome is not installed</span>")
            self.button_chrome.hide()
            self.chrome_check_button.hide()

    def setup_chromium_part(self, layout):
        """
        Method to instantiate Chromium UI section
        """
        installed_status = self.agent.check_chromium_extension_installation(
            self.agent.chromium_state_file_path,
            self.agent.chromium_ext_id)

        self.label_chromium_status = Gtk.Label()

        self.button_chromium = Gtk.Button()
        self.button_chromium.connect("clicked", self.chromium_clicked)
        self.button_chromium.set_size_request(160, 15)

        self.chromium_check_button = Gtk.CheckButton(label="Install automatically")
        self.chromium_check_button.connect("toggled", self.toggled_chromium)

        self.label_chromium = Gtk.Label()
        self.label_chromium.set_markup('<b>Chromium</b>')

        if self.agent.aea_chromium_ext_silent_install_user_enabled:
            self.chromium_check_button.set_active(True)
        else:
            self.chromium_check_button.set_active(False)

        layout.put(self.chromium_check_button, 50, 200)
        layout.put(self.button_chromium, 450, 195)
        layout.put(self.label_chromium_status, 55, 180)
        layout.put(self.label_chromium, 40, 160)
                
    def update_chromium_part(self, layout):
        """
        Method to update Chromium UI section
        """
        installed_status = self.agent.check_chromium_extension_installation(
            self.agent.chromium_state_file_path,
            self.agent.chromium_ext_id)

        if self.CHROMIUM in self.installed_browsers:
            if installed_status:
                self.label_chromium_status.set_markup("AEA Extension is installed")
                button_label_chromium = "Verify"
            else:
                self.label_chromium_status.set_markup("AEA Extension is not installed")
                button_label_chromium = "Install"

            if self.agent.aea_chromium_ext_silent_install_user_enabled:
                self.chromium_check_button.set_active(True)
            else:
                self.chromium_check_button.set_active(False)

            self.button_chromium.set_label(button_label_chromium)
            self.button_chromium.show()
            self.chromium_check_button.show()
        else:
            self.label_chromium_status.set_markup("<span color='red'>Chromium is not installed</span>")
            self.button_chromium.hide()
            self.chromium_check_button.hide()

    def setup_firefox_part(self, layout):
        """
        Method to instantiate Firefox UI section
        """
        installed_status = self.agent.check_firefox_extension_installation(
            self.agent.firefox_profiles_file_path,
            self.agent.firefox_ext_id)

        self.label_firefox_status = Gtk.Label()

        self.button_firefox = Gtk.Button()
        self.button_firefox.connect("clicked", self.firefox_clicked)
        self.button_firefox.set_size_request(160, 15)

        self.firefox_check_button = Gtk.CheckButton(label="Install automatically")
        self.firefox_check_button.connect("toggled", self.toggled_firefox)

        self.label_firefox = Gtk.Label()
        self.label_firefox.set_markup('<b>Firefox</b>')

        if self.agent.aea_firefox_ext_silent_install_user_enabled:
            self.firefox_check_button.set_active(True)
        else:
            self.firefox_check_button.set_active(False)

        layout.put(self.firefox_check_button, 50, 280)
        layout.put(self.button_firefox, 450, 275)
        layout.put(self.label_firefox_status, 55, 260)
        layout.put(self.label_firefox, 40, 240)
    
    def update_firefox_part(self, layout):
        """
        Method to update Firefox UI section
        """
        installed_status = self.agent.check_firefox_extension_installation(
            self.agent.firefox_profiles_file_path,
            self.agent.firefox_ext_id)

        if installed_status:
		self.label_firefox_status.set_markup("AEA Extension is installed")
		button_label_firefox = "Verify"
        else:
		self.label_firefox_status.set_markup("AEA Extension is not installed")
		button_label_firefox = "Install"

        if self.agent.aea_firefox_ext_silent_install_user_enabled:
            self.firefox_check_button.set_active(True)
        else:
            self.firefox_check_button.set_active(False)

        if self.FIREFOX in self.installed_browsers:
            self.button_firefox.set_label(button_label_firefox)
            self.button_firefox.show()
            self.firefox_check_button.show()
        else:
            self.label_firefox_status.set_markup("<span color='red'>Firefox is not installed</span>")
            self.button_firefox.hide()
            self.firefox_check_button.hide()

    def setup_introduction_part(self, layout):
        """
        Method to instantiate Intro UI section
        """
        self.button_close = Gtk.Button(label="CLOSE")
        self.button_close.connect("clicked", Gtk.main_quit)
        self.label_begin = Gtk.Label()
        self.label_begin.set_markup("""<b>You can verify the health and enable or disable automatic installation of the AEA\nbrowser extensions from here.</b>""")
        self.browser_restart_label = Gtk.Label()
        self.browser_restart_label.set_markup("<span color='red'>A browser restart may be required for changes to take effect.</span>")
        layout.put(self.browser_restart_label, 40, 310)
        self.browser_restart_label.hide()
        layout.put(self.button_close, 603, 365)
        self.set_size_request(690, 415)
        layout.put(self.label_begin, 40, 20)

    def toggled_firefox(self, button):
        """
        Method to toggle Firefox auto-install, called by Firefox checkbox
        """
        if button.get_active():
            self.agent.configure_firefox_silent_installation(enable = True)
            self.agent.aea_firefox_ext_silent_install_user_enabled = True
        else:
            self.agent.configure_firefox_silent_installation(enable = False)
            self.agent.aea_firefox_ext_silent_install_user_enabled = False

        self.browser_restart_label.show()
        self.agent.update_popup_preference(self.decision, self.most_recent_profile_chrome, self.most_recent_profile_firefox, self.most_recent_profile_chromium)
        self.update_ui()

    def toggled_chrome(self, button):
        """
        Method to toggle Chrome auto-install, called by Chrome checkbox
        """
        if button.get_active():
            self.agent.configure_chrome_silent_installation(enable = True)
            self.agent.aea_chrome_ext_silent_install_user_enabled = True
        else:
            self.agent.configure_chrome_silent_installation(enable = False)
            self.agent.aea_chrome_ext_silent_install_user_enabled = False

        self.browser_restart_label.show()
        self.agent.update_popup_preference(self.decision, self.most_recent_profile_chrome, self.most_recent_profile_firefox, self.most_recent_profile_chromium)
        self.update_ui()

    def toggled_chromium(self, button):
        """
        Method to toggle Chromium auto-install, called by Chromium checkbox
        """
        if button.get_active():
            self.agent.configure_chromium_silent_installation(enable = True)
            self.agent.aea_chromium_ext_silent_install_user_enabled = True
        else:
            self.agent.configure_chromium_silent_installation(enable = False)
            self.agent.aea_chromium_ext_silent_install_user_enabled = False

        self.browser_restart_label.show()
        self.agent.update_popup_preference(self.decision, self.most_recent_profile_chrome, self.most_recent_profile_firefox, self.most_recent_profile_chromium)
        self.update_ui()

    def chrome_clicked(self, button):
        """
        Method to open Chrome, called by Chrome button
        """
        webbrowser.get(self.GOOGLE_CHROME).open(self.agent.landing_page_url)
        self.update_ui()
    
    def chromium_clicked(self, button):
        """
        Method to open Chromium, called by Chromium button
        """
        webbrowser.get(self.CHROMIUM).open(self.agent.landing_page_url)
        self.update_ui()

    def firefox_clicked(self, button):
        """
        Method to open Chrome, called by Chrome button
        """
        webbrowser.get(self.FIREFOX).open(self.agent.landing_page_url)

    def set_timer(self):
        """
        GTK Timer Method, calls update_ui()
        """
        GObject.timeout_add(3*1000, self.update_ui)

if __name__ == "__main__":
    win = AEAExtensionUbuntuNotificationWindow()
    win.show_all()
    win.browser_restart_label.hide()
    win.update_ui()
    win.set_timer()
    Gtk.main()
    while Gtk.events_pending(): 
        Gtk.main_iteration_do(True)

