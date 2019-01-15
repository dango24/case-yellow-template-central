
"""
.. package:: acme
    :synopsis: Top level package containing all ACME modules. 
    :platform: OSX, Ubuntu

.. moduleauthor:: Beau Hunter <beauhunt@amazon.com>


"""

import argparse
import calendar
import datetime
import logging
import json
import os
import random
import re
import sys
import threading
import systemprofile
import cStringIO

__version__ = "1.4.22"

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

#MARK: Module defaults -
platform = systemprofile.current_platform()
if platform == "OS X" or platform == "macOS":
    BASE_DIR="/usr/local/amazon/var/acme"
elif platform == "Ubuntu":
    BASE_DIR="/usr/local/amazon/var/acme"
elif platform == "RedHat":
    BASE_DIR="/usr/local/amazon/var/acme"
else:
    raise UnsupportedPlatformError("Platform:{} is not supported by acme".format(platform))

#MARK: Classes -
class Enum(object):
    '''
    Abstract class which provides scaffolding for an enum-like interface. This
    class is not directly usable, but is meant to be subclassed by custom
    enum implementations.
        
    .. example:
        
        >>> class MyCustomEnum(Enum):
        ...    
        ...    NONE = 0
        ...    SUCCESS = 1
        ...    FAILED = 2
        ...
        >>> MyCustomEnum.NONE
        0
        >>> MyCustomEnum.to_string(3)
        "SUCCESS, FAILED"
        >>> MyCustomEnum.from_string("SUCCESS, FAILED")
        3
    
    '''
    
    @classmethod
    def to_string(cls, enum, proper_case=False):
        """
        Method which will return a string representation of the provided
        value, comma delimited.
        
        :param int enum: Our numeric enum value
        
        :returns string:
        
        .. example:
            
            >>> ResultStatus.to_string(3)
            'FLAG01, FLAG10'
            
            >>> ResultStatus.to_string(3, true)
            'Flag01, Flag10'

        """
        
        result = ""
        
        convert_proper = lambda x: x if len(x) <= 1 else "{}{}".format(
                                                x[:1].upper(),
                                                x[1:].lower())
        
        ## Build list of enum values, ordered by value
        attribs = set()
        for k, v in cls.__dict__.iteritems():
            try:
                if not k.startswith("_"):
                    if v == enum:
                        attribs.add(k)
                        break
                    elif v and v & enum == v:
                        attribs.add(k)
            except TypeError:
                pass
        
        if proper_case:
            mylist = map(lambda x: convert_proper(x), 
                            sorted(attribs, key=cls.__dict__.__getitem__))
        else:
            mylist = sorted(attribs, key=cls.__dict__.__getitem__)
        
        return ", ".join(mylist)
    
    @classmethod
    def from_string(cls, str):
        """
        Method which will return an integer representation of the provided 
        value.
        
        :param string str: String representation of our enum value
        
        .. example:
            
            >>> ResultStatus.from_string("EXISTED_DEVICE, EXISTED_INSTANCE")
            1088
        
        :returns int:
        
        """
        
        result = None
        
        ## Build list of enum values, ordered by value
        attribs = [entry.strip().lower() for entry in str.split(',')]
        
        for k, v in cls.__dict__.iteritems():
            if k.lower() in attribs:
                if result is None:
                    result = v
                else:
                    result |= v
        
        return result
        
class DataFormatter(object):
    """
    Class which provides data formatting and conversion.
    """
    
    timedelta_regex = None
    format = None
    
    @classmethod
    def convert_to_date(cls, value, format=None):
        """
        Method to convert the provided value to a :py:class:`datetime.datetime`
        object.
        
        :raises ValueError: If we cannot convert the value
        """
        the_date = None
        
        if isinstance(value, datetime.datetime):
            the_date = value
        
        ## Try epoch formatting
        if value is not None and the_date is None:
            if format is None or format == "epoch" or format == "%s":
                try:
                    the_date = datetime.datetime.utcfromtimestamp(float(value))
                except (TypeError,ValueError) as exp:
                    pass
                    
        ## If we don't have a date yet, try various date formatting
        if value is not None and the_date is None:
            formats = [DATE_FORMAT,"%Y-%m-%d %H:%M:%S",
                                        "%Y-%m-%d %H:%M:%S.%f",
                                        "%Y-%m-%dT%H:%M:%S",
                                        "%Y-%m-%dT%H:%M:%S%Z",
                                        "%Y-%m-%dT%H:%M:%S%z",
                                        "%Y-%m-%dT%H:%M:%SZ",
                                        "%Y-%m-%dT%H:%M:%S.%f",
                                        "%Y-%m-%dT%H:%M:%S.%f%Z",
                                        "%Y-%m-%dT%H:%M:%S.%f%z"]
            
            if cls.format is not None:
                formats.insert(0, cls.format)
            
            if format is not None:
                formats.insert(0, format)
            
            for format in formats:
                try:
                    the_date = datetime.datetime.strptime(value,format)
                    break
                except (TypeError,ValueError) as exp:
                    pass
            
        ## If we don't have a date yet, try common AWS format
        if value is not None and the_date is None:
            try:
                the_date = datetime.datetime.strptime(value[0:19],
                                                "%Y-%m-%d %H:%M:%S")
                if value[19]=='+':
                    the_date -= datetime.timedelta(
                                            hours=int(value[20:22]),
                                            minutes=int(value[23:]))
                elif value[19]=='-':
                    the_date += datetime.timedelta(
                                            hours=int(value[20:22]),
                                            minutes=int(value[23:]))
            except (TypeError, ValueError, IndexError) as exp:
                pass
            
        ## If we still don't have a date, throw an exception
        if value is not None and not the_date:  
            raise ValueError("Could not convert value:'{}' to date!".format(value))
        
        return the_date
    
    @classmethod
    def convert_date(cls, value, format=None):
        """
        Method to convert the provided date value to a string.
        """
        
        date_string = None
        
        if format is None:
            format = DATE_FORMAT
        
        the_date = cls.convert_to_date(value)
        
        if the_date:
            if (format.lower() == "timestamp" or format.lower() == "%s" 
                                                or format.lower() == "epoch"):
                date_string = calendar.timegm(the_date.timetuple())
            else:
                date_string = the_date.strftime(format)
        
        return date_string
    
    @classmethod
    def convert_to_timedelta(cls, value):
        """
        Method to convert the provided value to a timedelta object.
        
        """
        
        timedelta = None
        
        for i in xrange(0,1):
            if value is None:
                break
            elif isinstance(value,datetime.timedelta):
                timedelta = value
                break
            
            ## Check for float (seconds)
            try:
                timedelta = datetime.timedelta(seconds=float(value))
                break
            except (TypeError,ValueError):
                pass
            
            ## Check for 8601
            if not cls.timedelta_regex:
                pattern = "^P((?P<year>\d*?)Y)?((?P<month>\d*?)M)?((?P<week>\d*?)W)?((?P<day>\d*?)D)?(T((?P<hour>\d*)H)?((?P<minute>\d*)M)?((?P<second>.*)S)?)?"
                cls.timedelta_regex = re.compile(pattern)
                
            try:
                ## Extract the date portion
                result = cls.timedelta_regex.search(value)
                d = result.groupdict()
                
                weeks = None
                days = None
                hours = None
                minutes = None
                seconds = None
                
                if d["year"]:
                    days = (float(d["year"]) * 365)
                
                if d["month"]:
                    weeks = (float(d["month"]) * 4.34)
                
                if d["week"]:
                    if weeks is None:
                        weeks = 0
                    weeks += float(d["week"])
                
                if d["day"]:
                    if days is None:
                        days = 0
                    days += float(d["day"])
                
                if d["hour"]:
                    hours = float(d["hour"])
                
                if d["minute"]:
                    minutes = float(d["minute"])
                
                if d["second"]:
                    seconds = float(d["second"])
                    
                kwargs = {}
                if weeks is not None:
                    kwargs["weeks"] = weeks
                if days is not None:
                    kwargs["days"] = days
                if hours is not None:
                    kwargs["hours"] = hours
                if minutes is not None:
                    kwargs["minutes"] = minutes
                if seconds is not None:
                    kwargs["seconds"] = seconds
                
                timedelta = datetime.timedelta(**kwargs)
                
            except Exception as exp:
                raise
        
        if value and timedelta is None:
            raise ValueError("Failed to convert value:{} to a timedelta object!".format(value))
        
        return timedelta 
    
    @classmethod
    def convert_timedelta(cls, value, format=None):
        """
        Method to convert our timedelta object.
        """
        
        td_string = None
        
        the_td = cls.convert_to_timedelta(value)
        
        if value is not None:
            if format is None or format.lower().startswith("float"):
                td_string = the_td.total_seconds()
            
            if format and (format == "8601" or format.lower().startswith("iso")):
                td_string = "P"
                seconds = the_td.total_seconds()
                minutes, seconds = divmod(seconds, 60)
                hours, minutes = divmod(minutes, 60)
                days, hours = divmod(hours, 24)
                years, days = divmod(days,365)
                years, days, hours, minutes = map(int,(years,days,hours,minutes))
                seconds = round(seconds, 6)
                
                if years:
                    td_string += "{}Y".format(years)
                if days:
                    td_string += "{}D".format(days)
                
                if hours or minutes or seconds:
                    td_string += "T"
                if hours:
                    td_string += "{}H".format(hours)
                if minutes:
                    td_string += "{}M".format(minutes)
                if seconds:
                    td_string += "{}S".format(round(seconds,6))
        
        return td_string

class SerializedObject(object):
    """
    Class which provides serialization capabilities for an object.
    """
    
    class_lock = threading.RLock()
    mapped_classes = {}
    
    key_map = {}
    logger_name = "SerializedObject"
    data_formatter = None
    property_map_regex = None
    
    def __init__(self,key_map=None,dict_data=None,json_data=None):
        
        if key_map is None:
            self.key_map = self.__class__.key_map.copy()
        else:
            self.key_map = key_map
        
        if dict_data:
            self.load_dict(dict_data)
        
        if json_data:
            self.load_from_json(json_data)
    
    @classmethod
    def map_class(cls, key, class_ref):
        """
        Method to map the provided class to the provided key. Once mapped,
        the provided key can be used in key map definitions for object
        instantiation and population.
        """
        
        with cls.class_lock:
            cls.mapped_classes[key] = class_ref
    
    def parse_attribute_string(self,attribute_string):
        """
        Method which will parse a provided attribute string
        into a dictionary, keyed by parts.
        
        .. example:
            >>> self.parse_attribute_string("<format=datetime>;my_attr")
            { "property_name" : "my_attr",
                "format" : "datetime"
            }
           
        """
        
        logger = logging.getLogger(self.logger_name)
        
        metadata = {}
        
        if self.property_map_regex is None:
            self.property_map_regex = re.compile("(<(?P<md>.*)>;?)?(?P<name>.*)")
        
        property_metadata = None
        
        if attribute_string:        
            prop_result = self.property_map_regex.search(attribute_string)
            if prop_result is not None:
                property_metadata = prop_result.groupdict()["md"]
                name = prop_result.groupdict()["name"]
                if name:
                    metadata["property_name"] = name
            
            if property_metadata:
                for entry in property_metadata.split(","):
                    entry_set = entry.split("=")
                    try:
                        metadata[entry_set[0]] = entry_set[1]
                    except IndexError:
                        logger.warning("Invalid metadata entry:{}".format(entry))
                    
        return metadata
    
    def value_for_key(self, key):
        """
        Method which will return the value for the provided key.
        
        :param string key: The Key to lookup
        
        :raises KeyError: If key does not exist in our key_map
        
        .. note:
            Key lookup is case sensative.
        
        """
        
        ## If our key is not defined, raise a KeyError
        if not key in self.key_map.keys():
            raise KeyError(key)
        
        key_map = self.key_map_for_keys(keys=[key])
        
        return self.value_for_attribute_map(key_map.items()[0])
    
    def value_for_attribute_map(self,attribute_map):
        """
        Method which will interpret the provided mapping and output
        the resulting value.
        
        :Example:
            >>> a = SerializedObjectSubclass()
            >>> a.the_date = datetime.datetime.strptime("2015-01-01 00:00:00",acme.DATE_FORMAT)
            >>> a.value_for_attribute_map(("date","<type=datetime,format=epoch>;date"))
            1420070400
            >>> a.value_for_attribute_map(("date","<type=datetime,format=%Y>;date"))
            2015
            
        .. warning:
            Avoid using (,), (;) or (< >) in custom date format strings, 
            establish custom getters/setters if you intend to do this.
        
        """
        
        logger = logging.getLogger(self.logger_name)
        
        key,property = attribute_map
        
        property_name = None
        property_value = None
        property_value_transmog = None
        
        metadata = self.parse_attribute_string(property)
                
        if "property_name" in metadata and metadata["property_name"]:
            property_name = metadata["property_name"]
        else:
            property_name = key
                
        if "getter" in metadata:
            logger.log(1,"Key:'{}' Property:'{}' using getter:'{}'".format(key,property_name,metadata["getter"]))
            try:
                getter = getattr(self,metadata["getter"])
                property_value = getter()
            except AttributeError:
                pass
        else:
            try:
                property_value = getattr(self,property_name)
            except AttributeError:
                pass
                
        property_value_transmog = property_value
        
        ## Apply transforms
        format = None
        if "format" in metadata:
            format = metadata["format"]
        
        if "type" in metadata:
            if self.data_formatter is None:
                self.data_formatter = DataFormatter()
            
            df = self.data_formatter
            
            if metadata["type"] == "datetime":
                logger.log(1,"Performing transmog for key:'{}' property:'{}' type:'{}' format:'{}'".format(
                                                    key, 
                                                    property_name,
                                                    metadata["type"],format))
                try:
                    if property_value is not None:
                        property_value_transmog = df.convert_date(
                                                        value=property_value,
                                                        format=format)
                except Exception as exp:
                    raise TypeConversionError(key=key, value=property_value,
                                                    datatype=metadata["type"])
                    
            elif metadata["type"] == "timedelta":
                logger.log(1,"Performing transmog for key:'{}' property:'{}' type:'{}' format:'{}'".format(
                                                            key, 
                                                            property_name,
                                                            metadata["type"],
                                                            format))
                try:
                    if property_value is not None:
                        property_value_transmog = df.convert_timedelta(
                                                        value=property_value,
                                                        format=format)
                except Exception as exp:
                    raise TypeConversionError(key=key, value=property_value,
                                                    datatype=metadata["type"])
            elif metadata["type"] == "object":
                logger.log(1,"Performing transmog for key:'{}' property:'{}' type:'{}' format:'{}'".format(
                                                            key,
                                                            property_name,
                                                            metadata["type"],
                                                            format))
                if format and format == "output_null":
                    output_null = True
                else:
                    output_null = False
                
                if property_value:
                    property_value_transmog = property_value.to_dict(
                                                    output_null=output_null)
                    if not property_value_transmog:
                        property_value_transmog = None
            else:
                pass
                ##logger.log(2,"No transmog necessary for property:'{}' type:'{}'".format(property_name,metadata["type"]))
        
        return property_value_transmog

    def set_value_for_key(self, key, value):
        """
        Method which will return the value for the provided key.
        
        :param string key: The Key to lookup
        :param value: The value to set
                
        .. note:
            Key lookup is case sensative. If the provided key does 
            not exist in our key map, we'll create a default key_map 
            entry for it
        
        """
        
        ## If our key is not defined in our key_map, create it
        if not key in self.key_map.keys():
            self.key_map[key] = None
        
        key_map = self.key_map_for_keys(keys=[key])
        
        return self.set_value_for_attribute_map(value, key_map.items()[0])
    
    def set_value_for_attribute_map(self, value, attribute_map,
                                                        overwrite_null=True):
        """
        Method which will interpret the provided mapping and update
        the value for the prescribed attribute.
        
        :param value: The new value to set
        :type value: Varies depending on attribute
        :param attribute_map: Key, map entry for our attribute
        :type attribute_map: tuple (key, map)
        :param bool overwrite_null: If True, we will overwrite populated values
            with 'None', if passed value is 'None' (default True)
        
        :Example:
            >>> a = SerializedObjectSubclass()
            >>> a.the_date = datetime.datetime.utcnow()
            >>> a.value_
        
        """
        
        logger = logging.getLogger(self.logger_name)
        
        key, property = attribute_map
        
        property_name = None
        property_value = value
                
        metadata = self.parse_attribute_string(property)
        
        if "property_name" in metadata and metadata["property_name"]:
            property_name = metadata["property_name"]
        else:
            property_name = key
                
        ## Apply transforms
        format = None
        if "format" in metadata:
            format = metadata["format"]
        
        if "type" in metadata:
            if self.data_formatter is None:
                self.data_formatter = DataFormatter()
            
            df = self.data_formatter
            
            if metadata["type"] == "datetime":
                logger.log(1,"Performing transmog for key:'{}' property:'{}' type:'{}' format:'{}'".format(key,property_name,metadata["type"],format))
                try:
                    if value is not None:
                        property_value = df.convert_to_date(value=value, 
                                                                format=format)
                except Exception as exp:
                    raise TypeConversionError(key=key,value=value,datatype=metadata["type"])
                    
            elif metadata["type"] == "timedelta":
                logger.log(1,"Performing transmog for key:'{}' property:'{}' type:'{}' format:'{}'".format(key,property_name,metadata["type"],format))
                try:
                    if value is not None:
                        property_value = df.convert_to_timedelta(value=value)
                except Exception as exp:
                    raise TypeConversionError(key=key,value=value,datatype=metadata["type"])
            
            elif metadata["type"] == "object":
                
                the_class = None
                class_name = None
                try:
                    class_name = metadata["class"]
                    the_class = self.mapped_classes[class_name]
                except KeyError:
                    if class_name:
                        message = ("Failed to map value to type:'{}' for "
                            "key:'{}', class:'{}' is not mapped!".format(
                                                            metadata["type"],
                                                            key,
                                                            class_name))
                    else:
                        message = ("Failed to map value to datatype:'{}' for "
                            "key:'{}', no mapping class was provided!".format(
                                                            metadata["type"],
                                                            key))
                    
                    raise TypeConversionError(message=message,
                                                key=key, 
                                                value=property_value, 
                                                datatype=metadata["type"])
                
                ## Here if we've resolved a mapped class, check for our property
                ## If the property does not exist, instantiate it
                try:
                    property_value = getattr(self, property_name)
                    if property_value is None:
                        raise AttributeError()
                except AttributeError:
                    property_value = the_class()
                
                if format and format == "overwrite_null":
                    if isinstance(value, the_class):
                        property_value = value
                    else:
                        dict_data = value
                        if isinstance(value, basestr):
                            dict_data = json.loads(value)
                        
                        property_value.load_dict(dict_data, overwrite_null=True)
                else:
                    if isinstance(value, the_class):
                        property_value.load_dict(value.to_dict())
                    else:
                        dict_data = value
                        if isinstance(value, basestring):
                            dict_data = json.loads(value)
                        property_value.load_dict(dict_data)
            else:
                pass
                ##logger.log(2,"No transmog necessary for property:'{}' type:'{}'".format(property_name,metadata["type"]))
        
        if property_value is not None or overwrite_null:
            if "setter" in metadata:
                logger.log(1,"Key:'{}' Property:'{}' using setter:'{}'".format(
                                        key,property_name,metadata["setter"]))
                setter = getattr(self,metadata["setter"])
                setter(property_value)
            else:
                logger.log(1,"Key:'{}' Property:'{}' using local attribute:'{}'".format(
                                        key,property_name,property_name))
                try:
                    setattr(self,property_name,property_value)
                except AttributeError as exp:
                    raise AttributeError("Failed to set property:'{}'; {}".format(
                                        property_name, exp.message)), None, sys.exc_info()[2]
        
    def key_map_for_keys(self, keys, key_map=None, add_unknown=True):
        """
        Method which takes a list of keys and returns a dictionary of 
        mapped values. 
        
        :param list<string> keys: A list of keys to lookup            
        :param dict key_map: Dictionary of key->attribute mappings
                which represent local properties used in our output
        :param add_unknown: If true, keys requested that don't exist in the map will be added (default:true)
        :type add_unknown: (bool)
        
        :returns: dictionary of keys and mappings
        
        .. note:
            Keys are not case sensative. If a prescribed key does not
            exist in our key map, it will still be returned in our results.
            
            
        """
        
        if key_map is None:
            key_map = self.key_map
        
        my_key_map = {}
        
        for key in keys:
            found_key = False
            for mapped_key in key_map.keys():
                if key.lower() == mapped_key.lower():
                    my_key_map[mapped_key] = key_map[mapped_key]
                    found_key = True
            if not found_key and add_unknown:
                my_key_map[key] = None
                    
        return my_key_map
        
    def to_dict(self,key_map=None,output_null=True):
        """
        Method to export our record in key=>value dictionary form,
        as prescribed by our key_map
        
        :param dict key_map: Dictionary of key->attribute mappings
                which represent local properties used in our output
        :param bool output_null: If False, we will omit attributes with value
            of 'None' (default: True)
        """
        
        if key_map is None:
            key_map = self.key_map
        
        my_dict = {}
        
        logger = logging.getLogger(self.logger_name)
        
        for key,property in key_map.iteritems():
            try:
                value = self.value_for_attribute_map((key,property))
            except TypeConversionError as exp:
                logger.warning("An error occurred exporting value: {}".format(exp))
                logger.log(5,"Failure stack trace (handled cleanly):",
                                                                exc_info=1)                
                value = None
            
            except Exception as exp:
                logger.warning("Failed to map value for key:{} property:{}. Error:{}".format(
                                                    key,property,exp.message))
                logger.log(5,"Failure stack trace (handled cleanly):",
                                                                exc_info=1)                
                value = None
            
            if value is not None or output_null:
                my_dict[key] = value
                
        return my_dict
            
    def load_dict(self, data, key_map=None, overwrite_null=True, 
                                                    raise_on_error=None):
        """
        Method to load data from a dictionary.
        
        :param dict data: Dictionary of key->values to load
        :param dict key_map: Dictionary of key->attribute mappings
                which represent local properties used in our output
        :param bool overwrite_null: If True, we will overwrite populated values
            with 'None', if passed value is 'None' (default True)
        :param bool raise_on_error: If true, we will raise an ObjectLoadError
                    if any errors are encountered. This Exception is thrown
                    at the end of the process, such that any error loading one
                    key will not prevent additional keys from failing.
        
        :returns: (bool) True on successful load
        """
        
        logger = logging.getLogger(self.logger_name)
        
        if key_map is None:
            payload_map = self.key_map
        else:
            payload_map = key_map
        
        missing_keys = data.keys()
        secondary_matches = {}
        
        failures = []
        
        for key,attribute in payload_map.iteritems():
            for import_key in data.keys():
                if import_key.lower() == key.lower():
                    try:
                        self.set_value_for_attribute_map(data[import_key],
                                            (key,attribute),
                                            overwrite_null=overwrite_null)
                    except TypeConversionError as exp:
                        failures.append(exp)
                        logger.warning("An error occurred loading value: {}".format(exp))
                        logger.log(5,"Failure stack trace (handled cleanly):",
                                                                exc_info=1)
                    missing_keys.remove(import_key)
                    break
                else:
                    map = self.parse_attribute_string(attribute)
                    if ("property_name" in map and map["property_name"].lower() 
                                                    == import_key.lower()):
                        secondary_matches[import_key] = (key,attribute)
                    
        for missing_key in missing_keys:
            if missing_key in secondary_matches:
                try:
                    self.set_value_for_attribute_map(data[missing_key],
                                            secondary_matches[missing_key],
                                            overwrite_null=overwrite_null)
                except TypeConversionError as exp:
                    failures.append(exp)
                    logger.warning("An error occurred: {}".format(exp))
                    logger.log(5,"Failure stack trace (handled cleanly):",
                                                                exc_info=1)
        
        if not failures:
            return True
        elif failures and raise_on_error:
            raise ObjectLoadError(failures=failures)
        else:
            return False
    
                
    def to_json(self,key_map=None,pretty=True,output_null=True):
        """
        Method which returns our object serialized in JSON format
        
        :param dict key_map: Dictionary of key->attribute mappings
                which represent local properties used in our output
        :param bool output_null: If False, we will omit attributes with value
            of 'None' (default: True)

        """
        
        data = self.to_dict(key_map=key_map,output_null=output_null)
        
        if pretty:
            json_string = "{}\n" .format(json.dumps(data,indent=4))
        else:
            json_string = json.dumps(data)
        
        return json_string
    
    def load_from_json(self,json_data,key_map=None,overwrite_null=True):
        """
        Method which loads our object based on the provided JSON string.
        
        :param str json_data: Our json string to load.
        :param dict key_map: Dictionary of key->attribute mappings
                which represent local properties used in our output
        :param bool overwrite_null: If True, we will overwrite populated values
            with 'None', if passed value is 'None' (default True)
        """
        
        data = json.loads(json_data)
        
        self.load_dict(data=data,key_map=key_map,overwrite_null=overwrite_null)
             
    def load_from_file(self,filepath,key_map=None,overwrite_null=True):
        """
        Method to load settings from the provided filepath. We expect this
        file to contain JSON data.
        
        :param str filepath: The path to the file to load
        :param dict key_map: Dictionary of key->attribute mappings
                which represent local properties used in our output
        :param bool overwrite_null: If True, we will overwrite populated values
            with 'None', if passed value is 'None' (default True)
        
        :raises IOError,OSError: If filesystem problems are encountered
        """
        logger = logging.getLogger(self.logger_name)
        logger.log(2,"Loading from file:{}".format(filepath))
        
        with open(filepath,"r") as fh:
            str_data = fh.read()
            
        self.load_from_json(json_data=str_data,key_map=key_map,
                                                overwrite_null=overwrite_null)
                
    def save_to_file(self,filepath,key_map=None,output_null=True):
        """
        Method to save our current settings to the provided filepath. Data
        saved will be in JSON format.
        
        :param str filepath: The path to the file to load
        :param dict key_map: Dictionary of key->attribute mappings
                which represent local properties used in our output
        
        :raises IOError,OSError: If filesystem problems are encountered
        """
        logger = logging.getLogger(self.logger_name)
        logger.log(2,"Saving state to file:{}".format(filepath))
        
        logger = logging.getLogger(self.logger_name)
        
        logger.log(2,"Getting JSON")
        json_string = self.to_json(key_map=key_map,output_null=output_null)
        
        logger.log(2,"Opening File")
        with open(filepath,"w") as fh:
            logger.log(2,"Writing File...")
            fh.write(json_string)
        
        logger.log(2,"Finished.")
    
    def copy(self, key_map=None):
        """
        Method to return a shallow copy of our object.
        
        :param key_map: Dictionary that controls which data is copied. This is
                used solely for data copy, it does not define the key_map of 
                the newly created object.
        :type key_map: Dictionary<string,string>
        
        .. note:
            
        
        """
        
        data = self.to_dict(key_map=key_map)
        
        new_copy = self.__class__(dict_data=data, key_map=self.key_map)        
        return new_copy
        
    def deepcopy(self, key_map=None):
        """
        Method to provide a deep copy of our object.
        
        :param key_map: Dictionary that controls which data is copied. This is
                used solely for data copy, it does not define the key_map of 
                the newly created object.
        :type key_map: Dictionary<string,string>
        
        """
        
        json_data = self.to_json(key_map=key_map)
        
        new_copy = self.__class__(json_data=json_data, key_map=self.key_map)
        
        return new_copy
    
    def __copy__(self):
        """
        Method to conform to copy.copy protocol by providing a shallow
        copy. 
        """
        
        return self.copy()
        
    def __deepcopy__(self):
        """
        Method to conform to copy.copy protocol by providing a deep
        copy. 
        """
        
        return self.deepcopy()
        
    def __getitem__(self, key):
        """
        Method to support named-based geting (i.e. value = self["key"])
        """
        return self.value_for_key(key)
    
    def __setitem__(self, key, item):
        """
        Method to support named-based setting (i.e. self["key"] = value
        """
        self.set_value_for_key(key, item)
        
    def __contains__(self, key):
        """
        Method to support contains lookups (i.e. "key" in self)
        """
        
        return key in self.key_map.keys()
        
    def __iter__(self):
        """
        Method to support iteration
        """
        
        return self.to_dict().__iter__()
        
    def iteritems(self):
        """
        Method to support dict-style key,value iteration
        """
        
        return self.to_dict().iteritems()
    
        
    
class ConfigurableObject(SerializedObject):
    """
    Class which represents a serialied object which supports modification
    via configuration files
    """
    
    settings_keys = []              #: Default settings keys for our class
    
    def __init__(self, settings_keys=None, settings_filepath=None, 
                                                            *args, 
                                                            **kwargs):
        
        if settings_keys is None:
            settings_keys = self.__class__.settings_keys[:]
        
        self.settings_keys = settings_keys
        self.settings_filepath = settings_filepath
            
        super(ConfigurableObject, self).__init__(*args, **kwargs)
    
    def load_settings(self, filepath=None):
        """
        Method to load our settings from a JSON file.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        result = False
        if not filepath and self.settings_filepath:
            filepath = self.settings_filepath
         
        if filepath and os.path.isfile(filepath):
            logger.debug("Loading settings from file:{}".format(
                                                filepath))
            key_map = self.key_map_for_keys(self.settings_keys)
            self.load_from_file(key_map=key_map,filepath=filepath,
                                                    overwrite_null=True)
            result = True
        return result
    
    def save_settings(self,filepath=None):
        """
        Method used to save object settings to a JSON file.         
        """
        
        logger = logging.getLogger(self.logger_name)
        
        if filepath is None:
            filepath = self.settings_filepath
        
        if filepath:
            logger.debug("Saving settings to file:'{}'".format(
                                                filepath))
            key_map = self.key_map_for_keys(self.settings_keys)
            self.save_to_file(filepath=filepath,key_map=key_map,
                                                        output_null=False)
            result = True
        
        return result
                                                        
    def try_save_settings(self, filepath=None):
        """
        Method which will attempt to save our settings to disk, failing 
        gracefully if necessary.
        
        :param string filepath: The path to the file to save settings.
                
        :returns: (bool) True on successful save    
                
        """
        
        logger = logging.getLogger(self.logger_name)
        
        result = False
        try:
            result = self.save_settings(filepath=filepath)
        except Exception as exp:
            logger.warning("Failed to save settings to path:'{}'. Error: {}".format(
                                                        filepath,
                                                        exp.message))    
        return result

    def try_load_settings(self, filepath=None):
        """
        Method which will attempt to load our settings from disk, failing 
        gracefully if necessary.
        
       :param string filepath: The path to the file from which to load settings.
                
        :returns: (bool) True on successful load
                
        """
        
        logger = logging.getLogger(self.logger_name)
        
        result = False
        try:
            result = self.load_settings(filepath=filepath)
        except Exception as exp:
            logger.warning("Failed to load settings from path:'{}'. Error: {}".format(
                                                        filepath,
                                                        exp.message)) 
        return result

class PersistentObject(SerializedObject):
    """
    Class which represents a serialied object which contains interfaces for
    state persistence.
    """
    
    state_keys = []            #: Default state keys for our class
    
    def __init__(self, state_keys=None, state_filepath=None, 
                                                            *args, 
                                                            **kwargs):
        if state_keys is None:
            state_keys = self.__class__.state_keys[:]
        
        self.state_keys = state_keys
        self.state_filepath = state_filepath
            
        super(PersistentObject, self).__init__(*args, **kwargs)
        
    def load_state(self, filepath=None):
        """
        Method to load state from the provided filepath.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        result = False
        if filepath is None:
            filepath = self.state_filepath
        
        if filepath and os.path.isfile(filepath):
            logger.log(9, "Loading state from file:{}".format(
                                                filepath))
            key_map = self.key_map_for_keys(self.state_keys)
            self.load_from_file(key_map=key_map,filepath=filepath,
                                                    overwrite_null=False)
            result = True
        
        return result
    
    def save_state(self, filepath=None):
        """
        Method used to save state to the provided filepath.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        result = False
        
        if filepath is None:
            filepath = self.state_filepath
        
        if filepath:
            logger.debug("Saving state to file:'{}'".format(filepath))
            key_map = self.key_map_for_keys(self.state_keys)
            self.save_to_file(filepath=filepath,key_map=key_map,
                                                        output_null=False)
            result = True
            
        return result
                                                        
    def try_save_state(self, filepath=None):
        """
        Method which will attempt to save our state to disk, failing gracefully
        if necessary.
        
        :param string filepath: The path to the file to save our state.
                
        :returns: (bool) True on successful save
        
        """
        
        logger = logging.getLogger(self.logger_name)
        
        result = False
        try:
            result = self.save_state(filepath=filepath)
        except Exception as exp:
            logger.warning("Failed to save state to path:'{}'. Error: {}".format(
                                                        filepath,
                                                        exp.message))
        return result
    
    
    def try_load_state(self, filepath=None):
        """
        Method which will attempt to load our state from disk, failing gracefully
        if necessary.
        
        :param string filepath: The path to the file from which to load our state.
                
        :returns: (bool) True on successful load
                
        """
        
        logger = logging.getLogger(self.logger_name)
        
        result = False
        try:
            result = self.load_state(filepath=filepath)
        except Exception as exp:
            logger.warning("Failed to load state from path:'{}'. Error: {}".format(
                                                        filepath,
                                                        exp.message))    
        return result
            
class RecurringTimer(SerializedObject):
    """
    Class which provides a recurring timer function with customizable skewing,
    retry, and backoff behavior. 
    """
    
    name = "RecurringTimer"
    key_map = { "name": None,
                "frequency": "<type=timedelta>;",
                "retry_frequency": "<type=timedelta>;",
                "retry_exponent": None,
                "max_retry_frequency": "<type=timedelta>;",
                "skew": "<type=timedelta>;"
            }
    
    
    def __init__(self, frequency, handler, name=None, *args, **kwargs):
        """
        Our constructor.
        
        :param frequency: Frequency, in seconds that we will fire
        :type frequency: float
        :param handler: Our method to execute on our timer.
        :type handler: Method reference
        
        .. note:
            This class is a functional wrapper arround :py:class:`threading.Timer`
            Additional passed args (*args) and named parameters (**kwargs) will 
            be passed to the underlying `threading.Timer` instance.
        
        """
        
        self.frequency = frequency
        self.handler = handler
        
        if name:
            self.name = name
        else:
            try:
                self.name = handler.__name__
            except Exception:
                pass
        
        self.retry_frequency = None  #: If retry_frequency is defined and our 
                                #: handler fails (throws an exception), we
                                #: will re-attempt with this frequency.
        self.retry_exponent_base = 2
        self.max_retry_frequency = None       #: Our maximum interval we will
                                #: retry after multiple failures
        
        self.skew = None        #: The skew to add to our timer. The provided skew will 
                                #: result in our trigger executing with increased or 
                                #: decresaed frequency, based on a random draw
                                #: can be float or :py:class:`datetime.timedelta` object
        self.use_zero_offset_skew = None #: Specifies whether we use a zero-offset 
                                        #: When applying our skew.
        
        self.args = args
        self.kwargs = kwargs
        
        self._timer = None       
        self._should_run = False
        self._ctl_lock = threading.RLock()
        
        self._num_consecutive_failures = 0
        
        SerializedObject.__init__(self)
    
    def _handle_function(self, *args, **kwargs):
        """
        Our timer callback, we call our handler and then reset our timer.
        This method should not be called directly.
        """
        
        logger = logging.getLogger(__name__)
        
        logger.log(9, "Timer:'{}' is executing!".format(self.name))
        
        with self._ctl_lock:
            if self._should_run and self._timer:
                self._timer.cancel()
            elif not self._should_run:
                return
        
        reset_time = None
        try:
            self.handler(*args, **kwargs)
            self._num_consecutive_failures = 0
        except DeferredTimerException as exp:
            reset_time = exp.frequency
            logger.warning(exp.message)
        except Exception as exp:
            logger.error("Timer:'{}' failed to execute!. {}".format(self.name,
                                                                exp.message))
            logger.log(5,"Failure stack trace (handled cleanly)",exc_info=1)
            self._num_consecutive_failures += 1
        
        with self._ctl_lock:
            logger.log(9, "Timer:'{}' finished executing.".format(
                                                        self.name))
            self.reset(frequency=reset_time)
    
    def get_frequency(self):
        """
        Method which will return our next execution frequency, adjusted
        for skew and/or backoff, where appropriate.
        
        :returns: :py:class:`datetime.timedelta` object
        :returns: None - if no frequency is set
        
        """
        
        frequency = None
        ## Calculate for backoff
        if self._num_consecutive_failures and self.retry_frequency:
            frequency = datetime.timedelta(
                            seconds=self.retry_frequency.total_seconds() 
                                * (self.retry_exponent_base ** (self._num_consecutive_failures-1)))
            if self.max_retry_frequency:
                max = DataFormatter.convert_to_timedelta(self.max_retry_frequency)
                if frequency > max:
                    frequency = max
                
        elif self.frequency: 
            frequency = DataFormatter.convert_to_timedelta(self.frequency)
            skew = self.roll_skew(zero_offset=self.use_zero_offset_skew)
            frequency = frequency + skew
        
        return frequency
    
    def _setup_timer(self, frequency=None):
        """
        Method to instantiate a new timer instance, accounting for our skew.
        This method should not be called directly.
        
        :param frequency: Frequency, in seconds that we will fire next. This 
                            is a one-time setting. After this, we will resume
                            our normal interval/skew
        :type frequency: (float) seconds
        :type frequency: :py:class:`datetime.timedelta` object
        
        :returns: :py:class:`threading.Timer` instance
        """
        
        logger = logging.getLogger(__name__)
        
        ## If a frequency was provided, capture it so we don't apply skew
        if frequency is not None:
            frequency = DataFormatter.convert_timedelta(frequency,
                                                                format="float")
        else:
            frequency = self.get_frequency().total_seconds()
        
        ## Error handling to verify we don't have a negative frequency
        if frequency < 0:
            if self.retry_frequency:
                fallback_frequency = self["retry_frequency"]
            else:
                fallback_frequency = self["frequency"]
            
            logger.error("RecurringTimer:'{}' was setup with negative frequency, reverting to {}".format(
                                            self.name,
                                            fallback_frequency))
            frequency = fallback_frequency
        
        logger.log(9, "Timer:'{}' updating with frequency:{} sec.".format(
                                                        self.name,
                                                        frequency))
        timer = threading.Timer(frequency,self._handle_function,
                                                            *self.args, 
                                                            **self.kwargs)
        timer.daemon = True
        
        return timer
    
    def roll_skew(self, zero_offset=None, max_skew=None):
        """
        Method which can be ran to determine a new skew. 
        
        :params zero_offset: If true, our returned skew will be a positive
                        number between 0 and max_skew. If false, our skew
                        can be negative or positive.
        :type zero_offset: (bool)
        :params max_skew: Our maximum skew to use.
        :type max_skew: :py:class:`datetime.timedelta`
        :type max_skew: float (seconds)
        
        .. example:
            >>> t = RecurringTimer()
            >>> assert abs(t.roll_skew(max_skew=1)) <= .5
            >>> assert t.roll_skew(max_skew=1, zero_offset=True) <= 1
        
        .. note: 
            The timers configured execution frequency determines the upper
            boundary of our possible skew: the provided skew cannot exceed 
            and will thereby be constrained by the timers configured
            execution frequency. 
           
        :returns: :py:class:`datetime.timedelta` object
        """
        
        logger = logging.getLogger(__name__)
        
        result = datetime.timedelta(seconds=0)
        
        if not self.skew:
            return result
        
        if max_skew is None:
            skew = self["skew"]
        else:
            skew = acme.core.DataFormatter.convert_timedelta(
                                            max_skew, 
                                            format="float")
        
        if zero_offset is None:
            zero_offset = self.use_zero_offset_skew
        
        ## Verify that our skew does not exceed our frequency:
        frequency = self["frequency"]
        
        if skew > frequency:
            logger.warning("Timer skew:'{}sec' exceeds its configured frequency:'{}sec', using skew:'{}sec' ".format(
                                            skew,
                                            frequency,
                                            frequency))
            skew = frequency
        
        ## Check for integer skew, translate to milliseconds for better
        ## accuracy.
        max_ms = None
        try:
            max_ms = abs(skew * 1000.0)
        except (AttributeError, TypeError):
            pass
        
        if max_ms is None:
            logger.warning("Failed to set skew, could not interpret configured skew value:'{}'".format(self.name, self.retry_skew))
            return result
        
        try:
            rand_num = float(random.randint(0, int(max_ms)) / 1000.0)
            if not zero_offset:
                rand_skew = rand_num - (max_ms / 2000.0)
            else:
                rand_skew = rand_num
            result = datetime.timedelta(seconds=rand_skew)
        except Exception as exp:
            logger.warning("Failed to determine skew. Error:{}".format(self.handler, exp))
            logger.log(5,"Failure stack trace (handled cleanly)", exc_info=1)
        
        return result
        
    def reset(self, frequency=None):
        """
        Method which will reset our timer, accounting for our configured skew.
        
        :param frequency: Frequency, in seconds that we will fire next. This 
                            is a one-time setting. After this, we will resume
                            our normal interval/skew
        :type frequency: (float) seconds
        :type frequency: :py:class:`datetime.timedelta` object
        
        """
        
        with self._ctl_lock:
            if self._should_run:
                self._timer.cancel()
                self._timer = self._setup_timer(frequency=frequency)
                self._timer.start()
    
    def start(self, frequency=None):
        """
        Start our timer.
        
        :param frequency: Frequency, in seconds that we will initially fire. 
                            This is a one-time setting. After this, we will 
                            resume our normal interval/skew
        
        :raises RuntimeError: If we attempt to start twice, or if illegal 
                        values are provided.
        
        """
        
        with self._ctl_lock:
            ## Mimic standard timer behavior
            if self._should_run:
                raise RuntimeError("Threads can only be started once (use reset()?)")
            
            self._should_run = True
            self._timer = self._setup_timer(frequency=frequency)
            self._timer.start()
    
    def cancel(self):
        """
        Stop our timer
        """
        
        with self._ctl_lock:
            self._should_run = False
            if self._timer:
                self._timer.cancel()
                self._timer = None

class LogQueueHandler(logging.StreamHandler):
    """
    Custom logging stream handler to emit log entries over a queue.
    This is a poor-man's replacement to the 
    :py:class:`logging.handlers.QueueHandler` class introduced in Python 3
    """
    
    queue = None   #: :py:class:`multiprocessing.Queue` object to pass records to
    
    def __init__(self,queue=None,*args,**kwargs):
        """
        :param queue: Queue to log to
        :type queue: :py:class:`multiprocessing.Queue`
        """
        
        if queue:
            self.queue = queue
        
        logging.StreamHandler.__init__(self,*args,**kwargs)
        
    def emit(self, record):
        """
        Method to emit our log entry to our queue object.
        """
        try:
            if record.exc_info:
                record.exc_text = self.format_exception(record.exc_info)
                record.exc_info = None
            
            if self.queue:
                self.queue.put(record)
            
            self.flush()
        except:
            self.handleError(record)
    
    def format_exception(self, exc_info):
    
        ## Import traceback here, as this will very rarely be called
        import traceback
    
        s_io = cStringIO.StringIO()
        traceback.print_exception(exc_info[0], exc_info[1], exc_info[2], None, s_io)
        
        my_string = s_io.getvalue()
        s_io.close()
        
        if my_string[-1] == "\n":
            my_string = my_string[:-1]
        
        return my_string
                            
#MARK: Exceptions -
class SubSystemUnavailableError(Exception):
    """
    Thrown in the event that prerequisites for a given subsystem are unavailable.
    """
    pass

class ConfigurationError(Exception):
    """
    Thrown in the event that there are configuration problems.
    """
    pass

class UnsupportedPlatformError(Exception):
    """
    Exception thrown if we hit an unexpected platform.
    """
    pass

class ObjectLoadError(Exception):
    """
    Exception thrown if our object fails to load.
    """
    
    def __init__(self, message=None, failures=None, *args, **kwargs):

        self.failures = failures
        
        if message is None and self.failures:
            failed_keys = [error.key for error in self.failures]
            message = "Failed to load object, {} keys failed to load ('{}')".format(
                            len(failed_keys),
                            "', '".join(failed_keys))
        elif message is None:
            message = "Failed to load object!"
            
        super(ObjectLoadError, self).__init__(message, *args, **kwargs)
    

class TypeConversionError(Exception):
    """
    Exception thrown in the event that a piece of data cannot be converted.
    """
    def __init__(self, message=None, key=None, value=None, datatype=None, 
                                                            *args, 
                                                            **kwargs):

        self.key = key
        self.value = value
        self.datatype = datatype
        
        if message is None:
            message = "Failed to map value:'{}' to datatype:'{}' for key:'{}'".format(value,datatype,key)
        
        super(TypeConversionError, self).__init__(message, *args, **kwargs)

class DeferredTimerException(Exception):
    """
    Exception which is thrown when a timer should be deferred.
    """
    
    def __init__(self, message=None, frequency=None, *args, **kwargs):
        """
        :param string message: Our exception message
        :param float frequency: The deferral time
        """
        
        self.frequency = frequency
        if message is None:
            message = "Timer deffered for '{}' seconds!".format(frequency)
        
        super(DeferredTimerException, self).__init__(message, *args, **kwargs)


if __name__ == "__main__":
    cli = ACMECLI(arguments = sys.argv[1:])
    sys.exit(cli.run())

