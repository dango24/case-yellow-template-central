
import logging
import datetime

class EventRouter(object):
    """
    Class which helps route events based on the route mappings.
    """
    route_mappings = None  #: Dictionary which has mappings of sink and target based on subject area and event type
    default_route = None   #: Dictionary containing default route eg: {'default_sink':'kinesis','default_target':'standard.test'}
    publisher_map = {}   #: Dictionary containing mapping between sink and client
    logger_name = None              #: Name used by class logger objects
    default_transport = "json"       #: default_transport method, in case we dont find transport_mech for an event.
    
    #MARK: Constructors
    def __init__(self, route_mappings=None, default_route=None, publisher_map=None, *args, **kwargs):
        
        """
        Our Constructor.
        
        :param dict route_mappings: route_mappings defining the sink and target based on subject_area and event_type
        :param dict default_route: default_route defining the default sink and target. 
        :param dict publisher_map: Key mapping dictionary used for serialization
        
        """
        self.route_mappings = route_mappings
        self.default_route = default_route
        self.publisher_map = publisher_map
        self.logger_name = "KARL.EventRouter"
        
    def route_event(self, event):
        """
        Method to post our event to different services like Kinesis, SQS, SNS. This is a syncronous operation
        and is likely to throw an Exception. This method is generally for 
        internal use, new events should always be submitted through
        commit_event().
        
        :param event: The event to post.
        :type event: :py:class:`Event`
        
        """
        route = None
        publisher = None
        logger = logging.getLogger(self.logger_name)
        transport_mech = None
        
        try:
            route = self.get_route(event)
            
            transport_mech = route.get("transport_mech", self.default_transport)      #: If transport mechanism is not found then returns the default_transport mechanism
                
            publisher = self.publisher_map[route.get("sink")]
            publisher.publish_event(event, route.get("target"), transport_mech)
            logger.debug("Successfully committed KARL event:'{0}' with type:'{1}' with subject area:'{2}' date:'{3}' to stream:'{4}' with format:'{5}'".format(
                                            event.uuid,
                                            event.type, 
                                            event.subject_area,
                                            event.date,
                                            route.get("target"),
                                            transport_mech)) 
            
        except Exception as e:
            logger.error("Failed to route event, Error:{}".format(e))   
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
    def get_route(self, event):
        """
        Method which returns the route based on subject_area or event_type or returns default route.
        """
        
        route = None
        
        if self.route_mappings.get("event_types"):
            route = self.route_mappings.get("event_types").get(event.type)
        
        if route is None and self.route_mappings.get("subject_areas"):
            route = self.route_mappings.get("subject_areas").get(event.subject_area)
        
        if route is None and self.default_route:
            route = self.default_route
        elif route is None:
            route =  {"sink": "Kinesis", "target": "standard"}
        
        return route
        
class KinesisPublisher(object):
    """
    Class which helps to post event to Kinesis stream.
    """
    kinesis_client = None
    JWT         = None   #: transport mechanism is jwt, constant
    #MARK: Constructors
    def __init__(self, kinesis_client=None, identity=None, default_source=None,
                                                            *args, **kwargs):
        self.kinesis_client = kinesis_client
        self.identity = identity
        self.JWT = "jwt"
        self.logger_name = "KARL.Router"
        self.default_source = default_source
        
    def publish_event(self, event, stream_name, transport_mech):
        """
        Method which posts the event to Kinesis stream specified.
        """
        if not event.source and not self.default_source:
            raise ValueError("Event source not set and KARL has no default_source set, cannot post event!")
           
        if not stream_name:
            raise KinesisStreamNameError("Cannot post event! Could not resolve stream for event type:{}".format(event.type))
        
        event.submit_date = datetime.datetime.utcnow()    
        partition_key = event.source
        
        json_string = event.to_json()
        
        if str(transport_mech) == self.JWT:
            if not self.identity:
                raise ValueError("Cannot send event:'{}' ({}) as jwt. No identity is configured.".format(
                                event.uuid,
                                event.type))
            event_data = event.to_jwt(key=self.identity.private_key)
        else:
            event_data = json_string
        
        #Note: If we have boto3, use that
        if self.kinesis_client.__class__.__name__ == "KinesisConnection":
            self.kinesis_client.put_record(stream_name, event_data, partition_key)
        else:
            self.kinesis_client.put_record(StreamName=stream_name,
                                            Data=event_data,
                                            PartitionKey=partition_key)
        
class KinesisStreamNameError(Exception):
    """
    Exception raised in the event that we fail stream lookup.
    """
    pass


class FirehosePublisher(object):
    """
    Class that helps to post event to Firehose.
    """
    firehose_client = None
    JWT = None              #: transport mechanism is jwt, constant
    
    #MARK: Constructors
    def __init__(self, firehose_client=None, identity=None, default_source=None,
                                                            *args, **kwargs):
        self.firehose_client = firehose_client
        self.identity = identity
        self.JWT = "jwt"
        self.logger_name = "KARL.Router"
        self.default_source = default_source
    
    def publish_event(self, event, stream_name, transport_mech):
        '''
        Method which posts the event to Kinesis stream specified.
        :param event: Event object to publish
        :param stream_name: Delivery Stream name
        :param transport_mech: The mechanism to put the record as. (JWT, plain text etc;)
        '''
        if not event.source and not self.default_source:
            raise ValueError("Event source not set and KARL has no default_source set, cannot post event!")
        
        
        if not stream_name:
            raise ValueError("Cannot post event! Stream name is empty for event type:{1}".format(event.type))
        
        event.submit_date = datetime.datetime.utcnow()
        
        json_string = event.to_json()
        record = {}
        if str(transport_mech) == self.JWT:
            if not self.identity:
                raise ValueError("Cannot send event:'{}' ({}) as jwt. No identity is configured.".format(
                                event.uuid,
                                event.type))
            event_data = event.to_jwt(key=self.identity.private_key)
        else:
            event_data = json_string
        
        record['Data'] = event_data
        self.firehose_client.put_record(DeliveryStreamName=stream_name, Record=record)
        
        
        
        
