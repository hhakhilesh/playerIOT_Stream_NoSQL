from random import uniform
import streamschema_pb2
import time

class Footballer:

    def __init__(self,name,role,team):
        self.name = name
        self.role = role
        self.team = team

        self.strategy = None
        self.match = None
        self.sensor_id = None

        self.x = None
        self.y = None

    def enterMatch(self,strategy,match,sensor_id):

        self.strategy = strategy
        self.match = match
        self.sensor_id = sensor_id

        self.x = uniform(0,105)
        self.y = uniform(0,68)

    
    def serialize(self):

        proto_instance = streamschema_pb2.Position() # streamschema
        proto_instance.sensor_id = self.sensor_id
        proto_instance.timestamp_usec = int(time.mktime(time.gmtime()))
        proto_instance.location.x = self.x
        proto_instance.location.x = self.y
        proto_instance.player_info.name = self.name
        proto_instance.player_info.role = self.role
        proto_instance.player_info.strategy = self.strategy
        proto_instance.match_info.team = self.team
        proto_instance.match_info.team = str(self.match)

        serial_message = proto_instance.SerializeToString()
        return serial_message

    def move(self):

        if ((self.strategy is None) or (self.match is None)\
            or (self.sensor_id is None)):
                raise ValueError("Cannot move without entering a match.\
                                 Call enterMatch() first.")

        while(True):
            x_rand = uniform(-9.34,9.34)
            y_rand = uniform(-9.34,9.34)

            if((self.x + x_rand)<=105 and (self.x + x_rand)>=0 \
            and (self.y + y_rand)<=68 and (self.y + y_rand)>=0):

                self.x +=x_rand
                self.y +=y_rand
                break


        serial_message = self.serialize()
        return serial_message
