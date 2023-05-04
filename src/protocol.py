import enum
from socket import socket
import json
import pickle
import xml.etree.ElementTree as ET

class Serializer(enum.Enum):
    """Possible message serializers."""

    JSON = 0
    XML = 1
    PICKLE = 2

class Message:
    """Message Type."""
    def __init__(self, command):
        self.command = command

    def __repr__(self): # this will be used in the subclasses to represent JSON messages
        return '{"command":'
    
class SerializationMessage(Message):
    def __init__(self, command, code):
        super().__init__(command)
        self.code = code

    def __repr__(self):
        return super().__repr__() + f'"{self.command}", "code": "{self.code}"' + '}'

    def pickleMsg(self):
        return {"command": self.command, "code": self.code}
    
    def xmlMsg(self):
        return f'<?xml version="1.0"?><data type="{self.command}" code="{self.code}"></data>'

class SubMessage(Message):
    """Message to subscribe to a given topic."""
    def __init__(self, command, topic):
        super().__init__(command)
        self.topic = topic
    
    def __repr__(self):
        return super().__repr__() + f'"{self.command}", "topic": "{self.topic}"' + '}'
    
    def pickleMsg(self):
        return {"command": self.command, "topic": self.topic}
    
    def xmlMsg(self):
        return f'<?xml version="1.0"?><data command="{self.command}" topic="{self.topic}"></data>'
    

class PubMessage(Message):
    """Message to publish a given topic."""
    def __init__(self, command, topic, value):
        super().__init__(command)
        self.topic = topic
        self.value = value

    def __repr__(self):
        return super().__repr__() + f'"{self.command}", "topic": "{self.topic}", "value": {self.value}' + '}'
    
    def pickleMsg(self):
        return {"command": self.command, "topic": self.topic, "value": self.value}
    
    def xmlMsg(self):
        return f'<?xml version="1.0"?><data command="{self.command}" topic="{self.topic}" value="{self.value}"></data>'

class AskListMessage(Message):
    def __init__(self, command):
        super().__init__(command)

    def __repr__(self):
        return super().__repr__() + f'"{self.command}"' + '}'

    def pickleMsg(self):
        return {"command": self.command}
    
    def xmlMsg(self):
        return f'<?xml version="1.0"?><data command="{self.command}"></data>'

class ListMessage(Message):
    """Message to list all topics."""
    def __init__(self, command, topics):
        super().__init__(command)
        self.topics = topics

    def __repr__(self):
        return super().__repr__() + f'"{self.command}", "topics": {self.topics}' + '}'
    
    def pickleMsg(self):
        return {"command": self.command, "topics": self.topics}
    
    def xmlMsg(self):
        return f'<?xml version="1.0"?><data command="{self.command}" topics="{self.topics}"></data>'
    
class CancelMessage(Message):
    """Message to cancel a given topic."""
    def __init__(self, command, topic):
        super().__init__(command)
        self.topic = topic
    
    def __repr__(self):
        return super().__repr__() + f'"{self.command}", "topic": "{self.topic}"' + '}'
    
    def pickleMsg(self):
        return {"command": self.command, "topic": self.topic}
    
    def xmlMsg(self):
        return f'<?xml version="1.0"?><data command="{self.command}" topic="{self.topic}"></data>'
    
# class ReplyMessage(Message):
#     def __init__(self, command, topic, value):
#         super().__init__(command)
#         self.topic = topic
#         self.value = value

#     def __repr__(self):
#         return super().__repr__() + f'"{self.command}", "topic": "{self.topic}", "value": {self.value}' + '}'
    
#     def pickleMsg(self):
#         return {"command": self.command, "topic": self.topic, "value": self.value}
    
#     def xmlMsg(self):
#         return f'<?xml version="1.0"?><data command="{self.command}" topic="{self.topic} value="{self.value}"></data>'

class Protocol:
    """Protocol that implements the messages above"""

    @classmethod
    def serialize(cls, code) -> SerializationMessage:
        """Creates a RegsiterMessage object."""
        return SerializationMessage('type', code)

    @classmethod
    def subscribe(cls, topic: str) -> SubMessage:
        """Creates a SubMessage object."""
        return SubMessage('subscribe', topic)
    
    @classmethod
    def publish(cls, topic: str, value) -> PubMessage:
        """Creates a PubMessage object."""
        return PubMessage('publish', topic, value)
    
    @classmethod
    def ask_list(cls) -> AskListMessage:
        """Creates a AskListMessage object."""
        return AskListMessage('ask')

    @classmethod
    def list(cls, topics) -> ListMessage:
        """Creates a ListMessage object."""
        return ListMessage('list', topics)
    
    @classmethod
    def cancel(cls, topic: str) -> CancelMessage:
        """Creates a CancelMessage object."""
        return CancelMessage('cancel', topic)
    
    # @classmethod
    # def reply(cls, topic: str, value: str) -> ReplyMessage:
    #     """Creates a ReplyMessage object."""
    #     return ReplyMessage('reply', topic, value)

    @classmethod
    def send_msg(cls, connection: socket, msg: Message, code):
        """Sends through a connection a Message object."""

        if code == None: code=0

        if type(code) == str:
            code = int(code)

        # one byte in big endian to refer to the encoding needed (JSON, XML or pickle)
        connection.send(code.to_bytes(1, 'big'))

        if code == Serializer.JSON or code == 0:
            message = json.loads(msg.__repr__())
            message = json.dumps(message).encode('utf-8')
            # we need to send the length of message with a 2 byte big endian header
            connection.send(len(message).to_bytes(2, 'big'))
            # send the message
            connection.send(message)

        elif code == Serializer.XML or code == 1:
            message = msg.xmlMsg().encode('utf-8')
            # we need to send the length of message with a 2 byte big endian header
            connection.send(len(message).to_bytes(2, 'big'))
            # send the message
            connection.send(message)

        elif code == Serializer.PICKLE or code == 2:
            message = pickle.dumps(msg.pickleMsg())
            # we need to send the length of message with a 2 byte big endian header
            connection.send(len(message).to_bytes(2, 'big'))
            # send the message
            connection.send(message)

    @classmethod
    def recv_msg(cls, connection: socket) -> Message:
        """Receives through a connection a Message object."""
        code = int.from_bytes(connection.recv(1), 'big')
        miniHeader = int.from_bytes(connection.recv(2), 'big')

        if miniHeader == 0: # if there is no length
            return None
        
        try:
            if code == 0:
                tempMsg = connection.recv(miniHeader).decode('utf-8')
                message = json.loads(tempMsg)

            elif code == 1:
                tempMsg = connection.recv(miniHeader).decode('utf-8')
                message = {}
                root = ET.fromstring(tempMsg)
                for node in root.keys():
                    message[node] = root.get(node)

            elif code == 2:
                tempMsg = connection.recv(miniHeader)
                message = pickle.loads(tempMsg)

        except json.JSONDecodeError as err:
            raise ProtocolBadFormat(tempMsg)

        command = message["command"]

        if command == "type":
            return cls.serialize(message["code"])

        elif command == "subscribe":
            return cls.subscribe(message["topic"])

        elif command == "publish":
            return cls.publish(message["topic"], message["value"])

        elif command == "ask":
            return cls.ask_list()
        
        elif command == "list":
            return cls.list(message["topics"])
        
        elif command == "cancel":
            return cls.list(message["topic"])
        
        # elif command == "reply":
        #     return cls.reply(message["topic"], message["value"])

        else:
            return None
        
class ProtocolBadFormat(Exception):
    """Exception when source message is not Protocol."""

    def __init__(self, original_msg: bytes=None):
        """Store original message that triggered exception."""
        self._original = original_msg

    @property
    def original_msg(self) -> str:
        """Retrieve original message as a string."""
        return self._original.decode("utf-8")