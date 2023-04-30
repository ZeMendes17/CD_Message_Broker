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

    def __repr__(self): # this wiill be used in the subclasses to represent JSON messages
        return '{"command":'
    
class RegisterMessage(Message):
    def __init__(self, command, code):
        super().__init__(command)
        self.code = code

    def __repr__(self):
        return super().__repr__() + f'"register", "code": "{self.code}"' + '}'

    def pickleMsg(self):
        return {"command": "register", "code": self.code}
    
    def xmlMsg(self):
        return f'<?xml version="1.0"?><data command="{self.command}" code="{self.code}"></data>'

class SubMessage(Message):
    """Message to subscribe to a given topic."""
    def __init__(self, command, topic):
        super().__init__(command)
        self.topic = topic
    
    def __repr__(self):
        return super().__repr__() + f'"subscribe, "topic": "{self.topic}"' + '}'
    
    def pickleMsg(self):
        return {"command": "subscribe", "topic": self.topic}
    
    def xmlMsg(self):
        return f'<?xml version="1.0"?><data command="{self.command}" topic="{self.topic}"></data>'
    

class PubMessage(Message):
    """Message to publish a given topic."""
    def __init__(self, command, topic):
        super().__init__(command)
        self.topic = topic

    def __repr__(self):
        return super().__repr__() + f'"publish, "topic": "{self.topic}"' + '}'
    
    def pickleMsg(self):
        return {"command": "publish", "topic": self.topic}
    
    def xmlMsg(self):
        return f'<?xml version="1.0"?><data command="{self.command}" topic="{self.topic}"></data>'

class AskListMessage(Message):
    def __init__(self, command):
        super().__init__(command)

    def __repr__(self):
        return super().__repr__() + f'"ask' + '}'

    def pickleMsg(self):
        return {"command": "ask"}
    
    def xmlMsg(self):
        return f'<?xml version="1.0"?><data command="{self.command}"></data>'

class ListMessage(Message):
    """Message to list all topics."""
    def __init__(self, command, topics):
        super().__init__(command)
        self.topics = topics

    def __repr__(self):
        return super().__repr__() + f'list, "topics": {self.topics}' + '}'
    
    def pickleMsg(self):
        return {"command": "list"}
    
    def xmlMsg(self):
        return f'<?xml version="1.0"?><data command="{self.command}" topics="{self.topics}"></data>'
    
class CancelMessage(Message):
    """Message to cancel a given topic."""
    def __init__(self, command, topic):
        super().__init__(command)
        self.topic = topic
    
    def __repr__(self):
        return super().__repr__() + f'cancel, "topic": "{self.topic}"' + '}'
    
    def pickleMsg(self):
        return {"command": "cancel", "topic": self.topic}
    
    def xmlMsg(self):
        return f'<?xml version="1.0"?><data command="{self.command}" topic="{self.topic}"></data>'

class Protocol:
    """Protocol that implements the messages above"""

    @classmethod
    def register(cls, code) -> RegisterMessage:
        """Creates a RegsiterMessage object."""
        return RegisterMessage('register', code)

    @classmethod
    def subscribe(cls, topic: str) -> SubMessage:
        """Creates a SubMessage object."""
        return SubMessage('subscribe', topic)
    
    @classmethod
    def publish(cls, topic: str) -> PubMessage:
        """Creates a PubMessage object."""
        return PubMessage('publish', topic)
    
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
    

    @classmethod
    def send_msg(cls, connection: socket, msg: Message, code):
        """Sends through a connection a Message object."""

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

        if command == "register":
            return cls.register(message["code"])

        elif command == "subscribe":
            return cls.subscribe(message["topic"])

        elif command == "publish":
            return cls.publish(message["topic"])

        elif command == "ask":
            return cls.ask_list()
        
        elif command == "list":
            return cls.list(message["topics"])
        
        elif command == "cancel":
            return cls.list(message["topic"])

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