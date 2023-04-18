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

    def __str__(self) -> str:
        return '{"command": '

class SubMessage(Message):
    """Message to subscribe to a given topic."""
    def __init__(self, command, topic):
        super().__init__(command)
        self.topic = topic

    def __str__(self):
        return super().__str__() + f'"subscribe, "topic": "{self.topic}"' + '}'

class PubMessage(Message):
    """Message to publish a given topic."""
    def __init__(self, command, topic):
        super().__init__(command)
        self.topic = topic

    def __str__(self):
        return super().__str__() + f'"publish, "topic": "{self.topic}"' + '}'

class ListMessage(Message):
    """Message to list all topics."""
    def __init__(self, command):
        super().__init__(command)

    def __str__(self):
        return super().__str__() + 'list' + '}'
    
class CancelMessage(Message):
    """Message to cancel a given topic."""
    def __init__(self, command, topic):
        super().__init__(command)
        self.topic = topic
    
    def __str__(self):
        return super().__str__() + f'cancel, "topic": "{self.topic}"' + '}'

class Protocol:
    """Protocol that implements the messages above"""

    @classmethod
    def subscribe(cls, topic: str) -> SubMessage:
        """Creates a SubMessage object."""
        return SubMessage('subscribe', topic)
    
    @classmethod
    def publish(cls, topic: str) -> PubMessage:
        """Creates a PubMessage object."""
        return PubMessage('publish', topic)

    @classmethod
    def list(cls) -> ListMessage:
        """Creates a ListMessage object."""
        return ListMessage('list')
    
    @classmethod
    def cancel(cls, topic: str) -> CancelMessage:
        """Creates a CancelMessage object."""
        return CancelMessage('cancel', topic)
    

    @classmethod
    def send_msg(cls, connection: socket, msg: Message, encode: Serializer):
        """Sends through a connection a Message object."""

        if type(msg) is PubMessage:
            if encode == Serializer.JSON:
                message = { "command": "publish", "topic": f"{msg.topic}" }
                message = json.dumps(message).encode("utf-8")

            elif encode == Serializer.PICKLE:
                message = { "command": "publish", "topic": f"{msg.topic}" }
                message = pickle.dumps(message)

            elif encode == Serializer.XML:
                pass