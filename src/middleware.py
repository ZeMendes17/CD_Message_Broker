import socket
import selectors

# from src.middleware import MiddlewareType
from .protocol import Protocol

"""Middleware to communicate with PubSub Message Broker."""
from collections.abc import Callable
from enum import Enum
from queue import LifoQueue, Empty # needed?
from typing import Any


class MiddlewareType(Enum):
    """Middleware Type."""

    CONSUMER = 1
    PRODUCER = 2


class Queue:
    """Representation of Queue interface for both Consumers and Producers."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        """Create Queue."""
        self.topic = topic
        self._type = _type
        self.code = 0 # if it is not defined send in JSON
        # self.queue = LifoQueue()

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # self.selector = selectors.DefaultSelector()
        self.host = 'localhost'
        self.port = 5000
        self.socket.connect((self.host, self.port))
        # self.selector.register(self.socket, selectors.EVENT_READ, self.pull)

        # if _type == MiddlewareType.CONSUMER:
        #     Protocol.send_msg(self.socket, Protocol.subscribe(self.topic), self.code)


    def push(self, value):
        """Sends data to broker."""
        # mensagem de publicação para o broker
        # broker envia para todos os clientes que estão subscritos no topico
        message = Protocol.publish(self.topic, value)
        Protocol.send_msg(self.socket, message, self.code)

    def pull(self) -> (str, Any):
        """Receives (topic, data) from broker.

        Should BLOCK the consumer!"""
        # o primeiro pull envia a ultima subscrição
        # os próximos bloqueiam até alguém publicar algo no topico
        message = Protocol.recv_msg(self.socket)
        if message == None: 
            return None
        return (message.topic, message.value)


    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        # essencialmente pelos consumidores
        # enviar mensagem ao broker a pedir a lista de topicos
        # não retorna nada
        message = Protocol.ask_list()
        Protocol.send_msg(self.socket, message , self.code)

    def cancel(self):
        """Cancel subscription."""
        message = Protocol.cancel(self.topic)
        Protocol.send_msg(self.socket, message, self.code)


class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        self.code = 0
        Protocol.send_msg(self.socket, Protocol.serialize(self.code), 0)

        if _type == MiddlewareType.CONSUMER:
            Protocol.send_msg(self.socket, Protocol.subscribe(topic), self.code)


class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        self.code = 1
        Protocol.send_msg(self.socket, Protocol.serialize(self.code), 0)

        if _type == MiddlewareType.CONSUMER:
            Protocol.send_msg(self.socket, Protocol.subscribe(topic), self.code)

class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        self.code = 2
        Protocol.send_msg(self.socket, Protocol.register(self.code), 0)

        if _type == MiddlewareType.CONSUMER:
            Protocol.send_msg(self.socket, Protocol.subscribe(topic), self.code)