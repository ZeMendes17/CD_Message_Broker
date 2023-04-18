"""Message Broker"""
import enum
from typing import Dict, List, Any, Tuple
import socket


class Serializer(enum.Enum):
    """Possible message serializers."""

    JSON = 0
    XML = 1
    PICKLE = 2


class Broker:
    """Implementation of a PubSub Message Broker."""

    def __init__(self):
        """Initialize broker."""
        self.canceled = False
        self._host = "localhost"
        self._port = 5000
        self._topics = {}
        self.subscribers = {}


    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics containing values."""
        #TODO - verificate if this is correct and complete
        list = []
        for key in self._topics:
            list.append(key)
        return list
    
        # secalhar um send também de Pedido de Listagenm de topicos

    def get_topic(self, topic):
        """Returns the currently stored value in topic."""
        #TODO - verificate if this is correct
        # _topics é um dicionario de topicos que contem valor, 
        # ou seja se o topic estiver no dicionario tem algo publicado nele

        return self._topics[topic]

    def put_topic(self, topic, value):
        """Store in topic the value."""
        #TODO - verificate if this is correct
        self._topics[topic] = value

    def list_subscriptions(self, topic: str) -> List[Tuple[socket.socket, Serializer]]:
        """Provide list of subscribers to a given topic."""
        #TODO - verificate if this is correct and complete
        #maybe send something
        return self.subscribers[topic]

    def subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
        """Subscribe to topic by client in address."""
        #TODO - verificate if this is correct and complete
        # Mensagem de broker -> cliente  é em xml ou pickle
        # Mensagem de produtor -> broker  é em json
        
        if topic in self.subscribers:
            self.subscribers[topic].append((address, _format))
        else:
            self.subscribers[topic] = [(address, _format)]

        # caso tenha valor no topic (uma publicação) envia a ultima publicação a esse consumidor
        if topic in self._topics:
            address.send(self._topics[topic])
        
        # self.SubMesssage(Message) ou algo assim



    def unsubscribe(self, topic, address):
        """Unsubscribe to topic by client in address."""
        #TODO - verificate if this is correct and complete

        self.subscribers[topic].remove(address)
        
        # self.CancelSubMesssage(Message) ou algo assim


    def run(self):
        """Run until canceled."""

        while not self.canceled:
            pass
