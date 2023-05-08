"""Message Broker"""
import enum
from typing import Dict, List, Any, Tuple
import socket
import selectors
from .protocol import Protocol



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
        self._topics = {} # topic -> value
        self.subscribers = {} # topic -> [(client, serialization),...]
        self.socketSerialization = {} # socket -> Serialization (JSON, XML, PICKLE)
        
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.selector = selectors.DefaultSelector()
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((self._host, self._port))
        self.socket.listen(100)
        self.selector.register(self.socket, selectors.EVENT_READ, self.accept)

        print("BROKER initializaing...")

        
    def accept(self, sock, mask):
        conn, addr = sock.accept()                                  
        print('accepted', conn, 'from', addr)
        # conn.setblocking(False)
        self.selector.register(conn, selectors.EVENT_READ, self.read)

        message = Protocol.recv_msg(conn)
        code = message.code
        if type(code) == str: code = int(code)

        if message:
            print(conn, " is now registered")
            if code == 0 or code == Serializer.JSON:
                self.socketSerialization[conn] = Serializer.JSON
            elif code == 1 or code == Serializer.XML:
                self.socketSerialization[conn] = Serializer.XML
            elif code == 2 or code == Serializer.PICKLE:
                self.socketSerialization[conn] = Serializer.PICKLE


    def read(self,conn, mask):
        """verify the serialization method for each conn and decode it, used for 'old' connections """
        try:
            message = Protocol.recv_msg(conn)

            if message:
                print('Message received: ', message)
                msgCommand = message.command


                if msgCommand == 'subscribe': #SubMessage
                    print(conn, " has subbed to ", message.topic)
                    self.subscribe(message.topic,conn, self.socketSerialization[conn])

                elif msgCommand == 'publish': #PubMessage
                    print(conn, " published", message.topic, " --> ", message.value)
                    self.put_topic(message.topic, message.value)

                elif msgCommand == 'ask': #AskListMessage
                    print("Sending list of topics to ", conn)
                    Protocol.send_msg(conn, Protocol.list(self.list_topics()), self.socketSerialization[conn])

                elif msgCommand == 'cancel': #CancelMessage
                    print(conn, " has cancelled the subscription to ", message.topic)
                    self.unsubscribe(message.topic,conn)


        except ConnectionError:
            print(conn, 'disconnected')
            for i in self.subscribers:
                list_users = self.subscribers[i]
                for f in list_users:
                    if f[0] == conn:
                        self.subscribers[i].remove(f)
                        break

            self.selector.unregister(conn)
            conn.close()

    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics containing values."""
        list = []
        for key in self._topics:
            list.append(key)
        return list
    
        # secalhar um send também de Pedido de Listagenm de topicos

    def get_topic(self, topic):
        """Returns the currently stored value in topic."""
        # _topics é um dicionario de topicos que contem valor, 
        # ou seja se o topic estiver no dicionario tem algo publicado nele

        if topic in self._topics:
            return self._topics[topic]
        return None

    #store in topic the value. If the topic is a subtopic from another topic, this topic also receives the value 
    def put_topic(self, topic, value):
        """Store in topic the value."""
        # topics are in the format --> /weather, /weather/temp, /weather/pressure...
        self._topics[topic] = value

        # create list for subs if it does not exist and if it is a subtopic add subs from supertopic
        if topic not in self.subscribers:
            self.subscribers[topic] = []
            for t in self.subscribers:
                if t in topic:
                    for sub in self.list_subscriptions(t):
                        if sub not in self.list_subscriptions(topic):
                            self.subscribers[topic].append(sub)
            
        # send messages (publishes)
        if topic in self.subscribers:
            for sub in self.list_subscriptions(topic):
                Protocol.send_msg(sub[0], Protocol.publish(topic, value), sub[1].value)

    def list_subscriptions(self, topic: str) -> List[Tuple[socket.socket, Serializer]]:
        """Provide list of subscribers to a given topic."""
        if topic in self.subscribers:
            return self.subscribers[topic]
        return []

    def subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
        """Subscribe to topic by client in address."""
        # Mensagem de broker -> cliente  é em xml ou pickle
        # Mensagem de produtor -> broker  é em json

        if topic not in self.subscribers:
            self.subscribers[topic] = []
            for t in self.subscribers:
                if t in topic:
                    for sub in self.list_subscriptions(t):
                        if sub not in self.list_subscriptions(topic):
                            self.subscribers[topic].append(sub)

        self.subscribers[topic].append((address, _format))
        
        # send last published topic
        if topic in self._topics:
            Protocol.send_msg(address, Protocol.publish(topic, self._topics[topic]), _format.value) # sends the last message to the subscriber
        # has to be _format-value to send 0... instead of Seralizer.JSON... --> gives error in send_msg

    def unsubscribe(self, topic, address):
        """Unsubscribe to topic by client in address."""

        if topic in self.subscribers:
            for sub in self.subscribers[topic]: 
                if sub[0] == address: self.subscribers[topic].remove(sub)

        # self.CancelSubMesssage(Message) ou algo assim


    def run(self):
        """Run until canceled."""

        while not self.canceled:
            for key, mask in self.selector.select():
                callback = key.data
                callback(key.fileobj, mask)