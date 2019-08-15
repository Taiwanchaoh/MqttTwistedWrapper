"""
Wrapper to combine mqtt and twisted together
"""
from twisted.python import log
import paho.mqtt.client as mqtt
from twisted.internet import reactor
from twisted.internet.task import LoopingCall
import socket


class MqttSocket(object):
    """
    Combine the event loop of MQTT client and twisted reactor together.
    """
    def __init__(self, client, check_period=1.5):
        """
        Initialize MQTT socket.

        :param client: Paho MQTT client
        :param check_period: checking period [default is 1 second]
        """
        self.client = client
        self.client.on_socket_open = self.on_socket_open
        self.client.on_socket_close = self.on_socket_close
        self.client.on_socket_register_write = self.on_socket_register_write
        self.client.on_socket_unregister_write = self.on_socket_unregister_write
        self.misc = LoopingCall(self.misc_loop)
        self._check_period = check_period
        self.sock = None

    # IFileDescriptor properties
    def fileno(self):
        try:
            return self.sock.fileno()
        except socket.error:
            return -1

    def connectionLost(self, reason):
        # Do nothing
        pass

    # IReadDescriptor property
    def doRead(self):
        self.client.loop_read()

    # IWriteDescriptor property
    def doWrite(self):
        self.client.loop_write()

    # IRead/Write Descriptor shared properties
    def logPrefix(self):
        return 'mqtt_proxy'

    def on_socket_open(self, client, userdata, sock):
        """
        Setup the read monitor

        :param client: mqtt client
        :param userdata: mqtt client user data
        :param sock: sock
        :return: None
        """
        self.sock = sock

        # Start monitoring the socket
        reactor.addReader(self)
        self.misc.start(self._check_period).addErrback(lambda x: print(x))

    def on_socket_close(self, client, userdata, sock):
        """
        Removed from the read monitor.

        :param client: mqtt client
        :param userdata: mqtt client user data
        :param sock: sock
        :return: None
        """
        reactor.removeReader(self)
        self.sock = None
        log.msg("stop misc loop")
        self.misc.stop()

    def on_socket_register_write(self, client, userdata, sock):
        """
        Called when a write operation to the socket failed because it would have blocked, e.g. output buffer full. Use
        this to register the socket with an external event loop for writing.

        :param client: mqtt client
        :param userdata: mqtt client data
        :param sock: sock
        :return: None
        """
        reactor.addWriter(self)

    def on_socket_unregister_write(self, client, userdata, sock):
        """
        Remove from write monitor
        :param client: mqtt client
        :param userdata: mqtt client data
        :param sock: sock
        :return: None
        """
        reactor.removeWriter(self)

    def misc_loop(self):
        """
        Check the event loop periodically
        :return: None
        """
        if self.client.loop_misc() != mqtt.MQTT_ERR_SUCCESS:
            self.misc.stop()


class MqttTwistedClient(mqtt.Client):
    """
    Client object which has the exact behaviors as mqtt.client, except that it can run under twisted reactor.
    """
    def __init__(self, client_id="", clean_session=True, userdata=None, protocol=mqtt.MQTTv311, transport="tcp"):
        super().__init__(client_id, clean_session, userdata, protocol, transport)
        self._twisted_sock = None

    def connect(self, host, port=1883, keepalive=60, bind_address=""):
        self._twisted_sock = MqttSocket(self)
        super().connect(host, port, keepalive, bind_address)

    def connect_srv(self, domain=None, keepalive=60, bind_address=""):
        self._twisted_sock = MqttSocket(self)
        super().connect_srv(domain, keepalive, bind_address)

    def connect_async(self, host, port=1883, keepalive=60, bind_address=""):
        self._twisted_sock = MqttSocket(self)
        super().connect_async(host, port, keepalive, bind_address)

    def connect_status(self):
        """
        Check the connection status
        :return: [int]
            0: no connection
            1: connected
            -1: socket has not been set yet.
        """
        if self._twisted_sock is not None:
            if self._twisted_sock.sock is not None:
                return 1
            return 0
        else:
            return -1
