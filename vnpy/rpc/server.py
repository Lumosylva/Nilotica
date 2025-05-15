import threading
import traceback
from collections.abc import Callable
from time import time

import msgpack
import zmq

from .common import HEARTBEAT_INTERVAL, HEARTBEAT_TOPIC


class RpcServer:
    """"""

    def __init__(self) -> None:
        """
        Constructor
        """
        # Save functions dict: key is function name, value is function object
        self._functions: dict[str, Callable] = {}

        # Zmq port related
        self._context: zmq.Context = zmq.Context()

        # Reply socket (Request–reply pattern)
        self._socket_rep: zmq.Socket = self._context.socket(zmq.REP)

        # Publish socket (Publish–subscribe pattern)
        self._socket_pub: zmq.Socket = self._context.socket(zmq.PUB)

        # Worker thread related
        self._active: bool = False                          # RpcServer status
        self._thread: threading.Thread | None = None        # RpcServer thread
        self._lock: threading.Lock = threading.Lock()

        # Heartbeat related
        self._heartbeat_at: float | None = None

    def is_active(self) -> bool:
        """"""
        return self._active

    def start(
        self,
        rep_address: str,
        pub_address: str,
    ) -> None:
        """
        Start RpcServer
        """
        if self._active:
            return

        # Bind socket address
        self._socket_rep.bind(rep_address)
        self._socket_pub.bind(pub_address)

        # Start RpcServer status
        self._active = True

        # Start RpcServer thread
        self._thread = threading.Thread(target=self.run)
        self._thread.start()

        # Init heartbeat publish timestamp
        self._heartbeat_at = time() + HEARTBEAT_INTERVAL

    def stop(self) -> None:
        """
        Stop RpcServer
        """
        if not self._active:
            return

        # Stop RpcServer status first to signal the run loop to exit
        self._active = False

        # Close sockets immediately to prevent issues during thread termination
        try:
            self._socket_rep.close()
        except Exception as e:
            pass # Ignore exceptions during close

        try:
            self._socket_pub.close()
        except Exception as e:
            pass # Ignore exceptions during close

        # Terminate the ZMQ context to release resources
        try:
            self._context.term()
        except Exception as e:
            # Log error or pass if context termination fails
            # print(f"Error terminating ZMQ context: {e}") # Example direct print
            pass

    def join(self) -> None:
        # Wait for RpcServer thread to exit
        if self._thread and self._thread.is_alive():
            self._thread.join()
        self._thread = None

    def run(self) -> None:
        """
        Run RpcServer functions
        """
        while self._active:
            # Poll response socket for 1 second
            n: int = self._socket_rep.poll(1000)
            self.check_heartbeat()

            if not n:
                continue

            # Receive request data from Reply socket
            req_bytes = self._socket_rep.recv()
            try:
                req = msgpack.unpackb(req_bytes, raw=False)
            except (msgpack.UnpackException, TypeError, ValueError) as e_unpack:
                error_msg = f"Msgpack unpack error in RpcServer.run: {e_unpack}"
                print(error_msg) # Or use logger if available
                rep = [False, traceback.format_exc()]
                # Pack and send error response
                try:
                    rep_bytes = msgpack.packb(rep, use_bin_type=True)
                    self._socket_rep.send(rep_bytes)
                except (msgpack.PackException, TypeError) as e_pack:
                    print(f"Msgpack pack error sending error response: {e_pack}") # Or logger
                continue # Skip processing this invalid request

            # Get function name and parameters
            name, args, kwargs = req

            # Try to get and execute callable function object; capture exception information if it fails
            try:
                func: Callable = self._functions[name]
                r: object = func(*args, **kwargs)
                rep: list = [True, r]
            except Exception as e:  # noqa
                rep = [False, traceback.format_exc()]

            # send callable response by Reply socket
            try:
                rep_bytes = msgpack.packb(rep, use_bin_type=True)
                self._socket_rep.send(rep_bytes)
            except (msgpack.PackException, TypeError) as e_pack:
                 print(f"Msgpack pack error sending response for {name}: {e_pack}") # Or logger

        # Unbind socket address
        self._socket_pub.unbind(str(self._socket_pub.LAST_ENDPOINT))
        self._socket_rep.unbind(str(self._socket_rep.LAST_ENDPOINT))

    def publish(self, topic: str, data: object) -> None:
        """
        Publish data
        """
        with self._lock:
            try:
                 packed_data = msgpack.packb([topic, data], use_bin_type=True)
                 self._socket_pub.send(packed_data)
            except (msgpack.PackException, TypeError) as e_pack:
                 print(f"Msgpack pack error in RpcServer.publish (Topic: {topic}): {e_pack}") # Or logger

    def register(self, func: Callable) -> None:
        """
        Register function
        """
        self._functions[func.__name__] = func

    def check_heartbeat(self) -> None:
        """
        Check whether it is required to send heartbeat.
        """
        now: float = time()

        if self._heartbeat_at and now >= self._heartbeat_at:
            # Publish heartbeat
            self.publish(HEARTBEAT_TOPIC, now)

            # Update timestamp of next publish
            self._heartbeat_at = now + HEARTBEAT_INTERVAL
