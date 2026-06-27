import random
import socket
import threading
from typing import List
import time
from log import log

PING_INTERVAL = 3
PING_MAX_WAIT = 5
GOSSIP_SEND_INTERVAL = 5
NUM_MESSAGES = 10


def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = '127.0.0.1'
    finally:
        s.close()
    return ip


class Peers:
    def __init__(self, ip=None, port=0):
        if ip is None:
            self.ip = get_local_ip()
        else:
            self.ip = ip
        self.port = int(port)
        self.server_socket = None

        self.seed_list = []
        self.peer_list = []
        self.seed_connections: List[socket.socket] = []
        self.peer_connections: List[socket.socket] = []

        self.message_hashes = set()
        self.running_status = True
        self.isDead = False
        self.ping_tracker = {}
        self.peer_info = {}
        self._lock = threading.Lock()

    def creation(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.server_socket.bind((self.ip, self.port))
            if self.port == 0:
                self.port = self.server_socket.getsockname()[1]
            self.server_socket.listen()
            msg = f"Peer listening on {self.ip}:{self.port}"
            print(msg)
            log(msg)
            thread = threading.Thread(target=self.accept_connections, daemon=True)
            thread.start()
        except Exception as e:
            err_msg = f"Error creating peer server on {self.ip}:{self.port}: {e}"
            print(err_msg)

    def connect(self, seeds):
        if len(seeds) > 0:
            self.seed_list = random.sample(seeds, (len(seeds) // 2) + 1)
        for seed in self.seed_list:
            self.connect_to_seed(seed)
        self.request_peer_lists()
        self.connect_to_peers()
        self.send_connection_update()
        thread_death = threading.Thread(target=self.simulate_death, daemon=True)
        thread_death.start()
        if not self.isDead:
            thread_ping_sender = threading.Thread(target=self.ping_sender, daemon=True)
            thread_ping_sender.start()


    def connect_to_seed(self, seed):
        try:
            seed_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            seed_socket.connect((seed[0], seed[1]))
            msg = f"Peer(client)({self.ip}:{self.port}) -> Connected to seed {seed[0]}:{seed[1]}"
            print(msg)
            log(msg)
            seed_socket.sendall(f"PEER_SERVER:{self.port}\n".encode('utf-8'))
            self.seed_connections.append(seed_socket)
        except socket.error as e:
            err_msg = f"Peer(client)({self.ip}:{self.port}) -> Failed to connect to seed {seed[0]}:{seed[1]}. Error: {e}"
            print(err_msg)

    def request_peer_lists(self):
        merged_peers = {}
        for seed_socket in self.seed_connections:
            try:
                seed_socket.sendall(f"REQUEST_PEER_LIST:{self.port}\n".encode('utf-8'))
                raw = b""
                while not raw.endswith(b"\n"):
                    chunk = seed_socket.recv(4096)
                    if not chunk:
                        break
                    raw += chunk
                peer_list_str = raw.decode('utf-8')
                if peer_list_str:
                    for peer in peer_list_str.split('\n'):
                        if peer:
                            parts = peer.split(':')
                            if len(parts) != 3:
                                continue
                            ip, port_str, degree_str = parts
                            try:
                                port = int(port_str)
                                degree = int(degree_str)
                            except ValueError:
                                continue
                            key = (ip, port)
                            if key in merged_peers:
                                merged_peers[key] = max(merged_peers[key], degree)
                            else:
                                merged_peers[key] = degree
            except Exception as e:
                err_msg = f"Peer(client)({self.ip}:{self.port}) -> Error requesting peer list: {e}"
                print(err_msg)
        self.peer_list = [(ip, port, merged_peers[(ip, port)]) for (ip, port) in merged_peers]
        msg = f"Peer(client)({self.ip}:{self.port}) -> Merged peer list: {self.peer_list}"
        print(msg)
        log(msg)

    def accept_connections(self):
        while self.running_status and not self.isDead:
            try:
                connection, address = self.server_socket.accept()
                msg = f"Peer(server)({self.ip}:{self.port}) -> New connection from {address[0]}:{address[1]}"
                print(msg)
                log(msg)
                with self._lock:
                    self.peer_connections.append(connection)
                thread = threading.Thread(
                    target=self.peer_listener,
                    args=(connection, "", True),
                    daemon=True
                )
                thread.start()
            except Exception as e:
                if self.running_status:
                    print(f"Peer(server)({self.ip}:{self.port}) -> Error accepting connection: {e}")
                break

    def close(self):
        self.running_status = False
        if self.server_socket:
            try:
                self.server_socket.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            self.server_socket.close()
            msg = f"Peer on {self.ip}:{self.port} closed."
            print(msg)
            log(msg)

    def connect_to_peers(self):
        for peer in set(self.peer_list):
            peer_ip, peer_port, peer_degree = peer
            if peer_ip == self.ip and peer_port == self.port:
                continue
            threshold = 1 / (peer_degree + 1)
            rand_val = random.random()
            msg = f"Evaluating {peer_ip}:{peer_port} degree={peer_degree} threshold={threshold:.4f} rand={rand_val:.4f}"
            print(msg)
            log(msg)
            if len(self.peer_list) == 1:
                threshold = 0
            if rand_val > threshold:
                try:
                    peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    peer_socket.connect((peer_ip, peer_port))
                    with self._lock:
                        self.peer_connections.append(peer_socket)
                        self.peer_info[peer_socket] = (peer_ip, peer_port)
                    peer_socket.sendall(f"NEW_PEER_SERVER:{self.port}\n".encode('utf-8'))
                    msg = f"Peer(client)({self.ip}:{self.port}) -> Connected to peer {peer_ip}:{peer_port}"
                    print(msg)
                    log(msg)
                    thread = threading.Thread(
                        target=self.peer_listener,
                        args=(peer_socket, "", False),
                        daemon=True
                    )
                    thread.start()
                except socket.error as e:
                    print(f"Peer(client)({self.ip}:{self.port}) -> Failed to connect to {peer_ip}:{peer_port}. Error: {e}")

    def send_connection_update(self):
        connected_peers = []
        for peer_socket in self.peer_connections:
            try:
                peer_addr = peer_socket.getpeername()
                connected_peers.append(f"{peer_addr[0]}:{peer_addr[1]}")
            except Exception:
                continue
        new_degree = len(self.peer_connections)
        update_msg = f"CONNECTION_UPDATE:{self.ip}:{self.port}:{new_degree}:"
        if connected_peers:
            update_msg += ",".join(connected_peers)
        update_msg += "\n"
        for seed_socket in self.seed_connections:
            try:
                seed_socket.sendall(update_msg.encode('utf-8'))
                msg = f"Peer(client)({self.ip}:{self.port}) -> Sent connection update to seed"
                print(msg)
                log(msg)
            except Exception as e:
                print(f"Peer(client)({self.ip}:{self.port}) -> Failed to send connection update: {e}")

    def peer_listener(self, peer: socket.socket, buffer: str, handshake_required: bool = True):
        handshake_done = not handshake_required
        while self.running_status and not self.isDead:
            try:
                data = peer.recv(1024)
            except Exception as e:
                err_msg = f"Error receiving data: {e}"
                print(err_msg)
                log(err_msg)
                break
            if not data:
                break
            buffer += data.decode('utf-8')
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                if not line:
                    continue
                if not handshake_done:
                    if line.startswith("PEER_SERVER:") or line.startswith("NEW_PEER_SERVER:"):
                        try:
                            remote_port = int(line.split(":")[1])
                            remote_ip = peer.getpeername()[0]
                            with self._lock:
                                self.peer_info[peer] = (remote_ip, remote_port)
                            handshake_done = True
                            msg = f"Handshake complete with peer {remote_ip}:{remote_port}"
                            print(msg)
                            log(msg)
                        except ValueError:
                            log(f"Invalid handshake message: {line}")
                    else:
                        log(f"Expected handshake but received: {line}")
                else:
                    msg = f"Received: {line}"
                    print(msg)
                    log(msg)
                    if line.startswith("PING"):
                        peer.sendall("PONG\n".encode('utf-8'))
                        log(f"Sent PONG to {self.peer_info.get(peer, peer.getpeername())}")
                    elif line.startswith("PONG"):
                        with self._lock:
                            self.ping_tracker[peer] = [time.time(), 0]
                        log(f"Received PONG from {self.peer_info.get(peer, peer.getpeername())}")
                    elif line.startswith("GOSSIP:"):
                        try:
                            message_hash = int(line.split("GOSSIP:")[1])
                        except ValueError:
                            log(f"Failed to parse message hash: {line}")
                            continue
                        with self._lock:
                            already_seen = message_hash in self.message_hashes
                            if not already_seen:
                                self.message_hashes.add(message_hash)
                                peers_to_notify = [ps for ps in self.peer_connections if ps != peer]
                            else:
                                peers_to_notify = []
                        for peer_socket in peers_to_notify:
                            thread = threading.Thread(
                                target=self.gossip_sender_peer,
                                args=(peer_socket, message_hash),
                                daemon=True
                            )
                            thread.start()

    def gossip_sender_all(self):
        for i in range(NUM_MESSAGES):
            if not self.running_status or self.isDead:
                break
            message = f"{time.strftime('%H:%M:%S')}:{self.ip}:{self.port}:m"
            message_hash = hash(message)
            with self._lock:
                already_seen = message_hash in self.message_hashes
                if not already_seen:
                    self.message_hashes.add(message_hash)
                    peers_to_notify = list(self.peer_connections)
                else:
                    peers_to_notify = []
            if not already_seen:
                log(f"Peer {self.ip}:{self.port} -> Sending gossip hash for first time: {message}")
            for peer in peers_to_notify:
                thread = threading.Thread(
                    target=self.gossip_sender_peer,
                    args=(peer, message_hash),
                    daemon=True
                )
                thread.start()
            time.sleep(GOSSIP_SEND_INTERVAL)

    def gossip_sender_peer(self, peer: socket.socket, message_hash: int):
        try:
            peer.sendall(f"GOSSIP:{message_hash}\n".encode('utf-8'))
            msg = f"Peer({self.ip}:{self.port}) -> Sent GOSSIP:{message_hash}"
            print(msg)
        except Exception as e:
            if self.running_status:
                print(f"Peer({self.ip}:{self.port}) -> Error sending gossip: {e}")

    def ping_sender(self):
        while self.running_status and not self.isDead:
            for peer_socket in list(self.peer_connections):
                self.ping_sender_peer(peer_socket)
            time.sleep(PING_INTERVAL)


    def ping_sender_peer(self, peer_socket: socket.socket):
        if self.isDead or not self.running_status:
            return
        try:
            with self._lock:
                if peer_socket not in self.ping_tracker:
                    self.ping_tracker[peer_socket] = [time.time(), 0]
                peer_addr = self.peer_info.get(peer_socket, ("Unknown", "Unknown"))
                elapsed = time.time() - self.ping_tracker[peer_socket][0]
                miss_count = self.ping_tracker[peer_socket][1]

            if elapsed >= PING_MAX_WAIT:
                with self._lock:
                    self.ping_tracker[peer_socket] = [time.time(), miss_count + 1]
                    new_count = self.ping_tracker[peer_socket][1]
                if new_count >= 3:
                    msg = f"Peer(client)({self.ip}:{self.port}) -> Peer {peer_addr} is dead"
                    print(msg)
                    log(msg)
                    with self._lock:
                        if peer_socket in self.peer_connections:
                            self.peer_connections.remove(peer_socket)
                        self.peer_list = [p for p in self.peer_list if p[0:2] != peer_addr]
                    for seed_socket in self.seed_connections:
                        dead_msg = f"DEAD_NODE:{peer_addr[0]}:{peer_addr[1]}:{time.strftime('%H:%M:%S')}:{self.ip}:{self.port}\n"
                        seed_socket.sendall(dead_msg.encode('utf-8'))
                    peer_socket.close()
                    return
            peer_socket.sendall("PING\n".encode('utf-8'))
            msg = f"Peer(client)({self.ip}:{self.port}) -> Sent PING to {peer_addr}"
            print(msg)
            log(msg)
        except Exception as e:
            if not self.running_status:
                return
            peer_addr = self.peer_info.get(peer_socket, ("Unknown", "Unknown"))
            with self._lock:
                if peer_socket not in self.ping_tracker:
                    self.ping_tracker[peer_socket] = [time.time(), 1]
                else:
                    self.ping_tracker[peer_socket][1] += 1
                fail_count = self.ping_tracker[peer_socket][1]
            if fail_count >= 3:
                msg = f"Peer(client)({self.ip}:{self.port}) -> Peer {peer_addr} dead after ping failures"
                print(msg)
                log(msg)
                with self._lock:
                    if peer_socket in self.peer_connections:
                        self.peer_connections.remove(peer_socket)
                for seed_socket in self.seed_connections:
                    try:
                        dead_msg = f"DEAD_NODE:{peer_addr[0]}:{peer_addr[1]}:{time.strftime('%H:%M:%S')}:{self.ip}:{self.port}\n"
                        seed_socket.sendall(dead_msg.encode('utf-8'))
                    except Exception:
                        pass
                try:
                    peer_socket.close()
                except Exception:
                    pass


    def simulate_death(self):
        chance_to_die = 0.3
        if random.random() < chance_to_die:
            death_time = random.uniform(30, 60)
            time.sleep(death_time)
            self.isDead = True
            msg = f"Peer {self.ip}:{self.port} has died (simulated)."
            print(msg)
            log(msg)
        else:
            msg = f"Peer {self.ip}:{self.port} remains alive (simulation)."
            print(msg)
