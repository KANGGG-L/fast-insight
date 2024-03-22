import socket
import threading
from concurrent.futures import ThreadPoolExecutor
import analysis

BUFFER_SIZE = 1024
PATH = '~/fast-insight/'

class Server:
    def __init__(self, server_address: str = "127.0.0.1", server_port: int = 12345, max_clients: int = 5) -> None:
        self.server_address = (server_address, server_port)
        self.max_clients = max_clients
        self.executor = ThreadPoolExecutor(max_workers=self.max_clients)
        self.connected_clients = []
        self.waiting_clients = []
        self.lock = threading.Lock()

    def handle_client(self, client_socket, address):
        print(f"Connected to {address}")

        # Receive client request
        request = client_socket.recv(BUFFER_SIZE).decode()


        if request == 'ANALYSE_NOW':
            # Receive file from client
            client_index = self.connected_clients.index((client_socket, address))
            file_size = int(client_socket.recv(BUFFER_SIZE).decode())

            print(file_size)
            with open(str(client_index) + 'data', 'wb') as data_file:
                while file_size > 0:
                    data = client_socket.recv(BUFFER_SIZE)
                    data_file.write(data)
                    file_size -= len(data)
            print(f"Received file from {address} successfully.")

            # Analyze the file using analysis module
            output_path = PATH + f"{client_index}_report"
            analysis.run(str(client_index) + 'data', output_path, 0.5)  # Example threshold: 0.5

            # Send back the report to the client
            with open(output_path, 'rb') as report_file:
                report_data = report_file.read()
                client_socket.sendall(report_data)
            print(f"Report sent to {address}")

        client_socket.close()
        print(f"Connection closed with {address}")

        # Remove the client from connected_clients list
        with self.lock:
            self.connected_clients.remove((client_socket, address))

        # Check if there are waiting clients and serve them with FIFO rule
        if self.waiting_clients:
            client_socket, address = self.waiting_clients.pop(0)
            self.executor.submit(self.handle_client, client_socket, address)

    def client_handler(self):
        while True:
            if len(self.connected_clients) < self.max_clients:
                if self.waiting_clients:
                    client_socket, address = self.waiting_clients.pop(0)
                    self.executor.submit(self.handle_client, client_socket, address)
            else:
                # If max_clients limit reached, wait for a client to disconnect
                continue

    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(self.server_address)
        server_socket.listen()

        print(f"Server listening on {self.server_address[0]}:{self.server_address[1]}")

        threading.Thread(target=self.client_handler, daemon=True).start()

        while True:
            client_socket, address = server_socket.accept()
            with self.lock:
                if len(self.connected_clients) < self.max_clients:
                    self.connected_clients.append((client_socket, address))
                    self.executor.submit(self.handle_client, client_socket, address)
                else:
                    # If max_clients limit reached, add client to waiting list
                    self.waiting_clients.append((client_socket, address))

    def stop(self):
        self.executor.shutdown(wait=False)

def main():
    server = Server()
    try:
        server.start()
    except KeyboardInterrupt:
        print("Stopping server...")
        server.stop()

if __name__ == "__main__":
    main()
