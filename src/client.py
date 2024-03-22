import socket

BUFFER_SIZE = 1024

class Client:
    def __init__(self, server_address: str = "127.0.0.1", server_port: int = 12345) -> None:
        self.server_address = (server_address, server_port)

    def analyse_file(self, file_path: str) -> bytes:
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect(self.server_address)
            client_socket.sendall(b"ANALYSE_NOW")

            # Send file size
            file_size = str(len(open(file_path, 'rb').read()))
            client_socket.sendall(file_size.encode())

            # Send file data
            with open(file_path, 'rb') as file:
                while True:
                    data = file.read(BUFFER_SIZE)
                    if not data:
                        break
                    client_socket.sendall(data)

            # Receive and return the report
            report = client_socket.recv(BUFFER_SIZE)
            return report

        finally:
            client_socket.close()

def main():
    client = Client()

    file_path = input("Enter the file path to analyze: ")
    report = client.send_file(file_path)

    print("Report received:")
    print(report.decode())

if __name__ == "__main__":
    main()
