import asyncio
import logging
from prometheus_client import generate_latest, CollectorRegistry

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CustomHttpServer:
    def __init__(self, registry: CollectorRegistry):
        self.server = None
        self.registry = registry

    async def handle_request(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info('peername')
        logging.info(f"Received connection from {addr}")

        request_line = await reader.readline()
        if not request_line:
            writer.close()
            await writer.wait_closed()
            return

        request_line_str = request_line.decode('utf-8').strip()
        logging.info(f"Request: {request_line_str}")

        # Parse the request line (simplified parsing)
        try:
            method, path, _ = request_line_str.split()
        except ValueError:
            logging.warning(f"Could not parse request line: {request_line_str}")
            # Respond with 400 Bad Request for malformed request line
            response_headers = "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            writer.write(response_headers.encode('utf-8'))
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            return

        # Read headers (and ignore them for now)
        while True:
            header_line = await reader.readline()
            if header_line == b'\r\n':
                break

        if method == 'GET' and path == '/metrics':
            try:
                metrics_data = generate_latest(self.registry)
                body = metrics_data.decode('utf-8')
                status_line = "HTTP/1.1 200 OK"
                content_type = "text/plain; version=0.0.4; charset=utf-8"
            except Exception as e:
                logging.error(f"Error generating metrics: {e}")
                body = "Error generating metrics"
                status_line = "HTTP/1.1 500 Internal Server Error"
                content_type = "text/plain; charset=utf-8"
        else:
            # For any other path or method, respond with 404 Not Found
            body = "Not Found"
            status_line = "HTTP/1.1 404 Not Found"
            content_type = "text/plain; charset=utf-8"

        response_headers = f"{status_line}\r\n" \
                           f"Content-Type: {content_type}\r\n" \
                           f"Content-Length: {len(body.encode('utf-8'))}\r\n" \
                           "Connection: close\r\n\r\n"
        
        response = response_headers + body

        writer.write(response.encode('utf-8'))
        await writer.drain()
        
        logging.info(f"Sent {status_line} response to {addr}")
        writer.close()
        await writer.wait_closed()

    async def start(self, host: str, port: int):
        # The registry is now passed in __init__
        try:
            self.server = await asyncio.start_server(
                self.handle_request, host, port)
        except Exception as e:
            logging.error(f"Failed to start server on {host}:{port}: {e}")
            # Potentially re-raise or handle more gracefully if this is critical
            return

        addr = self.server.sockets[0].getsockname()
        logging.info(f'Serving on {addr}')

        async with self.server:
            await self.server.serve_forever()

    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            logging.info("Server stopped.")

async def main():
    # Example usage (optional, can be removed or kept for testing)
    # In a real application, you would import and register your CustomCollector here
    # from dispatcherd.service.metrics import CustomCollector 

    registry = CollectorRegistry()
    # Example: If CustomCollector is defined and ready to be used:
    # custom_collector = CustomCollector() # Assuming it takes no args for simplicity
    # registry.register(custom_collector)
    # logging.info("CustomCollector registered with the registry.")

    # If CustomCollector is not yet available or not used in this scope, 
    # the server will still run and serve default Python metrics if any are registered by default,
    # or an empty metrics page if nothing is registered.
    
    server = CustomHttpServer(registry=registry)
    try:
        logging.info("Starting server with Prometheus metrics endpoint enabled.")
        await server.start('127.0.0.1', 8080)
    except KeyboardInterrupt:
        logging.info("Server shutting down...")
    except Exception as e:
        logging.error(f"Server failed to start or run: {e}")
    finally:
        await server.stop()

if __name__ == '__main__':
    # This part is for direct execution testing.
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Main process interrupted. Shutting down.")
    except Exception as e:
        logging.error(f"An error occurred in main: {e}")
