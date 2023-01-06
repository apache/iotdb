from iotdb.service.server import MLNodeServer


if __name__ == "__main__":
    server = MLNodeServer()
    server.serve()