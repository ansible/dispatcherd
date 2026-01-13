class DummySyncConnection:
    def __init__(self):
        self.closed = 0

    def close(self):
        self.closed = 1


class DummyAsyncConnection:
    def __init__(self):
        self.closed = 0

    async def close(self):
        self.closed = 1
