class BaseConnector:
    def __init__(self):
        pass

    def connect(self):
        raise NotImplementedError("Subclasses should implement this method")

    def close(self):
        raise NotImplementedError("Subclasses should implement this method")

class NoOpConnector(BaseConnector):
    def __init__(self):
        super().__init__()
        self.client = None

    def connect(self):
        self.client = None

    def close(self):
        pass
