class BaseConnector:
    def __init__(self):
        pass

    def connect(self):
        raise NotImplementedError("Subclasses should implement this method")

    def close(self):
        raise NotImplementedError("Subclasses should implement this method")
