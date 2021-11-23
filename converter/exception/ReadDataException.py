class ReadDataException(Exception):
    def __init__(self, message, *args, **kwargs):
        super().__init__(message, *args, **kwargs)

class FileDataException(Exception):
    def __init__(self, message):
        super().__init__(message)
