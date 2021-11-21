class ReadDataException(Exception):
    """Base class for other exceptions"""
    # def __init__(msg, *args, **kwargs):
    #     super().__init__(msg, *args, **kwargs)

    def __init__(self, message, *args, **kwargs):
        super().__init__(message, *args, **kwargs)

class FileDataException(Exception):
    def __init__(self, message):
        super().__init__(message)
