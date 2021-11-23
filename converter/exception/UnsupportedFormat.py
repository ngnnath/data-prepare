class UnsupportedFormatException(Exception):
    def __init__(self, message):
        super().__init__(message)

class InvalidCsvException(Exception):
    def __init__(self, message):
        super().__init__(message)


class DirectoryNotFoundException(Exception):
    def __init__(self, message):
        super().__init__(message)