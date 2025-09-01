class FileNotFound(Exception):
    def __init__(self, _file_name):
        super().__init__(f"{_file_name} file is missing!")