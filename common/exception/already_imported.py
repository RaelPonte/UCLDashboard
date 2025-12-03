class AlreadyImportedError(Exception):
    def __init__(self):
        self.message = "This file has already been imported."
    
    def __str__(self):
        return self.message