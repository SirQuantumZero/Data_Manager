# src/database/__init__.py
from .core import Database, Core
from .operations import Operations 
from .migrations import Migrations
from .monitoring import Monitoring
from .config import Config

class Database:
    def __init__(self):
        self.config = Config()
        self.core = Core(self.config)
        self.operations = Operations(self.core)
        self.migrations = Migrations(self.core)
        self.monitoring = Monitoring(self.core)