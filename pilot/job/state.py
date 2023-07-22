from enum import Enum


class State(Enum):
    UNKNOWN = 1
    PENDING = 2
    RUNNING = 3
    FAILED = 4
    DONE = 5
