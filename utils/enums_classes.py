from enum import Enum


class ParallelTechnique(Enum):
    MULTIPROCESSING = "multiprocessing"
    MULTITHREADING = "multithreading"
    SYNCHRONOUS = "synchronous"


class ParsingTechnique(Enum):
    SYNCHRONOUS = "synchronous"
    ASYNCHRONOUS = "asynchronous"
