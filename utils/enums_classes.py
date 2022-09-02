from enum import Enum


class ParallelTechnique(Enum):
    MULTIPROCESSING = "multiprocessing"
    MULTITHREADING = "multithreading"
    SYNCHRONOUS = "synchronous"

