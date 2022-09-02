from utils.enums_classes import ParallelTechnique


def format_soupification_argument(soupification: str) -> ParallelTechnique:
    if soupification == "multiprocessing":
        return ParallelTechnique.MULTIPROCESSING
    elif soupification == "multithreading":
        return ParallelTechnique.MULTITHREADING
    elif soupification == "synchronous":
        return ParallelTechnique.SYNCHRONOUS
    else:
        raise ValueError("Invalid value supplied to the --soupification argument")
