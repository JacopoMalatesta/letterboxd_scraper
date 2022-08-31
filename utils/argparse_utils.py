from utils.generic_scraping_functions import ParallelTechnique


def format_how_argument(how: str) -> ParallelTechnique:
    if how == "multiprocessing":
        return ParallelTechnique.MULTIPROCESSING
    elif how == "multithreading":
        return ParallelTechnique.MULTITHREADING
    elif how == "synchronous":
        return ParallelTechnique.SYNCHRONOUS
    else:
        raise ValueError("Invalid value supplied to the --how argument")
