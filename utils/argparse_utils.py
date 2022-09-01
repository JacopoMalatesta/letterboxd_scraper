from utils.generic_scraping_functions import ParsingTechnique, ParallelTechnique


def format_parsing_argument(parsing: str) -> ParsingTechnique:
    if parsing == "synchronous":
        return ParsingTechnique.SYNCHRONOUS
    elif parsing == "asynchronous":
        return ParsingTechnique.ASYNCHRONOUS


def format_soupification_argument(soupification: str) -> ParallelTechnique:
    if soupification == "multiprocessing":
        return ParallelTechnique.MULTIPROCESSING
    elif soupification == "multithreading":
        return ParallelTechnique.MULTITHREADING
    elif soupification == "synchronous":
        return ParallelTechnique.SYNCHRONOUS
    else:
        raise ValueError("Invalid value supplied to the --soupification argument")
