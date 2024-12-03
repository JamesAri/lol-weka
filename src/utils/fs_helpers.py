import os
from typing import List


def get_filepaths_from_dir(directory: str) -> List[str]:
    """
    Returns a list of filepaths from a given directory (`directory` included in the path).
    """
    if not os.path.isdir(directory):
        raise ValueError(f"[!] The directory {directory} does not exist.")
    filepaths = [os.path.join(directory, filename) for filename in os.listdir(directory) if os.path.isfile(os.path.join(directory, filename))]
    return sorted(filepaths)
