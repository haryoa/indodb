from os import PathLike
from pathlib import Path
import tempfile


def get_temp_path() -> Path:
    """
    GEt temporrary directory
    """
    get_temp_dir = tempfile.gettempdir()
    dir_path = Path(get_temp_dir) / Path("indodb/test")
    return dir_path
