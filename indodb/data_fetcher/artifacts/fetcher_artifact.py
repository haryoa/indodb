"""
Data Fetcher Artifacts!
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class DataFetcherArtifacts:
    """
    Parameters
    ----------
    file_location: str
        Directory of file
    file_id: str
        File identifier whether it's an URL or other form
    """

    file_location: Optional[str] = None
    file_id: Optional[str] = None
