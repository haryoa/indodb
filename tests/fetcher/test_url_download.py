# pylint: disable=all
# type: ignore

import os
import tempfile
from pathlib import Path

from indodb.data_fetcher.implementation.url_downloads import UrlDownloadFetcher
from tests.utils import get_temp_path


def test_url_download_txt(tmp_path):
    """
    Download a file and check if it exists
    """
    file_location = tmp_path / "test.csv"
    url_downloader = UrlDownloadFetcher(
        output_location=file_location,
        url="https://raw.githubusercontent.com/haryoa/testing_utils/main/text.txt",
    )
    artifact_downloader = url_downloader()
    assert file_location.is_file()
    assert str(artifact_downloader.file_location) == str(file_location)
    os.remove(file_location)
