"""
URL Downloader module
"""

import shutil
import urllib.request

from indodb.data_fetcher.artifacts.fetcher_artifact import DataFetcherArtifacts


class UrlDownloadFetcher:  # pylint: disable=too-few-public-methods
    """
    URL Downloader Fetcher to fetch URL
    """

    def __init__(self, output_location: str, url: str) -> None:
        """
        Url downloader fetcher that download any file provided in URL.

        Parameters
        ----------
        output_location : str
            The location of the output
        url: str
            URL of the program that will be downloaded
        """
        self.output_location = output_location
        self.url = url

    def __call__(self) -> DataFetcherArtifacts:
        """
        Download the file in the provided URL

        **FUTURE FEATURE**
        1. Add progress bar downloader
        (probably using modified one from
        [here](https://stackoverflow.com/questions/15644964/python-progress-bar-and-downloads)

        Returns
        -------
        DataFetcherArtifacts
            Data Artifact that will be passed to the next pipeline
        """
        # Download the file from `url` and save it locally under `file_name`:
        with urllib.request.urlopen(self.url) as response, open(
            self.output_location, "wb"
        ) as out_file:
            shutil.copyfileobj(response, out_file)
        return DataFetcherArtifacts(
            file_location=self.output_location, file_id=self.url
        )
