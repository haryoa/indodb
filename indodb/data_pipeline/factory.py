"""
Factory on data pipelining
"""
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional, Union

from indodb.data_fetcher.artifacts.fetcher_artifact import DataFetcherArtifacts
from indodb.data_fetcher.db_fetcher import fetch_data_meta
from indodb.data_fetcher.implementation.url_downloads import UrlDownloadFetcher
from indodb.data_transformer.implementation.csv_file import CSVFileTransformer
from indodb.data_core.core import IDataset
from indodb.data_transformer.implementation.tsv_file import TSVFileTransformer


CACHE_DEFAULT_LOCATION = Path.home() / ".indodb_cache/data/"
JSON_WEB_LOCATION = (
    "https://raw.githubusercontent.com/haryoa/indodb-list/main/database.json"
)

map_formatter: Dict[str, Any] = {
    "csv_data": CSVFileTransformer,
    "tsv_data": TSVFileTransformer,
}
map_downloader: Dict[str, Any] = {"url_download": UrlDownloadFetcher}


def prepare_cache_data(
    data_meta: Dict[str, Any], cache_location_project_dir: str
) -> None:
    """
    Prepare cache data (downloading them and put it to its proper
    location)

    Parameters
    ----------
    data_meta : Dict[str, Any]
        data meta from the database
    cache_location_project_dir : str
        Cache directory
    """
    path = Path(cache_location_project_dir)
    os.makedirs(path, exist_ok=True)
    with open(path / "metadata.json", "w+", encoding="utf-8") as file:
        json.dump(data_meta, file, indent=4)
    for key, value in data_meta.get("data", {}).items():
        output_path_file = path / f"{key}.parquet"

        if not output_path_file.is_file():
            print(f"Downloading {key} for {path.name}")
            print(f"Output to {str(path)}")

            temp_file = str(path / f"{key}.tmp")

            # Download and output it to temporary location
            obj_preprocesser = map_downloader[value["preprocess"]](
                url=value["source"], output_location=temp_file
            )

            obj_formatter_args = value.get("source_args", {})
            out_artifact: DataFetcherArtifacts = obj_preprocesser()
            # Transform the file to parquet file format
            obj_formatter = map_formatter[value["source_format"]](
                filepath=out_artifact.file_location,
                output_path=output_path_file,
                **obj_formatter_args,
            )
            obj_formatter()
            os.remove(temp_file)


def load_data(
    data_id: str,
    data_segment: str,
    cache_location: Optional[Union[str, Path]] = None,
    source_db_type: str = "url",
    source_db_location: Optional[str] = None,
) -> IDataset:
    """
    Load defined data. Download data if it's not available in the cache folder.
    It will download ALL SEGMENT OF THE DATA.

    it will use IDB_CACHE_DATASET environment variable if `cache_location` is not
    provided.

    source_db_location will get API response from the IDB_DB_URL environment variable
    if source_db_type is `url`

    Parameters
    ----------
    data_id : str
        Data that you want to download. For more details, visit xxx
    data_segment : str
        Data segment from the data_id (e.g.: "train", "test", "dev")
    cache_location : Union[os.PathLike, str, Path]
        Cache location that will be used to place downloaded dataset.
    source_db_type : str
        Database fetch type. Possible value is `url` and `json_file`.
        `url`: fetch json from this URL in `source_db_location`
        `json_file`: Local JSON data file that is located in `source_db_location`
    source_db_location : str
        Location to fetch based on `source_db_type`.
        It will be downloaded to the location
    """
    if cache_location is None:
        cache_location = os.getenv("IDB_CACHE_DATASET")
        if cache_location is None:
            # Default location
            cache_location = CACHE_DEFAULT_LOCATION
    if source_db_location is None:
        source_db_location = os.getenv("IDB_DB_URL")
        if source_db_location is None:
            source_db_location = JSON_WEB_LOCATION
            source_db_type = "url"

    # path_file = Path(cache_location) / f"{data_id}/metadata.json"

    # In the future check if metadata is different!
    # if not path_file.is_file():
    data_meta = fetch_data_meta(
        source_db_type=source_db_type,
        source_db_location=source_db_location,
        data_id=data_id,
    )

    # Download based on metadata
    prepare_cache_data(data_meta, str(Path(cache_location) / f"{data_id}"))

    data = IDataset(data_id=data_id, data_segment=data_segment, data_dir=cache_location)

    return data
