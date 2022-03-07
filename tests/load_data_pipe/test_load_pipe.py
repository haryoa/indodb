# pylint: disable=all
# type: ignore

from indodb.data_core.core import IDataset
import os
from pathlib import Path
import pandas as pd
from indodb.data_pipeline.factory import load_data


def test_data_factory():
    # Assume it is already created by the downloader and data processer
    data = IDataset(
        data_id="dummy",
        data_segment="test",
        data_dir="tests/dummy_data/cache_test/",
    )
    data.data_id == "dummy_data/csv_data"
    df = data.to_pandas()
    assert df.shape[0] == 3
    hf_df = data.to_hf_datasets()
    assert hf_df.shape == (3, 2)


def test_full_load_data(tmp_path):
    """
    Testing use case of loading dataset till the
    data go out
    """
    temp_data = tmp_path / "cache"
    try:
        # If exists, remove first
        os.rmdir(temp_data)
    except FileNotFoundError:
        pass
    os.makedirs(temp_data, exist_ok=True)
    data = load_data(
        data_id="dummy_data/csv_data",
        data_segment="test",
        cache_location=temp_data,
        source_db_type="json_file",  # Expected web_api (default) or (json_file)
        source_db_location="tests/dummy_data/db.json",
    )
    # PIPELINE:
    # - Download data and create parquet if not exists
    # - Create dataset factory to produce the class.
    # Create Dataset Factory
    # Expected URL or FILE ( default is None)
    data_df = data.to_pandas()  # return data_df

    assert isinstance(data_df, pd.DataFrame)
    # dataset will be loaded as pandas
    # check its cache file if exists
    assert Path(temp_data / "dummy_data/csv_data/test.parquet").is_file()
    assert Path(temp_data / "dummy_data/csv_data/metadata.json").is_file()
    assert data_df.shape[0] == 5
