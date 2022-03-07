# pylint: disable=all
# type: ignore

import os

import vaex
from indodb.data_transformer.implementation.csv_file import CSVFileTransformer
from tests.utils import get_temp_path


def test_transform_csv_file():
    """
    Read CSV and make it to parquet file!
    """
    filepath = "tests/dummy_data/data_tested.csv"
    parquet_path = get_temp_path() / "tmp.parquet"
    csv_file_transformer = CSVFileTransformer(
        filepath=filepath, output_path=parquet_path
    )
    pipe_artifact = csv_file_transformer()
    assert pipe_artifact.output_path == str(parquet_path)

    df_pq = vaex.open(parquet_path)
    os.remove(parquet_path)
    assert df_pq.shape[0] == 3
