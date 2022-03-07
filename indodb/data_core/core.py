"""
Core data module
"""
import json
from pathlib import Path
from typing import List, Union

import pandas as pd
from datasets import Dataset as HuggingfaceDataset
from indodb.exception import DataFormatNotAllowed

# Allowed datatype to be converted
TO_ALLOWED_ALL = ["labelled_dataset"]  # Allowed in every format
TO_ALLOWED_PANDAS: List[str] = []  # allowed to be converted to pandas
TO_ALLOWED_HF_DATASET: List[str] = []  # allowed to be converted to hf dataset


class IDataset:
    """
    IDataset class
    """

    def __init__(
        self, data_id: str, data_segment: str, data_dir: Union[str, Path]
    ) -> None:
        """
        IDataset is a class that prepare data to be used to some format
        such as Pandas, Vaex, and huggingface datasets.

        Use to_(x) (`pandas`) to export the data to pandas


        Parameters
        ----------
        data_id : str
            Data identifier according to the database
        data_segment : str
            Segment of the data.
        data_dir : str
            Data directory of your cache database
        """
        self.data_id = data_id
        self.data_segment = data_segment
        self.data_path = Path(data_dir) / data_id

        self.parquet_file = self.data_path / f"{data_segment}.parquet"
        if not self.parquet_file.is_file():
            raise FileNotFoundError(f"File {self.parquet_file} is not found!")
        with open(self.data_path / "metadata.json", "r+", encoding="utf-8") as file:
            self.metadata = json.load(file)["data"]

    def to_pandas(self) -> pd.DataFrame:
        """
        Export the data into pandas format
        """
        if self.metadata.get(self.data_segment).get("datatype") in TO_ALLOWED_ALL:
            parq_df = pd.read_parquet(self.parquet_file)
        else:
            raise DataFormatNotAllowed(
                f"{self.data_id} is not allowed to be used in Pandas format."
            )
        return parq_df

    def to_hf_datasets(self) -> HuggingfaceDataset:
        """
        Export the data into huggingface datasets
        """
        if self.metadata.get(self.data_segment).get("datatype") in TO_ALLOWED_ALL:
            parq_data: HuggingfaceDataset = HuggingfaceDataset.from_parquet(
                str(self.parquet_file)
            )
        else:
            raise DataFormatNotAllowed(
                f"{self.data_id} is not allowed to be used in"
                "huggingface dataset format."
            )
        return parq_data
