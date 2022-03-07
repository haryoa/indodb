"""
TSV file module
"""

from typing import List, Optional

import pandas as pd
from indodb.data_transformer.artifacts.transformer_artifact import TransformerArtifacts


class TSVFileTransformer:  # pylint: disable=too-few-public-methods
    """
    Transform TSV file!
    """

    def __init__(
        self,
        filepath: str,
        output_path: str,
        no_header: bool = False,
        header_columns: Optional[List[str]] = None,
    ) -> None:
        """
        CSV file ingester and transform them to parquet form

        Parameters
        ----------
        filepath : str
            File path of the csv file
        output_path : str
            Output path of the file
        no_header : bool, optional
            Whether the tsv has header or not, by default False
        header_columns : Optional[List[str]], optional
            Header columns substition, by default None
        """
        self.filepath = filepath
        self.output_path = str(output_path)
        self.no_header = no_header
        self.header_columns = header_columns

    def __call__(self) -> TransformerArtifacts:
        """
        Convert CSV file to parquet one

        Returns
        -------
        DataFetcherArtifacts
            Data Artifact that will be passed to the next pipeline
        """
        # We will use pandas
        header = None if self.no_header else "infer"
        pd_df = pd.read_csv(
            self.filepath, sep="\t", header=header, names=self.header_columns
        )

        # output it to parquet
        pd_df.to_parquet(self.output_path, index=False)
        return TransformerArtifacts(output_path=self.output_path)
