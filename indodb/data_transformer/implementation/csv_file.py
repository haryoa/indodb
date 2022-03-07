"""
CSV file module
"""

import pandas as pd

from indodb.data_transformer.artifacts.transformer_artifact import TransformerArtifacts


class CSVFileTransformer:  # pylint: disable=too-few-public-methods
    """
    Transform CSV file!
    """

    def __init__(
        self, filepath: str, output_path: str, encoding: str = "utf-8"
    ) -> None:
        """
        CSV file ingester and transform them to parquet form
        """
        self.filepath = filepath
        self.output_path = str(output_path)
        self.encoding = encoding

    def __call__(self) -> TransformerArtifacts:
        """
        Convert CSV file to parquet one

        Returns
        -------
        DataFetcherArtifacts
            Data Artifact that will be passed to the next pipeline
        """
        # We will use pandas
        pd_df = pd.read_csv(self.filepath, encoding=self.encoding)

        # output it to parquet
        pd_df.to_parquet(self.output_path, index=False)
        return TransformerArtifacts(output_path=self.output_path)
