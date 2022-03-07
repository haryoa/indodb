"""
Transformer Output (Artifacts)
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class TransformerArtifacts:
    """
    Parameters
    ----------
    output_path: str
        output path of the transformed data
    """

    output_path: Optional[str] = None
