import os
import sys
import pandas as pd
from typing import Type, List
from datetime import datetime
import numpy as np

class BaseBikeShareRecord:
    staging_table: str = None
    s3_prefix: str = None
    _registry: List[Type['BaseBikeShareRecord']] = []

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls not in BaseBikeShareRecord._registry:
            BaseBikeShareRecord._registry.append(cls)

    @classmethod
    def validate_schema(cls, df: pd.DataFrame) -> bool:
        """Validate if the dataframe contains all required columns.
        
        This method should be implemented by subclasses to check if the dataframe
        contains the expected columns. Type validation is handled during transformation.
        """
        raise NotImplementedError("Subclasses must implement validate_schema")

    @classmethod
    def to_dataframe(cls, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Transform raw dataframe into standardized model format.
        
        This method should be implemented by subclasses to:
        1. Rename columns to match model's field names
        2. Convert data types as needed
        3. Add source_file column
        4. Return only the columns defined in the model
        """
        raise NotImplementedError("Subclasses must implement to_dataframe") 