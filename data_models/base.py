import os
import logging
import pandas as pd
from typing import Type, List
from datetime import datetime

logger = logging.getLogger(__name__)


class BaseBikeShareRecord:
    staging_table: str = None
    s3_prefix: str = None
    _required_columns: list = []
    _registry: List[Type['BaseBikeShareRecord']] = []

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls not in BaseBikeShareRecord._registry:
            BaseBikeShareRecord._registry.append(cls)

    @classmethod
    def validate_schema(cls, df: pd.DataFrame) -> bool:
        """Validate if the dataframe contains all required columns.

        Uses cls._required_columns defined in each subclass to check whether
        the given DataFrame has every expected column. Logs missing columns
        at DEBUG level so diagnostics are available without custom env vars.
        """
        missing_columns = [col for col in cls._required_columns if col not in df.columns]
        if missing_columns:
            logger.debug(
                "%s validation failed - missing columns: %s. Available: %s",
                cls.__name__,
                missing_columns,
                list(df.columns),
            )
        return not missing_columns

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