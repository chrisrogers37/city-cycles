# Import models for easier access
from .london_bike import LondonLegacyBikeShareRecord, LondonModernBikeShareRecord, get_london_model_class
from .nyc_bike import NYCLegacyBikeShareRecord, NYCModernBikeShareRecord, get_nyc_model_class

__all__ = [
    'NYCModernBikeShareRecord',
    'NYCLegacyBikeShareRecord',
    'get_nyc_model_class'
] 