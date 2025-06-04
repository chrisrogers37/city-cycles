# Import models for easier access
from .london_bike import LondonLegacyBikeShareRecord, LondonModernBikeShareRecord
from .nyc_bike import NYCLegacyBikeShareRecord, NYCModernBikeShareRecord

__all__ = [
    'NYCModernBikeShareRecord',
    'NYCLegacyBikeShareRecord'
] 