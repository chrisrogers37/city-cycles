from data_models.london_bike import LondonLegacyBikeShareRecord, LondonModernBikeShareRecord
from data_models.nyc_bike import NYCLegacyBikeShareRecord, NYCModernBikeShareRecord
from data_models.weather import HourlyWeatherRecord

MODEL_REGISTRY = [
    LondonLegacyBikeShareRecord,
    LondonModernBikeShareRecord,
    NYCLegacyBikeShareRecord,
    NYCModernBikeShareRecord,
    HourlyWeatherRecord,
] 