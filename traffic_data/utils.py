from datetime import date
from typing import List, Tuple
import pandas as pd

from .enums import City

class Calendar:
    @staticmethod
    def holidays() -> List[date]:
        holidays = [date(2019, 4, 19), date(2019, 4, 22), date(2019, 5, 1), date(2019, 5, 8), date(2019, 5, 30)]
        return holidays

    @staticmethod
    def fridays_and_saturdays() -> List[date]:
        days = pd.date_range(start='2019-03-16', end='2019-05-31')
        weekends = [day.date() for day in days if day.dayofweek in [4, 5]]
        return weekends


class Anomalies:
    @staticmethod
    def get_anomaly_dates_by_city(city: City):
        if city == City.BORDEAUX:
            anomaly_dates = [date(2019, 4, 9), date(2019, 4, 12), date(2019, 4, 14), date(2019, 5, 12),
                             date(2019, 5, 22), date(2019, 5, 23), date(2019, 5, 24), date(2019, 5, 25)]
        elif city == City.TOULOUSE:
            anomaly_dates = [date(2019, 4, 12), date(2019, 4, 14), date(2019, 5, 12), date(2019, 5, 22),
                             date(2019, 5, 23), date(2019, 5, 24), date(2019, 5, 25)]
        elif city == City.DIJON:
            anomaly_dates = [date(2019, 4, 9), date(2019, 5, 12)]
        else:
            # date 2019-03-31 contains nan values
            anomaly_dates = [date(2019, 3, 31), date(2019, 4, 14), date(2019, 5, 12)]
        return anomaly_dates

    @staticmethod
    def get_all_anomaly_dates():
        anomaly_dates = [date(2019, 4, 9), date(2019, 4, 12), date(2019, 4, 14), date(2019, 5, 12),
                         date(2019, 5, 22), date(2019, 5, 23), date(2019, 5, 24), date(2019, 5, 25)]
        return anomaly_dates


class CityDimensions:
    city_dims = {
        'Bordeaux': (334, 342),
        'Clermont-Ferrand': (208, 268),
        'Dijon': (195, 234),
        'France': (9742, 9588),
        'Grenoble': (409, 251),
        'Lille': (330, 342),
        'Lyon': (426, 287),
        'Mans': (228, 246),
        'Marseille': (211, 210),
        'Metz': (226, 269),
        'Montpellier': (334, 327),
        'Nancy': (151, 165),
        'Nantes': (277, 425),
        'Nice': (150, 214),
        'Orleans': (282, 256),
        'Paris': (409, 346),
        'Rennes': (423, 370),
        'Saint-Etienne': (305, 501),
        'Strasbourg': (296, 258),
        'Toulouse': (280, 347),
        'Tours': (251, 270)
    }

    @classmethod
    def get_city_dim(cls, city) -> Tuple[int, int]:
        city_name = city.value  # Get the city name from the Enum member
        return cls.city_dims.get(city_name)
