# coding=utf-8

"""
Pyccuweather
The Python Accuweather API

connector.py
Basic connector object and methods

(c) Chris von Csefalvay, 2015.
"""

import requests
from pyccuweather import errors
from pyccuweather.froots import froot
from pyccuweather.objects import *
import os


class Connection(object):
    """
    Represents a connection to the Accuweather API.

    :param API_KEY: API key
    :param api_type: whether the enterprise api system ('dev' - apidev.accuweather.com or 'production' - api.accuweather.com) 
    or developer api portal - dataservice.accuweather.com is used
    :param retry: number of retries of failed operations - TODO: implement
    :raise errors.MalformattedAPIKeyError: if the API key is not a 32-character string, an error is thrown
    """

    def __init__(self, API_KEY: str=None, api_type: str='apidev', retry: int=3):

        # TODO: implement retries

        self.API_TYPES = ('api', 'apidev', 'dataservice')

        try:
            self.API_KEY = os.environ["ACCUWEATHER_APIKEY"]
        except KeyError:
            try:
                assert isinstance(API_KEY, str)
                assert len(API_KEY) is 32
                self.API_KEY = API_KEY
            except AssertionError:
                raise errors.MalformattedAPIKeyError()

        self.api_type = api_type if api_type in self.API_TYPES else 'apidev'

        self.API_ROOT = "http://{api_type}.accuweather.com".format(api_type=self.api_type)
        self.API_VERSION = "v1"
        self.retries = retry

    def __str__(self):
        return u"Accuweather connector to {0:s}".format(self.API_ROOT)

    def wipe_api_key(self):
        """
        Wipes API key from a Connection instance
        :return: void
        """
        self.API_KEY = None

    ########################################################
    # Location resolvers                                   #
    ########################################################

    def loc_geoposition(self, lat: float, lon: float):
        """
        Resolves location based on geoposition.

        :param lat: latitude
        :param lon: longitude
        :return: Location object
        """

        try:
            assert isinstance(lat, (int, float)) and isinstance(lon, (int, float))
        except:
            raise ValueError

        try:
            assert abs(lat) <= 90 and abs(lon) <= 180
        except:
            raise errors.RangeError(lat, lon)

        payload = {"q": u"{0:.4f},{1:.4f}".format(lat, lon)}
        resp = self.handle_request('loc_geoposition', payload=payload)

        assert len(resp) > 0

        if isinstance(resp, list):
            return Location(resp[0])
        elif isinstance(resp, dict):
            return Location(resp)

    def loc_string(self, search_string: str, country_code: str=None):
        """
        Resolves a search string and an optional country code to a location.

        :param search_string: search string
        :param country_code: country code to which the search will be limited
        :return: a LocationSet of results
        """
        
        payload = {"q": search_string}
        fargs = {}

        if country_code is not None:
            try:
                assert len(country_code) is 2
            except:
                raise errors.InvalidCountryCodeError(country_code)

            fkeyid = 'loc_search_country'
            fargs = {'country_code': country_code}

        else:
            fkeyid = 'loc_search'


        resp = self.handle_request(fkeyid, fargs=fargs, payload=payload)

        _result = list()
        if len(resp) > 0:
            for each in resp:
                loc = Location(lkey=each["Key"],
                               lat=each["GeoPosition"]["Latitude"],
                               lon=each["GeoPosition"]["Longitude"],
                               localized_name=each["LocalizedName"],
                               english_name=each["EnglishName"],
                               region=each["Region"],
                               country=each["Country"],
                               administrative_area=each["AdministrativeArea"],
                               timezone=each["TimeZone"]
                               )
                _result.append(loc)
        else:
            raise errors.NoResultsError(search_string)

        return (LocationSet(results=_result,
                            search_expression=search_string,
                            country=country_code))

    def loc_postcode(self, country_code: str, postcode: str):
        """
        Resolves location based on postcode. Only works in selected countries (US, Canada).

        :param country_code: Two-letter country code
        :param postcode: Postcode
        :return: Location object
        """

        try:
            assert len(country_code) is 2
        except:
            raise errors.InvalidCountryCodeError(country_code)

        resp = self.handle_request('loc_postcode', 
                                    fargs={'country_code': country_code}, 
                                    payload={'q': postcode})

        assert len(resp) > 0

        if isinstance(resp, list):
            return Location(resp[0])
        elif isinstance(resp, dict):
            return Location(resp)

    def loc_ip(self, ip_address:str):
        """
        Resolves location based on IP address.

        :param ip_address: IP address
        :return: Location object
        """

        payload = {"q": ip_address}
        resp = self.handle_request('loc_ip_address', payload=payload)

        assert len(resp) > 0

        if isinstance(resp, list):
            return Location(resp[0])
        elif isinstance(resp, dict):
            return Location(resp)

    def loc_lkey(self, lkey:int):
        """
        Resolves location by Accuweather location key.

        :param lkey: Accuweather location key
        :return: Location object
        """

        fargs = {'location_key': lkey}
        resp = self.handle_request('loc_lkey', fargs=fargs)

        assert len(resp) > 0

        if isinstance(resp, list):
            return Location(resp[0])
        elif isinstance(resp, dict):
            return Location(resp)

    ########################################################
    # Current conditions                                   #
    ########################################################

    def get_current_wx(self, lkey:int=None, location:Location=None, current:int=0, details:bool=True):
        """
        Get current weather conditions.

        :param lkey: Accuweather location key
        :param location: Location object
        :param current: horizon - current weather, 6 hours or 24 hours
        :param details: should details be provided?
        :return: raw observations or CurrentObs object
        """

        assert current in [0, 6, 24]
        assert lkey is not None or location is not None

        fargs = {'location_key': lkey}

        if current is 0:
            fkeyid = 'currentconditions'
        else:
            fkeyid = 'currentconditions_{current}'.format(current=current)

        payload = {"details": "true" if details is True else "false"}

        resp = self.handle_request(fkeyid, fargs=fargs, payload=payload)

        return CurrentObs(resp)

    ########################################################
    # Forecasts                                            #
    ########################################################

    def get_forecast(self, forecast_type:str, lkey:int, details:bool=True, metric:bool=True):
        forecast_types = ["1h", "12h", "24h", "72h", "120h", "240h",
                          "1d", "5d", "10d", "15d", "25d", "45d"]
        assert forecast_type in forecast_types

        fkeyid = u"forecast_{0:s}".format(forecast_type)
        fargs = {'location_key': lkey}
        payload = {"details": "true" if details == True else "false",
                   "metric": "true" if metric == True else "false"}

        resp = self.handle_request(fkeyid, fargs, payload)

        if forecast_type[-1] is "h":
            return HourlyForecasts(resp)
        elif forecast_type[-1] is "d":
            return DailyForecasts(resp)

    ########################################################
    # Air quality                                          #
    ########################################################

    def get_airquality(self, lkey:int, current:bool=True):
        # TODO: Refactor. lkey seemingly contains alpha characters.
        assert isinstance(lkey, int)

        if current:
            fkeyid = "airquality_current"
        else:
            fkeyid = "airquality_yesterday"

        fargs = {'location_key': lkey}

        return self.handle_request(fkeyid, fargs=fargs)

    ########################################################
    # Climo                                                #
    ########################################################

    def get_actuals(self, lkey:int, start_date:str, end_date:str=None):

        # TODO: Return object
        # (needs API access)

        fargs = {'location_key': lkey}

        if end_date:
            fkeyid = "climo_actuals_range"
            payload = {"start": start_date,
                       "end": end_date}
        else:
            fkeyid = "climo_actuals_date"
            fargs['data'] = start_date

        return self.handle_request(fkeyid, fargs=fargs, payload=payload)

    def get_records(self, lkey, start_date, end_date=None):

        # TODO: Return object
        # (needs API access)
        
        fargs = {'location_key': lkey}
        payload = {"start": start_date}

        if end_date:
            fkeyid = "climo_records_range"
            payload["end"] = end_date
        else:
            fkeyid = "climo_records_date"

        return self.handle_request(fkeyid, fargs=fargs, payload=payload)

    def get_normals(self, lkey, start_date, end_date=None):

        # TODO: Return object
        # (needs API access)

        fargs = {'location_key': lkey}
        payload = {'start': start_date}

        if end_date:
            fkeyid = "climo_normals_range"
            payload['end'] = end_date
        else:
            fkeyid = "climo_normals_date"

        return self.handle_request(fkeyid, fargs=fargs, payload=payload)

    ########################################################
    # Alerts                                               #
    ########################################################

    def get_alerts(self, lkey, forecast_range):

        # TODO: Return object
        # (needs API access)

        assert isinstance(forecast_range, int)
        fkeyid = u"alarms_{0:d}d".format(forecast_range)
        fargs = {'location_key': lkey}

        return self.handle_request(fkeyid, fargs=fargs)

    def handle_request(self, fkeyid, fargs={}, payload={}):
        fargs['api_type'] = self.api_type
        url = froot(fkeyid, **fargs)

        payload["apikey"] = self.API_KEY
        resp = requests.get(url=url, params=payload)
        
        if resp.status_code == 403:
            raise errors.UnauthorisedError()

        return resp.json()

