# -*- coding: utf-8 -*-

import requests


class GeoLocation(object):

  """Wrapper for Google Geocoding API V3."""

  @classmethod
  def get_coordinates(cls, address, region='', language='', api_key=None):
    """ Given a string address, return latitude and longitude."""
    response = cls.geocode(address, region=region, language=language, api_key=api_key)
    if response:
      return response[0]["geometry"]["location"]["lat"], response[0]["geometry"]["location"]["lng"]
    else:
      return None

  @classmethod
  def geocode(cls, address, region='', language='ja', api_key=None):
    """ Given a string address, return full geolocation information."""
    params = {
        'address': address,
        'sensor': 'false',
        'bounds': '',
        'region': region,
        'language': language
    }
    if api_key:
      params['key'] = api_key
    response = cls.fetch_data(params)
    return response

  @classmethod
  def reverse_geocode(cls, lat, lng, language='ja', api_key=None):
    """ Given a string address, return full geolocation information."""
    params = {
        'latlng': '%s,%s' % (lat, lng),
        'language': language
    }
    if api_key:
      params['key'] = api_key
    response = cls.fetch_data(params)
    return response

  @classmethod
  def fetch_data(cls, params={}):
      """Retrieve a JSON object from a (parameterized) URL.

      params: dictionary with the query parameters to use for
              - Geocoding (Latitude/Longitude Lookup)) or
              - Reverse Geocoding (Address Lookup)

      return: JSON object with the data fetched from that URL as a JSON-format object.

      """
      request = requests.Request(
          'GET',
          url='https://maps.google.com/maps/api/geocode/json?',
          params=params,
          headers={'accept-language': params.get('language') or 'ja'})

      session = requests.Session()

      response = session.send(request.prepare())
      session.close()

      if response.status_code == 403:
        return None
      response_json = response.json()

      if response_json['status'] != 'OK':
        return None
      return response_json['results']
