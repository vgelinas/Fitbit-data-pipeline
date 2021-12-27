"""
A Fitbit class handling all requests interactions with the Fitbit web API.
See https://dev.fitbit.com/build/reference/web-api/ for details.
"""
import requests
import time
import datetime
from db_tables import FitbitCredentials


class Fitbit:
    """A wrapper class for calling the Fitbit web API via oauth2, handling
    the authentication, rate limiting and token refresh process automatically.

    See https://dev.fitbit.com/build/reference/web-api/ for details.
    """

    def __init__(self, session, seconds_between_calls=1, verbose=False):
        self.session = session
        self.seconds_between_calls = seconds_between_calls
        self.verbose = verbose

        # fetch API credentials from database
        fitbit_credentials = self.session.query(FitbitCredentials).get(1)

        self.client_id = fitbit_credentials.client_id
        self.client_secret = fitbit_credentials.client_secret
        self.access_token = fitbit_credentials.access_token
        self.refresh_token = fitbit_credentials.refresh_token
        self.expires_at = fitbit_credentials.expires_at

        self.session.commit()

    def __wait_for_api_rate_limit_refresh(self):
        """
        Sleep until next hour, plus five minutes to allow the API rate limit to
        refresh. Called when API rate limit is reached.
        """
        offset = 5 	# number of minutes past the hour as offset

        delta = datetime.timedelta(hours=1)
        now = datetime.datetime.now()
        next_hour = (now + delta).replace(microsecond=0, second=0, minute=0)

        # add some minutes to be safe
        next_hour += datetime.timedelta(minutes=offset)

        # sleep until next hour
        if self.verbose:
            time_of_refresh = next_hour.strftime("%H:%M:%S")
            print("Hitting API rate limit. Sleeping until {time}".format(
                                                        time=time_of_refresh))

        sleep_seconds = (next_hour - now).seconds

        # sleep until next hour
        time.sleep(sleep_seconds)

    def __update_static_tokens(self, tokens_dict):
        """
        Updates token data in database. This is for when access_token
        expires and new tokens are fetched from the api.
        """
        fitbit_credentials = self.session.query(FitbitCredentials).get(1)

        fitbit_credentials.access_token = tokens_dict["access_token"]
        fitbit_credentials.refresh_token = tokens_dict["refresh_token"]
        fitbit_credentials.expires_at = tokens_dict["expires_at"]

        self.session.commit()

    def refresh_tokens(self):
        """
        Fetch a new token dictionary from Fitbit API, and refresh the token
        attributes (access_token, refresh_token, expires_at) as well as their
        database instance.
        """

        # Fetch new token dict from Fitbit server
        response = requests.post(url='https://api.fitbit.com/oauth2/token',
                                 data={"client_id": self.client_id,
                                       "grant_type": "refresh_token",
                                       "refresh_token": self.refresh_token
                                       },
                                 auth=(self.client_id, self.client_secret)
                                 )

        # Check if rate limit is reached, if so sleep and try again.
        if response.status_code == 429:  # api rate limit reached
            self.__wait_for_api_rate_limit_refresh()
            self.refresh_tokens()

        elif response.status_code != 200:
            raise Exception(response.status_code)

        else:
            tokens = response.json()

            # Store tokens' time of expiry so we know when to refresh again
            expires_in = float(tokens['expires_in'])
            tokens['expires_at'] = time.time() + expires_in

            # Update everything with new token data
            self.access_token = tokens['access_token']
            self.refresh_token = tokens['refresh_token']
            self.expires_at = tokens['expires_at']
            self.__update_static_tokens(tokens)

    def get_resource(self, url):
        """
        Wrapper for requests.get method, passing along access token. First
        checks if access_token exists or has expired, and refresh it if needed.
        """
        # Check access token is still valid.
        if (not self.expires_at) or (time.time() >= float(self.expires_at)):
            self.refresh_tokens()

        # Wait a few seconds before next call.
        # This is to prevent continuously hitting the rate limit.
        time.sleep(self.seconds_between_calls)

        # Send in request.
        if self.verbose:
            now = datetime.datetime.now().strftime("%H:%M:%S %h %d")
            print("API call at {time} ~ {url}".format(time=now, url=url))

        headers = {'Authorization': 'Bearer {}'.format(self.access_token)}
        response = requests.request('GET', url=url, headers=headers)

        # Check if rate limit is reached, if so sleep and try again.
        if response.status_code == 429:
            self.__wait_for_api_rate_limit_refresh()
            response = self.get_resource(url=url)

        return response
