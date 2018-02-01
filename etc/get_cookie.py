from requests.packages.urllib3.exceptions import InsecureRequestWarning

import sys
import requests
from bs4 import BeautifulSoup
import logging
import datetime
from traitlets.config import LoggingConfigurable
import argparse


requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
requests.packages.urllib3.disable_warnings()  # disable SSL warnings


def main(parsedArgs):

    def login_in_dcos():
        """
        Function that simulates the login in DCOS flow with SSO to obtain a valid
        cookie that will be used to make requests to Marathon
        """

        def obtain_cookie(response, key):
            cookies = [r.cookies for r in response.history if key in r.cookies.keys()]
            if cookies:
                return cookies[-1]
            else:
                message = "Error getting sso token: No '{cookie}' cookie found in responses".format(cookie=key)

        # First request to mesos master to be redirected to gosec sso login page and be given a session cookie
        first_response = requests.get('%s/login?firstUser=false' % parsedArgs.url, verify=False)
        callback_url = first_response.url
        session_id_cookie = obtain_cookie(first_response, 'JSESSIONID')

        # Parse response body for hidden tags needed in the data of our login post request
        body = first_response.text
        parser = BeautifulSoup(body, "lxml")
        hidden_tags = [tag.attrs for tag in parser.find_all("input", type="hidden")]
        data = {tag['name']: tag['value'] for tag in hidden_tags
                if tag['name'] == 'lt' or tag['name'] == 'execution'}

        # Add the rest of needed fields and login credentials in the data of
        # our login post request and send it
        data.update(
            {'_eventId': 'submit',
             'submit': 'LOGIN',
             'username': parsedArgs.username,
             'password': parsedArgs.password}
        )

        login_response = requests.post(callback_url, data=data, cookies=session_id_cookie, verify=False)

        # Obtain dcos cookie from response
        return obtain_cookie(login_response, 'dcos-acs-auth-cookie')

    cookie = login_in_dcos()
    print(dict(cookie)['dcos-acs-auth-cookie'])

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--url")
    parser.add_argument("--username")
    parser.add_argument("--password")

    parsedArgs = parser.parse_args(sys.argv[1:])

    main(parsedArgs)
