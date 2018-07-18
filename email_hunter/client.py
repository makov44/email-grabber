import requests


class EmailHunterClient:
    def __init__(self, api_key, api_version='v2'):
        self.api_key = api_key
        self.api_version = api_version
        self.base_url = 'https://api.emailhunter.co/{}/'.format(api_version)

    def _make_request(self, url, payload):
        r = requests.get(url, params=payload)
        data = r.json()
        # Raise error if not 200 OK
        r.raise_for_status()

        return data

    def search(self, domain, offset=0, type_=None, limit=100, department=None):
        """
        Returns all the email addresses found using one given domain name, with Email Hunter's sources.
        :param domain: The domain name to check for email addresses
        :return: A list of email addresses, with type and sources
        """
        url = self.base_url + 'domain-search'
        payload = {'api_key': self.api_key, 'domain': domain, 'offset': offset, 'limit': limit}

        if type_:
            payload['type'] = type_

        if department:
            payload['department'] = department

        return self._make_request(url, payload)

    def generate(self, domain, first_name, last_name):
        """
        Guesses the most likely email of a person from their first name, last name, and a domain name
        :param domain: The domain name to search
        :param first_name: The first name of the person to search for.
        :param last_name: The last name of the person to search for.
        :return: Tuple, email, score (int)
        """
        url = self.base_url + 'email-finder'
        payload = {'api_key': self.api_key, 'domain': domain, 'first_name': first_name, 'last_name': last_name}
        data = self._make_request(url, payload)

        return data['email'], data['score']

    def verify(self, email):
        """
        Checks if a given email address is deliverable and has been found on the internet.
        :param email: the email address you want to check
        :return: Dict
        """
        url = self.base_url + 'email-verifier'
        payload = {'api_key': self.api_key, 'email': email}
        data = self._make_request(url, payload)

        return data