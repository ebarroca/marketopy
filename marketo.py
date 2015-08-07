import requests
from util import *


class MarketoClient:

    """Basic Marketo Client"""

    def __init__(self, identity, client_id, client_secret, api):
        self.api_endpoint = api
        self.identity_endpoint = identity
        self.client_id = client_id
        self.client_secret = client_secret
        self.api_version = "v1"

        self.refresh_auth_token()

    def refresh_auth_token(self):
        auth_url = "%s/oauth/token?grant_type=client_credentials&client_id=%s&client_secret=%s" % (
            self.identity_endpoint, self.client_id, self.client_secret)
        debug("Calling %s" % auth_url)
        r = requests.get(auth_url)
        r.raise_for_status()

        auth_data = r.json()
        log("Access token acquired: %s expiring in %s" %
            (auth_data['access_token'], auth_data['expires_in']))
        self.auth_token = auth_data['access_token']

    def get_paging_token(self, since):
        """
        Get a paging token.
        Format expeced: 2014-10-06.
        """
        resource = "activities/pagingtoken.json"
        params = {"sinceDatetime": since}

        data = self.auth_get(resource, params)
        return data["nextPageToken"]

    def get_leadchanges(self, fields, since):
        """
        Get lead changes.
        Params: fields = ["company", "score", "firstName"]
        """
        return LeadChangeSet(self, since, fields, page_size=300)

    def get_lead_by_id(self, id, fields=None):
        """Get a lead by its ID"""
        resource = "lead/%i.json" % id
        data = self.auth_get(resource)

        return data

    def get_leads_by_id(self, ids, fields=None):
        params = {"filterType": "id",
                  "filterValues": ",".join(ids),
                  "fields": ",".join(fields)
                  }
        resource = "leads.json"

        data = self.auth_get(resource, params=params)
        return data["result"]

    def build_resource_url(self, resource):
        res_url = "%s/%s/%s" % (self.api_endpoint, self.api_version, resource)
        return res_url

    def auth_get(self, resource, params=[], page_size=None):
        """
        Make an authenticated GET to Marketo
        page_size: page size, max and default 300
        """

        headers = {"Authorization": "Bearer %s" % self.auth_token}
        if page_size is not None:
            params['batchSize'] = page_size

        res_url = self.build_resource_url(resource)
        r = requests.get(res_url, headers=headers, params=params)
        r.raise_for_status()
        data = r.json()

        if data["success"] is False:
            err = data["errors"][0]
            raise Exception("Error %s - %s, calling %s" %
                            (err["code"], err["message"], r.url))

        return data


class BaseResource(object):
    RESOURCE = "resources"

    def __init__(self, client, id):
        self._client = client
        self._resource = self.RESOURCE
        self.id = id
        self._data_cache = None
        self.url = None
        self._fields = None

        if self._resource not in self._client.fields:
            self._client.load_fields_for_resource(self._resource)

    def __getattr__(self, name):

        if self.HAS_CUSTOM_FIELDS and name in self._field_names:
            attr = self._field_names[name]
        else:
            attr = name

        if attr in self._data:
            value = self._data[attr]
            return value
        else:
            raise AttributeError

    @property
    def _data(self):
        if self._data_cache is None:
            resource = "%s/%s.json" % (self.RESOURCE_SEGMENT, self.id)
            data = self.auth_get(resource)
            self.url = r.url
            self._data_cache = data

        return self._data_cache


class Lead(BaseResource):
    RESOURCE_SEGMENT = "leads"


class LeadChangeSet:

    """
    REST Resource: activities/leadchanges.json
    Represent a set of changed leads, only taking into account changed leads,
    not new leads.
    TODO: handle new leads
    """

    def __init__(self, client, since, fields, page_size):
        self.resource = "activities/leadchanges.json"
        self.client = client
        self.since = since
        self.fields = fields
        self.page_size = page_size
        self.has_more_result = False
        self.next_page_token = None
        self.changes = []
        self.fetch_next_page()

    def __iter__(self):
        return self

    def __next__(self):
        if len(self.changes) == 0 and not self.has_more_result:
            raise StopIteration

        if len(self.changes) == 0 and self.has_more_result:
            self.fetch_next_page()

        return self.changes.pop(0)

    def fetch_next_page(self):
        debug("[mkto] Fetching next page for LeadChangeSet")
        if self.next_page_token is None:
            self.next_page_token = self.client.get_paging_token(
                since=self.since)

        params = {
            "fields": ','.join(self.fields),
            "nextPageToken": self.next_page_token}

        data = self.client.auth_get(self.resource, params, self.page_size)

        # If moreResult is true, set flag on object and next page token, if
        # not, reset them
        if data["moreResult"]:
            self.has_more_result = True
            self.next_page_token = data["nextPageToken"]
        else:
            self.has_more_result = False
            self.next_page_token = None

        for lead in self.prepare_results(data["result"]):
            self.changes.append(lead)

    def prepare_results(self, results):
        """
        Iterates over change results and output an
        array with changed fields and values
        """
        for c in results:
            changed_fields = {}
            changed_fields["id"] = c['leadId']

            # if no fields updated -> new lead -> skip
            if len(c["fields"]) == 0:
                continue

            for f in c["fields"]:
                changed_fields[f["name"]] = f["newValue"]
            yield changed_fields
