import requests
import logging
import time


class MarketoClient:

    """Basic Marketo Client"""

    def __init__(self, identity, client_id, client_secret, api):
        self.api_endpoint = api
        self.identity_endpoint = identity
        self.client_id = client_id
        self.client_secret = client_secret
        self.api_version = "v1"
        self._fields = None
        self._session = requests.Session()

        self.refresh_auth_token()

    def refresh_auth_token(self):
        auth_url = "%s/oauth/token?grant_type=client_credentials" % (
            self.identity_endpoint)
        auth_url += "&client_id=%s&client_secret=%s" % (self.client_id,
                                                        self.client_secret)
        debug("Calling %s" % auth_url)
        r = requests.get(auth_url)
        r.raise_for_status()

        auth_data = r.json()
        log("Access token acquired: %s expiring in %s" %
            (auth_data['access_token'], auth_data['expires_in']))
        self.auth_token = auth_data['access_token']

    @property
    def fields(self):
        if self._fields is None:
            res = "leads/describe.json"
            fields = self.auth_get(res)["result"]
            fields = [f["rest"]["name"] for f in fields]
            self._fields = fields

        return self._fields

    def get_paging_token(self, since):
        """
        Get a paging token.
        Format expeced: 2014-10-06.
        """
        resource = "activities/pagingtoken.json"
        params = {"sinceDatetime": since}

        data = self.auth_get(resource, params)
        return data["nextPageToken"]

    def get_leadchanges(self, since, fields):
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

    def query_leads(self, query, return_fields=None):
        """Query leads by any parameters.
        query: dict of fields / value to query on
        return fields: array of which fields should be requested from marketo
        """
        resource = "leads.json"
        params = {
          "filterType": ",".join(query.keys()),
          "filterValues": ",".join(query.values())}
        if return_fields is not None:
            params["fields"] = return_fields

        data = self.auth_get(resource, params=params)
        return data["result"]

    def build_resource_url(self, resource):
        res_url = "%s/%s/%s" % (self.api_endpoint, self.api_version, resource)
        return res_url

    def auth_get(self, resource, params=[], page_size=None):
        """
        Make an authenticated GET to Marketo, check success and
        return dict from json response.
        page_size: page size, max and default 300
        """

        headers = {"Authorization": "Bearer %s" % self.auth_token}
        if page_size is not None:
            params['batchSize'] = page_size

        res_url = self.build_resource_url(resource)
        r = self._session.get(res_url, headers=headers, params=params)
        r.raise_for_status()
        data = r.json()

        if data["success"] is False:
            err = data["errors"][0]
            raise Exception("Error %s - %s, calling %s" %
                            (err["code"], err["message"], r.url))
        time.sleep(20/80)
        return data


class Lead(object):

    def __init__(self, client, id):
        self._client = client
        self._resource = "leads.json"
        self.id = id
        self._data_cache = None
        self._default_fields = None

    def __getattr__(self, name):
        log("Looking for %s" % name)
        if name not in self.fields:
            raise AttributeError

        if name in self._data:
            return self._data[name]
        elif name in self.fields:
            self._load_data(name)
            return self._data[name]
        else:
            raise AttributeError

    @property
    def fields(self):
        return self._client.fields

    @property
    def _data(self):
        if self._data_cache is None:
            if self._default_fields is not None:
                self._load_data(self._default_fields)
            else:
                self._load_data()

        return self._data_cache

    def _load_data(self, fields=None):
        "Load lead data for fields provided, or use default fields."
        resource = "leads/%s.json" % (self.id)

        params = {}
        if fields is not None:
            if type(fields) is str:
                fields = [fields]
            params = {"fields": ",".join(fields)}

        result = self._client.auth_get(resource, params)["result"][0]
        if self._data_cache is not None:
            newdata = self._data_cache.copy()
            newdata.update(result)
            self._data_cache = newdata
        else:
            self._data_cache = result



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

class PagedMarketoResult:

    def __init__(self, client, resource, since, fields, page_size):
        self.resource = resource
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
        debug("fetching next page")
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


def debug(msg):
    logger = logging.getLogger(__name__)
    logger.debug(msg)

def log(msg):
    logger = logging.getLogger(__name__)
    logger.info(msg)
