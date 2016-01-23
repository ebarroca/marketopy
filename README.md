#Marketopy - Natural Python client for Marketo REST API

Simple Python client for the Marketo REST API.

##Main features
- *Lead* object with natural properties, lazy loaded from Marketo
- Support *Get Lead Changes* to fetch changes from Marketo

##Use

```python
from marketopy import marketo

mkto = marketo.MarketoClient(IDENTITY_ENDPOINT, CLIENT_ID,
                              CLIENT_SECRET, API_ENDPOINT)
#Look for changes on properties leadScore and email
changes = mkto.get_leadchanges(since="2015-08-04", fields=["leadScore", "email"])
for c in changes:
    do something

#Get a lead
myLead = marketo.Lead(mkto, "12345")
print(myLead.email)
print(myLead.myCustomProperty) #fetched lazily and cached

#Update lead data
myLead.firstName = "New Name"
myLead.leadRole = "My Role"
myLead.save()
```

##Coming soon
- Campaign object
