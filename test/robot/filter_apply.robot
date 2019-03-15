*** Settings ***
Resource  common.robot

*** Variables ***
${settings}     {"indexedFields": ["str", "num", "numlist"]}
${index}  {"name" : "testindex", "numShards": 5}


*** Test Cases ***
Create a new application
    Set Headers  ${header}
    POST         /1/applications    ${app}
    Integer     response status     200

Create a new index
    Set Headers  ${appheader}
    POST        /1/indexes         ${index}
    Integer     response status     200

Configure the index
    Set Headers  ${appheader}
    POST        /1/indexes/testindex/settings         ${settings}
    Integer     response status     200

Load some data
    @{json_data}  Set Variable   
       ...  [
       ...   {"str": "a", "num": 1, "numlist": [1,2,3]},
       ...   {"str": "a", "num": 2, "numlist": [2,3,4]},
       ...   {"str": "a", "num": 3, "numlist": [3,4,5]},
       ...   {"str": "a", "num": 4, "numlist": [4,5,6]},
       ...   {"str": "a", "num": 5, "numlist": [5,6,7]}
       ...  ]
    ${json_str}     Catenate    @{json_data}

    Set Headers  ${appheader}
    POST         /1/indexes/testindex   ${json_str}
    Integer     response status     200
    Wait Until Keyword Succeeds	100x	10ms   No Jobs

Test an empty query
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"q": ""}
    Integer     response status     200
    Integer     $.totalHits         5

Test query numeq filter
    POST         /1/indexes/testindex/query  {"q": "", "filter": {"num": 5}}
    Integer     response status     200
    Integer     $.totalHits         1
    POST         /1/indexes/testindex/query  {"q": "", "filter": {"numlist": 5}}
    Integer     response status     200
    Integer     $.totalHits         3
    POST         /1/indexes/testindex/query  {"q": "", "filter": {"numlist": {"$eq": 5}}}
    Integer     response status     200
    Integer     $.totalHits         3

Test query numeq non matching filter
    POST         /1/indexes/testindex/query  {"q": "", "filter": {"num": 500}}
    Integer     response status     200
    Integer     $.totalHits         0

Delete the index
    Set Headers  ${appheader}
    DELETE      /1/indexes/testindex
    Integer     response status     200

Delete the application
    Set Headers  ${header}
    DELETE      /1/applications/appfortests
    Integer     response status     200

