*** Settings ***
Resource  common.robot

*** Variables ***
${settings}     {"indexedFields": ["title"] }
${index}  {"name" : "testindex", "numShards": 1}

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
    POST        /1/indexes/testindex/settings         ${settings}
    Integer     response status     200

Load some data
    @{json_data}  Set Variable   
       ...  [
       ...   {"title": "amazong this amaznger this bmazon this amaozn this amazon this amazion ama-xon this amazing a.m.a.z.o.n here amazos"},
       ...   {"title": "in the amazon middle"},
       ...   {"title": "amazin in the start"},
       ...   {"title": "in the end amazon"}
       ...  ]
    ${json_str}     Catenate    @{json_data}

    Set Headers  ${appheader}
    POST         /1/indexes/testindex   ${json_str}
    Integer     response status     200
    Wait Until Keyword Succeeds	100x	10ms   No Jobs

Test query empty
    POST         /1/indexes/testindex/query  {"explain": true}
    Integer     response status     200

Test query amazon
    POST         /1/indexes/testindex/query  {"q": "amazon"}
    Integer     response status     200

Load test data
    Clear Index
    @{json_data}  Set Variable   
       ...  [
       ...   {"title": "an anaemic person lol"},
       ...   {"title": "in the end amazon"},
       ...   {"title": "have a bet"},
       ...   {"title": "havelock a bet"}
       ...  ]
    ${json_str}     Catenate    @{json_data}

    Set Headers  ${appheader}
    POST         /1/indexes/testindex   ${json_str}
    Integer     response status     200
    Wait Until Keyword Succeeds	100x	10ms   No Jobs

Test an
    POST         /1/indexes/testindex/query  {"q": "an"}
    Integer     response status     200
    POST         /1/indexes/testindex/query  {"q": "an "}
    Integer     response status     200

Test have
    POST         /1/indexes/testindex/query  {"q": "have"}
    Integer     response status     200
    POST         /1/indexes/testindex/query  {"q": "have "}
    Integer     response status     200

Test havel
    POST         /1/indexes/testindex/query  {"q": "havel"}
    Integer     response status     200
    POST         /1/indexes/testindex/query  {"q": "havel "}
    Integer     response status     200

Test hav
    POST         /1/indexes/testindex/query  {"q": "hav"}
    Integer     response status     200
    POST         /1/indexes/testindex/query  {"q": "hav "}
    Integer     response status     200

Test havelo
    POST         /1/indexes/testindex/query  {"q": "havelo"}
    Integer     response status     200
    POST         /1/indexes/testindex/query  {"q": "havelo "}
    Integer     response status     200



Delete the index
    DELETE      /1/indexes/testindex
    Integer     response status     200

Delete the application
    Set Headers  ${header}
    DELETE      /1/applications/appfortests
    Integer     response status     200

