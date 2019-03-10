*** Settings ***
Resource  common.robot

*** Variables ***
${settings}     {"indexedFields": ["str"] }
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
       ...   {"str": "a new hope"},
       ...   {"str": "anew hope"},
       ...   {"str": "a newhope"},
       ...   {"str": "anewhope"},
       ...   {"str": "a blah new woah hope"},
       ...   {"str": "test"}
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
    Integer     $.totalHits         6

Test query test
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"q": "a new hope", "explain": true}
    Output
    Integer     response status     200

Delete the index
    Set Headers  ${appheader}
    DELETE      /1/indexes/testindex
    Integer     response status     200

Delete the application
    Set Headers  ${header}
    DELETE      /1/applications/appfortests
    Integer     response status     200

