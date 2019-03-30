*** Settings ***
Resource  common.robot

*** Variables ***
${settings}     {"indexedFields": ["str", "str2", "str3"] }
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
       ...   {"str": "aa bb cc", "str2": "dd ee ff", "str3": "gg hh ii"}
       ...  ]
    ${json_str}     Catenate    @{json_data}

    Set Headers  ${appheader}
    POST         /1/indexes/testindex   ${json_str}
    Integer     response status     200
    Wait Until Keyword Succeeds	100x	10ms   No Jobs

Test query empty
    POST         /1/indexes/testindex/query  {"explain": true}
    Integer     response status     200
    Integer     $.totalHits         1

Test query aa
    POST         /1/indexes/testindex/query  {"q": "aa", "explain": true}
    Integer     response status     200
    Integer     $.totalHits         1
    Integer     $.hits[0]._explain.field     0

Test query aa bb
    POST         /1/indexes/testindex/query  {"q": "aa bb", "explain": true}
    Integer     response status     200
    Integer     $.totalHits         1
    Integer     $.hits[0]._explain.field     0

Test query aa dd
    POST         /1/indexes/testindex/query  {"q": "aa dd", "explain": true}
    Integer     response status     200
    Integer     $.totalHits         1
    Integer     $.hits[0]._explain.field     0

Test query bb hh
    POST         /1/indexes/testindex/query  {"q": "bb hh", "explain": true}
    Integer     response status     200
    Integer     $.totalHits         1
    Integer     $.hits[0]._explain.field     0

Test query ee hh
    POST         /1/indexes/testindex/query  {"q": "ee hh", "explain": true}
    Integer     response status     200
    Integer     $.totalHits         1
    Integer     $.hits[0]._explain.field     1

Delete the index
    DELETE      /1/indexes/testindex
    Integer     response status     200

Delete the application
    Set Headers  ${header}
    DELETE      /1/applications/appfortests
    Integer     response status     200

