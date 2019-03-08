*** Settings ***
Resource  common.robot

*** Variables ***
${settings}     {"indexedFields": ["str"] }


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
       ...   {"str": "this is worst"},
       ...   {"str": "this is best"},
       ...   {"str": "this best worst"},
       ...   {"str": "testing this"},
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
    Integer     $.totalHits         5

Test query test
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"q": "test", "explain": true}
    Integer     response status     200
    Integer     $.totalHits         4
    Integer     $.hits[0]._explain.exact    1
    Integer     $.hits[0]._explain.field    0
    Integer     $.hits[0]._explain.position     1
    Integer     $.hits[1]._explain.exact    0
    Integer     $.hits[1]._explain.position     1
    Integer     $.hits[1]._explain.typos    0
    Integer     $.hits[2]._explain.typos    1
    Integer     $.hits[2]._explain.position     2
    Integer     $.hits[3]._explain.typos    1
    Integer     $.hits[3]._explain.position     3

Test query th
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"q": "th", "explain": true}
    Integer     response status     200
    Integer     $.totalHits         4
    Integer     $.hits[0]._explain.position     1
    Integer     $.hits[1]._explain.position     1
    Integer     $.hits[2]._explain.position     1
    Integer     $.hits[3]._explain.position     2
 
Test query worst
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"q": "worst", "explain": true}
    Integer     response status     200
    Integer     $.totalHits         2
    Integer     $.hits[0]._explain.position     3
    Integer     $.hits[1]._explain.position     3
    Integer     $.hits[1]._explain.exact        1
    Integer     $.hits[1]._explain.exact        1
 
Test query wors
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"q": "wors", "explain": true}
    Integer     response status     200
    Integer     $.totalHits         2
    Integer     $.hits[0]._explain.position     3
    Integer     $.hits[1]._explain.position     3
    Integer     $.hits[1]._explain.exact        0
    Integer     $.hits[1]._explain.exact        0

 Test query wurs
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"q": "wurs", "explain": true}
    Integer     response status     200
    Integer     $.totalHits         2
    Integer     $.hits[0]._explain.position     3
    Integer     $.hits[1]._explain.position     3
    Integer     $.hits[1]._explain.typos        1
    Integer     $.hits[1]._explain.typos        1
 

Delete the index
    Set Headers  ${appheader}
    DELETE      /1/indexes/testindex
    Integer     response status     200

Delete the application
    Set Headers  ${header}
    DELETE      /1/applications/appfortests
    Integer     response status     200

