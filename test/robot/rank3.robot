*** Settings ***
Resource  common.robot

*** Variables ***
${settings}     {"indexedFields": ["str"] }
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
       ...   {"str": "one two three four five six seven eight nine ten"}
       ...  ]
    ${json_str}     Catenate    @{json_data}

    Set Headers  ${appheader}
    POST         /1/indexes/testindex   ${json_str}
    Integer     response status     200
    Wait Until Keyword Succeeds	100x	10ms   No Jobs

Test query one
    POST         /1/indexes/testindex/query  {"q": "one", "explain": true}
    Integer     response status     200
    Integer     $.hits[0]._explain.typos        0
    Integer     $.hits[0]._explain.position     1

Test query fivs
    POST         /1/indexes/testindex/query  {"q": "fivs", "explain": true}
    Integer     response status     200
    Integer     $.hits[0]._explain.typos        1
    Integer     $.hits[0]._explain.position     5

Test query eight
    POST         /1/indexes/testindex/query  {"q": "eight", "explain": true}
    Integer     response status     200
    Integer     $.hits[0]._explain.typos        0
    Integer     $.hits[0]._explain.position     8

Test query one two
    POST         /1/indexes/testindex/query  {"q": "one two", "explain": true}
    Integer     response status     200
    Integer     $.hits[0]._explain.typos        0
    Integer     $.hits[0]._explain.exact        2
    Integer     $.hits[0]._explain.position     1
    Integer     $.hits[0]._explain.proximity    1

Test query one foor
    POST         /1/indexes/testindex/query  {"q": "one foor", "explain": true}
    Integer     response status     200
    Integer     $.hits[0]._explain.typos        1
    Integer     $.hits[0]._explain.exact        1
    Integer     $.hits[0]._explain.position     1
    Integer     $.hits[0]._explain.proximity    3

Test query one six 
    POST         /1/indexes/testindex/query  {"q": "one six", "explain": true}
    Integer     response status     200
    Integer     $.hits[0]._explain.typos        0
    Integer     $.hits[0]._explain.exact        2
    Integer     $.hits[0]._explain.position     1
    Integer     $.hits[0]._explain.proximity    5

Test query three six 
    POST         /1/indexes/testindex/query  {"q": "thre six", "explain": true}
    Integer     response status     200
    Integer     $.hits[0]._explain.typos        1
    Integer     $.hits[0]._explain.exact        1
    Integer     $.hits[0]._explain.position     3
    Integer     $.hits[0]._explain.proximity    3

Test query one nin
    POST         /1/indexes/testindex/query  {"q": "one nin", "explain": true}
    Integer     response status     200
    Integer     $.hits[0]._explain.typos        0
    Integer     $.hits[0]._explain.exact        1
    Integer     $.hits[0]._explain.position     1
    Integer     $.hits[0]._explain.proximity    8

Test query one ten
    POST         /1/indexes/testindex/query  {"q": "one ten", "explain": true}
    Integer     response status     200
    Integer     $.hits[0]._explain.typos        0
    Integer     $.hits[0]._explain.exact        2
    Integer     $.hits[0]._explain.position     1
    Integer     $.hits[0]._explain.proximity    8



Delete the index
    DELETE      /1/indexes/testindex
    Integer     response status     200

Delete the application
    Set Headers  ${header}
    DELETE      /1/applications/appfortests
    Integer     response status     200

