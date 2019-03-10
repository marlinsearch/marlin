*** Settings ***
Resource  common.robot

*** Variables ***
${settings}     {"indexedFields": ["str", "strlist"] }
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
       ...   {"str": "one two three four five six seven eight nine ten", "strlist": ["aaaaa", "bbbbb", "cccccc"]}
       ...  ]
    ${json_str}     Catenate    @{json_data}

    Set Headers  ${appheader}
    POST         /1/indexes/testindex   ${json_str}
    Integer     response status     200
    Wait Until Keyword Succeeds	100x	10ms   No Jobs

Test query one aaaa
    POST         /1/indexes/testindex/query  {"q": "one aaaa", "explain": true}
    Integer     response status     200
    Integer     $.hits[0]._explain.typos        0
    Integer     $.hits[0]._explain.position     1
    Integer     $.hits[0]._explain.proximity    8
    
Test query one aaaa bbbb
    POST         /1/indexes/testindex/query  {"q": "aaaa bbbb", "explain": true}
    Integer     response status     200
    Integer     $.hits[0]._explain.typos        1
    Integer     $.hits[0]._explain.position     1
    Integer     $.hits[0]._explain.field        1
    Integer     $.hits[0]._explain.proximity    8

Test query one aaaa ten
    POST         /1/indexes/testindex/query  {"q": "aaaa ten", "explain": true}
    Integer     response status     200
    Integer     $.hits[0]._explain.typos        1
    Integer     $.hits[0]._explain.position     10
    Integer     $.hits[0]._explain.field        0
    Integer     $.hits[0]._explain.proximity    8

Delete the index
    DELETE      /1/indexes/testindex
    Integer     response status     200

Delete the application
    Set Headers  ${header}
    DELETE      /1/applications/appfortests
    Integer     response status     200

