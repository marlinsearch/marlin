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
       ...   {"str": "why is this my best day"},
       ...   {"str": "this is why then my"},
       ...   {"str": "why my test me"},
       ...   {"str": "this is a new a new whymy"},
       ...   {"str": "mya what is then blah whys this"}
       ...  ]
    ${json_str}     Catenate    @{json_data}

    Set Headers  ${appheader}
    POST         /1/indexes/testindex   ${json_str}
    Integer     response status     200
    Wait Until Keyword Succeeds	100x	10ms   No Jobs

Test query whys my
    POST         /1/indexes/testindex/query  {"q": "whys my", "explain": true}
    Integer     response status     200
    Integer     $.totalHits         4
    Integer     $.hits[0]._explain.typos        0
    Integer     $.hits[0]._explain.position     1
    Integer     $.hits[0]._explain.proximity    5
    Integer     $.hits[0]._explain.exact        1
    Integer     $.hits[1]._explain.exact        1
    Integer     $.hits[1]._explain.typos        1

Test query why my
    POST         /1/indexes/testindex/query  {"q": "why my", "explain": true}
    Integer     response status     200
    Integer     response status     200
    Integer     $.totalHits         4

Test query a
    POST         /1/indexes/testindex/query  {"q": "a new", "explain": true}
    Integer     response status     200
    Integer     $.totalHits         1


Delete the index
    DELETE      /1/indexes/testindex
    Integer     response status     200

Delete the application
    Set Headers  ${header}
    DELETE      /1/applications/appfortests
    Integer     response status     200

