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
       ...   {"str": "test"},
       ...   {"str": "best"},
       ...   {"str": "atest"},
       ...   {"str": "testa"},
       ...   {"str": "tset"},
       ...   {"str": "etst"},
       ...   {"str": "tets"},
       ...   {"str": "tesg"},
       ...   {"str": "tset"},
       ...   {"str": "tast"},
       ...   {"str": "tegt"}
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
    Integer     $.totalHits         11

Test query test
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"q": "test"}
    Integer     response status     200
    Integer     $.totalHits         11

Test query btest
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"q": "btest"}
    Integer     response status     200
    Integer     $.totalHits         4

Test query atest
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"q": "atest"}
    Integer     response status     200
    Integer     $.totalHits         3

Test query ctest
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"q": "ctest"}
    Integer     response status     200
    Integer     $.totalHits         3

Test query tes
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"q": "tes"}
    Integer     response status     200
    Integer     $.totalHits         3

Test query te
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"q": "te"}
    Integer     response status     200
    Integer     $.totalHits         5

Test query t
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"q": "t"}
    Integer     response status     200
    Integer     $.totalHits         8

Delete the index
    Set Headers  ${appheader}
    DELETE      /1/indexes/testindex
    Integer     response status     200

Delete the application
    Set Headers  ${header}
    DELETE      /1/applications/appfortests
    Integer     response status     200

