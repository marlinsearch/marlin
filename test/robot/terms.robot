*** Settings ***
Resource  common.robot

*** Variables ***
${settings}     {"indexedFields": ["str"] }
${index}  {"name" : "testindex", "nShards": 1}

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
   ...   {"str": "aaaa"},
   ...   {"str": "aaaa bbbb"},
   ...   {"str": "aaaa bbbb cccc"},
   ...   {"str": "aaaa bbbb cccc dddd"},
   ...   {"str": "aaaa bbbb cccc dddd eeee"},
   ...   {"str": "aaaabbbb cccc dddd eeee"},
   ...   {"str": "aaaa bbbbcccc ddddeeee"},
   ...   {"str": "aaaabbbbccccddddeeee"}
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
    Integer     $.totalHits         8

Test query aaaa
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"q": "aaaa"}
    Integer     response status     200
    Integer     $.totalHits         8

Test query dddd
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"q": "dddd"}
    Integer     response status     200
    Integer     $.totalHits         4

Test query baaa
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"q": "baaa"}
    Integer     response status     200
    Integer     $.totalHits         8

Test query baaa bbbb
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"q": "baaa bbbb"}
    Integer     response status     200
    Integer     $.totalHits         5

Test query aaaa bbbb
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"q": "aaaa bbbb"}
    Integer     response status     200
    Integer     $.totalHits         7

Test query aaaa bbbb cccc
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"q": "aaaa bbbb cccc"}
    Integer     response status     200
    Integer     $.totalHits         6

Test query aaaa bbbb cccc dddd
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"q": "aaaa bbbb cccc dddd"}
    Integer     response status     200
    Integer     $.totalHits         5

Test query aaaa bbbb cccc dddd e
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"q": "aaaa bbbb cccc dddd e"}
    Integer     response status     200
    Integer     $.totalHits         4

Delete the index
    Set Headers  ${appheader}
    DELETE      /1/indexes/testindex
    Integer     response status     200

Delete the application
    Set Headers  ${header}
    DELETE      /1/applications/appfortests
    Integer     response status     200

