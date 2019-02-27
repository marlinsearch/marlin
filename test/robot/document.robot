*** Settings ***
Resource  common.robot

*** Variables ***
${settings}     {"indexedFields": ["str", "bool", "num"], "facetFields": ["facet"]}
${index}        {"name" : "testindex", "numShards": 1}

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
   ...   {"_id": "id1", "str": "aaaa", "facet": "aaaa", "bool": true, "num": 1},
   ...   {"_id": "id2", "str": "aaaa bbbb", "facet": "bbbb", "bool": false, "num": 2},
   ...   {"_id": "id3", "str": "aaaa bbbb cccc", "facet": "cccc", "bool": true, "num": 3}
   ...  ]

    ${json_str}     Catenate    @{json_data}
    Set Headers  ${appheader}
    POST         /1/indexes/testindex   ${json_str}
    Integer     response status     200
    Wait Until Keyword Succeeds	100x	10ms   No Jobs

Get document id4
    Set Headers  ${appheader}
    GET         /1/indexes/testindex/id4
    Integer     response status     404

Get document id1
    Set Headers  ${appheader}
    GET         /1/indexes/testindex/id1
    Integer     response status     200
    String      $._id               id1
    String      $.str               aaaa

Get document id3
    Set Headers  ${appheader}
    GET         /1/indexes/testindex/id3
    Integer     response status     200
    String      $._id               id3
    String      $.str               aaaa bbbb cccc

Test an empty query before delete
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"q": ""}
    Integer     response status     200
    Integer     $.totalHits         3

Delete document id1
    Set Headers  ${appheader}
    DELETE      /1/indexes/testindex/id1
    Integer     response status     200
    Wait Until Keyword Succeeds	100x	10ms   No Jobs
    POST         /1/indexes/testindex/query  {"q": ""}
    Integer     response status     200
    Integer     $.totalHits         2

Delete document id3
    Set Headers  ${appheader}
    DELETE      /1/indexes/testindex/id3
    Integer     response status     200
    Wait Until Keyword Succeeds	100x	10ms   No Jobs
    POST         /1/indexes/testindex/query  {"q": ""}
    Integer     response status     200
    Integer     $.totalHits         1

Add document id3
    Set Headers  ${appheader}
    POST         /1/indexes/testindex   {"_id": "id3", "str": "aaaa bbbb cccc"}
    Integer     response status     200
    Wait Until Keyword Succeeds	100x	10ms   No Jobs
    POST         /1/indexes/testindex/query  {"q": ""}
    Integer     response status     200
    Integer     $.totalHits         2
    GET         /1/indexes/testindex/id3
    Integer     response status     200
    String      $._id               id3
    String      $.str               aaaa bbbb cccc
    Integer     $._docid            1

Add document id1
    Set Headers  ${appheader}
    POST         /1/indexes/testindex   {"_id": "id1", "str": "aaaa bbbb cccc"}
    Integer     response status     200
    Wait Until Keyword Succeeds	100x	10ms   No Jobs
    POST         /1/indexes/testindex/query  {"q": ""}
    Integer     response status     200
    Integer     $.totalHits         3
    GET         /1/indexes/testindex/id1
    Integer     response status     200
    String      $._id               id1
    String      $.str               aaaa bbbb cccc
    Integer     $._docid            3

Delete All docs
    Set Headers  ${appheader}
    DELETE      /1/indexes/testindex/id1
    DELETE      /1/indexes/testindex/id2
    DELETE      /1/indexes/testindex/id3

Delete the index
    Set Headers  ${appheader}
    DELETE      /1/indexes/testindex
    Integer     response status     200

Delete the application
    Set Headers  ${header}
    DELETE      /1/applications/appfortests
    Integer     response status     200

