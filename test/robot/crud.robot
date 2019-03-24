*** Settings ***
Resource  common.robot

*** Variables ***
${settings}     {"indexedFields": ["str", "n", "nl", "b"], "facetFields": ["f", "fl", "n", "nl"]}
${index}  {"name" : "testindex", "nShards": 5}


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
       ...   {"_id": "1", "str": "ffff", "n": 1, "nl": [1,2,3], "f": "aaa", "fl": ["zzz", "yyy"], "b": true},
       ...   {"_id": "2", "str": "gggg", "n": 2, "nl": [2,3,4], "f": "bbb", "fl": ["yyy", "xxx"], "b": false},
       ...   {"_id": "3", "str": "hhhh", "n": 3, "nl": [3,4,5], "f": "ccc", "fl": ["xxx", "www"], "b": true},
       ...   {"_id": "4", "str": "iiii", "n": 4, "nl": [4,5,6], "f": "ddd", "fl": ["www", "vvv"], "b": false},
       ...   {"_id": "5", "str": "jjjj", "n": 5, "nl": [5,6,7], "f": "eee", "fl": ["vvv", "uuu"], "b": true}
       ...  ]
    ${json_str}     Catenate    @{json_data}

    Set Headers  ${appheader}
    POST         /1/indexes/testindex   ${json_str}
    Integer     response status     200
    Wait Until Keyword Succeeds	100x	10ms   No Jobs

Test basic queries
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {}
    Integer     response status     200
    Integer     $.totalHits         5
    POST         /1/indexes/testindex/query  {"q":"hhhh"}
    Integer     $.totalHits         1
    POST         /1/indexes/testindex/query  {"filter": {"n": 3}}
    Integer     $.totalHits         1
    POST         /1/indexes/testindex/query  {"filter": {"f": "ccc"}}
    Integer     $.totalHits         1
    POST         /1/indexes/testindex/query  {"filter": {"b": true}}
    Integer     $.totalHits         3

Test delete document
    DELETE      /1/indexes/testindex/3
    Wait Until Keyword Succeeds	100x	10ms   No Jobs
    POST         /1/indexes/testindex/query  {}
    Integer     response status     200
    Integer     $.totalHits         4
    POST         /1/indexes/testindex/query  {"q":"hhhh"}
    Integer     $.totalHits         0
    POST         /1/indexes/testindex/query  {"q":"hh"}
    Integer     $.totalHits         0
    POST         /1/indexes/testindex/query  {"filter": {"n": 3}}
    Integer     $.totalHits         0
    POST         /1/indexes/testindex/query  {"filter": {"f": "ccc"}}
    Integer     $.totalHits         0
    POST         /1/indexes/testindex/query  {"filter": {"b": true}}
    Integer     $.totalHits         2

Test add document
    PUT         /1/indexes/testindex/3    {"_id": "3", "str": "hhhh", "n": 3, "nl": [3,4,5], "f": "ccc", "fl": ["xxx", "www"], "b": true}
    Integer     response status     200
    Wait Until Keyword Succeeds	100x	10ms   No Jobs
    POST         /1/indexes/testindex/query  {"q":"hhhh"}
    Integer     $.totalHits         1
    POST         /1/indexes/testindex/query  {"q":"hh"}
    Integer     $.totalHits         1
    POST         /1/indexes/testindex/query  {"filter": {"n": 3}}
    Integer     $.totalHits         1
    POST         /1/indexes/testindex/query  {"filter": {"f": "ccc"}}
    Integer     $.totalHits         1
    POST         /1/indexes/testindex/query  {"filter": {"b": true}}
    Integer     $.totalHits         3

Test replace document
    PUT         /1/indexes/testindex/3    {"str": "kkkk", "n": 6, "nl": [8,9,10], "f": "fff", "fl": ["ttt", "sss"], "b": false}
    Integer     response status     200
    Wait Until Keyword Succeeds	100x	10ms   No Jobs
    POST         /1/indexes/testindex/query  {"q":"kkkk"}
    Integer     $.totalHits         1
    POST         /1/indexes/testindex/query  {"filter": {"n": 6}}
    Integer     $.totalHits         1
    POST         /1/indexes/testindex/query  {"filter": {"f": "fff"}}
    Integer     $.totalHits         1
    POST         /1/indexes/testindex/query  {"filter": {"fl": "ttt"}}
    Integer     $.totalHits         1
    POST         /1/indexes/testindex/query  {"filter": {"b": false}}
    Integer     $.totalHits         3
    POST         /1/indexes/testindex/query  {"filter": {"nl": 9}}
    Integer     $.totalHits         1
    POST         /1/indexes/testindex/query  {"filter": {"nl": 8}}
    Integer     $.totalHits         1
    POST         /1/indexes/testindex/query  {"filter": {"n": 3}}
    Integer     $.totalHits         0
    POST         /1/indexes/testindex/query  {"q":"hhhh"}
    Integer     $.totalHits         0
    POST         /1/indexes/testindex/query  {"filter": {"f": "ccc"}}
    Integer     $.totalHits         0
    POST         /1/indexes/testindex/query  {"filter": {"b": true}}
    Integer     $.totalHits         2
 
Test update document
    PATCH       /1/indexes/testindex/3    {"str": "hhhh", "n": 3, "nl": [8,10,11]}
    Integer     response status     200
    Wait Until Keyword Succeeds	100x	10ms   No Jobs
    POST         /1/indexes/testindex/query  {"q":"hhhh"}
    Integer     $.totalHits         1
    POST         /1/indexes/testindex/query  {"filter": {"n": 3}}
    Integer     $.totalHits         1
    POST         /1/indexes/testindex/query  {"filter": {"nl": 8}}
    Integer     $.totalHits         1
    POST         /1/indexes/testindex/query  {"filter": {"nl": 11}}
    Integer     $.totalHits         1
    POST         /1/indexes/testindex/query  {"filter": {"nl": 9}}
    Integer     $.totalHits         0
    POST         /1/indexes/testindex/query  {"filter": {"n": 6}}
    Integer     $.totalHits         0
    POST         /1/indexes/testindex/query  {"q":"kkkk"}
    Integer     $.totalHits         0
    POST         /1/indexes/testindex/query  {"q":"kk"}
    Integer     $.totalHits         0

*** Test Case ***
Delete the index
    Set Headers  ${appheader}
    DELETE      /1/indexes/testindex
    Integer     response status     200

Delete the application
    Set Headers  ${header}
    DELETE      /1/applications/appfortests
    Integer     response status     200

