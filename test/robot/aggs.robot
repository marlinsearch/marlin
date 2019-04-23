*** Settings ***
Resource  common.robot

*** Variables ***
${settings}     {"indexedFields": ["str", "n", "nl", "b"], "facetFields": ["f", "fl", "n", "nl"]}
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
       ...   {"str": "a", "n": 1, "nl": [8,2,3], "f": "aaa", "fl": ["zzz", "yyy"], "b": true},
       ...   {"str": "a", "n": 2, "nl": [2,3,4], "f": "bbb", "fl": ["yyy", "xxx"], "b": false},
       ...   {"str": "a", "n": 11, "nl": [3,4,5], "f": "ccc", "fl": ["xxx", "www"], "b": true},
       ...   {"str": "a", "n": 4, "nl": [4,5,6], "f": "ccc", "fl": ["www", "vvv", "xxx"], "b": false},
       ...   {"str": "a", "n": 5, "nl": [5,6,7], "f": "eee", "fl": ["vvv", "uuu"], "b": true}
       ...  ]
    ${json_str}     Catenate    @{json_data}

    Set Headers  ${appheader}
    POST         /1/indexes/testindex   ${json_str}
    Integer     response status     200
    Wait Until Keyword Succeeds	100x	10ms   No Jobs

Test a empty query
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"q": ""}
    Integer     response status     200
    Integer     $.totalHits         5

Test max aggr
    POST         /1/indexes/testindex/query  {"aggs": {"max_n": {"max": {"field": "n"}}, "max_nl": {"max": {"field": "nl"}}}}
    Integer     $.aggs.max_n.value  11

Test min aggr
    POST         /1/indexes/testindex/query  {"aggs": {"min_n": {"min": {"field": "n"}}, "min_nl": {"min": {"field": "nl"}}}}
    Integer     $.aggs.min_n.value  1

Delete the index
    Set Headers  ${appheader}
    DELETE      /1/indexes/testindex
    Integer     response status     200

Delete the application
    Set Headers  ${header}
    DELETE      /1/applications/appfortests
    Integer     response status     200

