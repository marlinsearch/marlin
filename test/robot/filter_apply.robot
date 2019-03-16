*** Settings ***
Resource  common.robot

*** Variables ***
${settings}     {"indexedFields": ["str", "n", "nl", "b"], "facetFields": ["f", "fl"]}
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
       ...   {"str": "a", "n": 1, "nl": [1,2,3], "f": "aaa", "fl": ["zzz", "yyy"], "b": true},
       ...   {"str": "a", "n": 2, "nl": [2,3,4], "f": "bbb", "fl": ["yyy", "xxx"], "b": false},
       ...   {"str": "a", "n": 3, "nl": [3,4,5], "f": "ccc", "fl": ["xxx", "www"], "b": true},
       ...   {"str": "a", "n": 4, "nl": [4,5,6], "f": "ddd", "fl": ["www", "vvv"], "b": false},
       ...   {"str": "a", "n": 5, "nl": [5,6,7], "f": "eee", "fl": ["vvv", "uuu"], "b": true}
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

Test query num eq filter
    POST         /1/indexes/testindex/query  {"q": "", "filter": {"n": 5}}
    Integer     response status     200
    Integer     $.totalHits         1
    POST         /1/indexes/testindex/query  {"q": "", "filter": {"nl": 5}}
    Integer     response status     200
    Integer     $.totalHits         3
    POST         /1/indexes/testindex/query  {"q": "", "filter": {"nl": {"$eq": 5}}}
    Integer     response status     200
    Integer     $.totalHits         3

Test query num eq non matching filter
    POST         /1/indexes/testindex/query  {"q": "", "filter": {"n": 7}}
    Integer     response status     200
    Integer     $.totalHits         0

Test query str eq filter
    POST         /1/indexes/testindex/query  {"q": "", "filter": {"f": "aaa"}}
    Integer     response status     200
    Integer     $.totalHits         1
    POST         /1/indexes/testindex/query  {"q": "", "filter": {"fl": {"$eq": "uuu"}}}
    Integer     response status     200
    Integer     $.totalHits         1
    POST         /1/indexes/testindex/query  {"q": "", "filter": {"fl": "xxx"}}
    Integer     response status     200
    Integer     $.totalHits         2
 
Test query str eq non matching filter
    POST         /1/indexes/testindex/query  {"q": "", "filter": {"f": "fff"}}
    Integer     response status     200
    Integer     $.totalHits         0

Test query bool eq filter
    POST         /1/indexes/testindex/query  {"q": "", "filter": {"b": true}}
    Integer     response status     200
    Integer     $.totalHits         3
    POST         /1/indexes/testindex/query  {"q": "", "filter": {"b": false}}
    Integer     response status     200
    Integer     $.totalHits         2
 
Test query num ne filter
    POST         /1/indexes/testindex/query  {"q": "", "filter": {"nl": {"$ne": 5}}}
    Integer     response status     200
    Integer     $.totalHits         2
    POST         /1/indexes/testindex/query  {"q": "", "filter": {"n": {"$ne": 1}}}
    Integer     response status     200
    Integer     $.totalHits         4
    POST         /1/indexes/testindex/query  {"q": "", "filter": {"n": {"$ne": 7}}}
    Integer     response status     200
    Integer     $.totalHits         5

Test query str ne filter
    POST         /1/indexes/testindex/query  {"q": "", "filter": {"fl": {"$ne": "uuu"}}}
    Integer     response status     200
    Integer     $.totalHits         4
    POST         /1/indexes/testindex/query  {"q": "", "filter": {"f": {"$ne": "aaa"}}}
    Integer     response status     200
    Integer     $.totalHits         4
    POST         /1/indexes/testindex/query  {"q": "", "filter": {"f": {"$ne": "blah"}}}
    Integer     response status     200
    Integer     $.totalHits         5
 
Test query bool ne filter
    POST         /1/indexes/testindex/query  {"q": "", "filter": {"b": {"$ne": true}}}
    Integer     response status     200
    Integer     $.totalHits         2
    POST         /1/indexes/testindex/query  {"q": "", "filter": {"b": {"$ne": false}}}
    Integer     response status     200
    Integer     $.totalHits         3
 
Test query or filter
    POST         /1/indexes/testindex/query  {"q": "", "filter": {"n": {"$or": [1,5]}}}
    Integer     response status     200
    Integer     $.totalHits         2
    POST         /1/indexes/testindex/query  {"q": "", "filter": {"$or": [{"n": 2}, {"n": 4}]}}
    Integer     response status     200
    Integer     $.totalHits         2
    POST         /1/indexes/testindex/query  {"q": "", "filter": {"$or": [{"n": 100}, {"n": 200}]}}
    Integer     response status     200
    Integer     $.totalHits         0
    POST         /1/indexes/testindex/query  {"q": "", "filter": {"$or": [{"n": 3}, {"n": 200}]}}
    Integer     response status     200
    Integer     $.totalHits         1
    POST         /1/indexes/testindex/query  {"q": "", "filter": {"$or": [{"n": 3}, {"n": 200}, {"b": false}]}}
    Integer     response status     200
    Integer     $.totalHits         3
    POST         /1/indexes/testindex/query  {"q": "", "filter": {"f": {"$or": ["aaa", "bbb"]}}}
    Integer     response status     200
    Integer     $.totalHits         2
 
Test query and filter
    POST         /1/indexes/testindex/query  {"q": "", "filter": {"n": {"$and": [1,5]}}}
    Integer     $.totalHits         0
    POST         /1/indexes/testindex/query  {"filter": {"$and": [{"n": 3}, {"nl": 3}, {"b": false}]}}
    Integer     $.totalHits         0
    POST         /1/indexes/testindex/query  {"filter": {"$and": [{"n": 3}, {"nl": 3}, {"b": true}]}}
    Integer     $.totalHits         1
    POST         /1/indexes/testindex/query  {"filter": {"nl": [3,4]}}
    Integer     $.totalHits         2

Test query gt filter
    POST         /1/indexes/testindex/query  {"filter": {"n": {"$gt": 100}}}
    Integer     $.totalHits         0
    POST         /1/indexes/testindex/query  {"filter": {"n": {"$gt": 0}}}
    Integer     $.totalHits         5
    POST         /1/indexes/testindex/query  {"filter": {"nl": {"$gt": 3}}}
    Integer     $.totalHits         4
    POST         /1/indexes/testindex/query  {"filter": {"nl": {"$gt": 4}}}
    Integer     $.totalHits         3

 Test query gte filter
    POST         /1/indexes/testindex/query  {"filter": {"n": {"$gte": 100}}}
    Integer     $.totalHits         0
    POST         /1/indexes/testindex/query  {"filter": {"n": {"$gte": 0}}}
    Integer     $.totalHits         5
    POST         /1/indexes/testindex/query  {"filter": {"nl": {"$gte": 3}}}
    Integer     $.totalHits         5
    POST         /1/indexes/testindex/query  {"filter": {"nl": {"$gte": 4}}}
    Integer     $.totalHits         4

 Test query lt filter
    POST         /1/indexes/testindex/query  {"filter": {"n": {"$lt": 100}}}
    Integer     $.totalHits         5
    POST         /1/indexes/testindex/query  {"filter": {"n": {"$lt": 0}}}
    Integer     $.totalHits         0
    POST         /1/indexes/testindex/query  {"filter": {"nl": {"$lt": 3}}}
    Integer     $.totalHits         2
    POST         /1/indexes/testindex/query  {"filter": {"nl": {"$lt": 4}}}
    Integer     $.totalHits         3

 Test query lte filter
    POST         /1/indexes/testindex/query  {"filter": {"n": {"$lte": 100}}}
    Integer     $.totalHits         5
    POST         /1/indexes/testindex/query  {"filter": {"n": {"$lte": 1}}}
    Integer     $.totalHits         1
    POST         /1/indexes/testindex/query  {"filter": {"nl": {"$lte": 3}}}
    Integer     $.totalHits         3
    POST         /1/indexes/testindex/query  {"filter": {"nl": {"$lte": 4}}}
    Integer     $.totalHits         4

 Test query range filter
    POST         /1/indexes/testindex/query  {"filter": {"n": {"$gt": 1, "$lt": 3}}}
    Integer     $.totalHits         1
    POST         /1/indexes/testindex/query  {"filter": {"n": {"$gte": 1, "$lte": 3}}}
    Integer     $.totalHits         3
 

Delete the index
    Set Headers  ${appheader}
    DELETE      /1/indexes/testindex
    Integer     response status     200

Delete the application
    Set Headers  ${header}
    DELETE      /1/applications/appfortests
    Integer     response status     200

