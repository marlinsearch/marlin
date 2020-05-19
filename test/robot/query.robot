*** Settings ***
Resource  common.robot


*** Variables ***
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
    Set Headers  ${appheader}
    POST        /1/indexes/testindex/settings         ${settings}
    Integer     response status     200

Load some data
    ${testjson}  Input  ${CURDIR}/../test.json
    Set Headers  ${appheader}
    POST         /1/indexes/testindex  ${testjson}[data][0:6000]
    Integer     response status     200
    Wait Until Keyword Succeeds	100x	10ms   No Jobs

Test an empty query
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"q": ""}
    Integer     response status     200
    Integer     $.totalHits         6000

Configure full scan threshold
    Set Headers  ${appheader}
    POST        /1/indexes/testindex/settings         {"fullScanThreshold": 5000, "rankBy": {"num": "asc"}}
    Integer     response status     200

Get settings the index
    Set Headers  ${appheader}
    GET         /1/indexes/testindex/settings
    Integer     response status     200

Test another empty query
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"q": ""}
    Integer     response status     200
    Integer     $.totalHits         6000
    String      $.indexName         testindex

Test multiple queries
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"requests" : [{"q": ""}, {"q": "blahblahblahblahblahblahblahblah"}]}
    Integer     response status     200
    Integer     $.results[0].totalHits         6000
    Integer     $.results[1].totalHits         0
    String      $.results[1].indexName         testindex
    String      $.results[0].indexName         testindex

Test app query
    Set Headers  ${appheader}
    POST         /1/query  {"q": "", "indexName": "testindex"}
    Integer     response status     200
    Integer     $.totalHits         6000
    String      $.indexName         testindex

Test app multiple queries
    Set Headers  ${appheader}
    POST         /1/query  {"requests" : [{"q": "", "indexName": "testindex"}, {"q": "blahblahblahblahblahblahblahblah", "indexName": "testindex"}]}
    Integer     response status     200
    Integer     $.results[0].totalHits         6000
    Integer     $.results[1].totalHits         0
    String      $.results[1].indexName         testindex
    String      $.results[0].indexName         testindex


Test app query failures
    Set Headers  ${appheader}
    POST         /1/query  {"q": "", "indexName": "testindexblah"}
    Integer     response status     400
    POST         /1/query  {"q": "", "iexName": "testindexblah"}
    Integer     response status     400
 
Delete the index
    Set Headers  ${appheader}
    DELETE      /1/indexes/testindex
    Integer     response status     200

Delete the application
    Set Headers  ${header}
    DELETE      /1/applications/appfortests
    Integer     response status     200
