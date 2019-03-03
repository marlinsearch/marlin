*** Settings ***
Resource  common.robot

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
    POST         /1/indexes/testindex  ${testjson}[data][0:100]
    Integer     response status     200
    Wait Until Keyword Succeeds	100x	10ms   No Jobs
    #GET     /1/indexes/testindex/mapping
    #Output

Test an empty query
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"q": ""}
    Integer     response status     200
    Integer     $.totalHits         100

Test an empty filter
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"filter": "{}"}
    Integer     response status     200
    POST         /1/indexes/testindex/query  {"filter": null}
    Integer     response status     200

Test single valid filter
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"filter": {"facet": "testme"}}
    Integer     response status     200

Test single invalid filter
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"filter": {"acet": "testme"}}
    Integer     response status     400

Test single string eq filter
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"filter": {"str": "testme"}}
    Integer     response status     400

Test single fail num eq filter
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"filter": {"id": "testme"}}
    Integer     response status     400

Test single num eq filter
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"filter": {"num": 5}}
    Integer     response status     200

Test single fail bool eq filter
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"filter": {"bool": 5}}
    Integer     response status     400

Test single bool eq filter
    POST         /1/indexes/testindex/query  {"filter": {"bool": true}}
    Integer     response status     200

Test valid and filter
    POST         /1/indexes/testindex/query  {"filter": {"bool": true, "facet": "testme", "facetonly": ["test", "two", "three"]}}

Test valid or filter
    POST         /1/indexes/testindex/query  {"filter": {"bool": true, "facet": "testme", "facetonly": {"$or": ["test", "two", "three"]}}}

Test valid string eq filter
    POST         /1/indexes/testindex/query  {"filter": {"facet": {"$eq": "testme"}}}
    Integer     response status     200

Test invalid string eq filter
    POST         /1/indexes/testindex/query  {"filter": {"facet": {"$eq": ["testme"]}}}
    Integer     response status     400

Test invalid string eq filter null
    POST         /1/indexes/testindex/query  {"filter": {"facet": {"$eq": null}}}
    Integer     response status     400

Test invalid string eq filter num
    POST         /1/indexes/testindex/query  {"filter": {"facet": {"$gt": "test"}}}
    Integer     response status     400


Delete the index
    Set Headers  ${appheader}
    DELETE      /1/indexes/testindex
    Integer     response status     200

Delete the application
    Set Headers  ${header}
    DELETE      /1/applications/appfortests
    Integer     response status     200
