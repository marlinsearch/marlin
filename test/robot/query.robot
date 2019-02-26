*** Settings ***
Library         REST    http://localhost:9002


*** Variables ***
${header}   {"x-marlin-application-id": "abcdefgh", "x-marlin-rest-api-key": "12345678901234567890123456789012"}
${appheader}  {"x-marlin-application-id": "aaaaaaaa", "x-marlin-rest-api-key": "12345678901234567890123456789012"}
${app}  {"name" : "appfortests", "appId": "aaaaaaaa", "apiKey": "12345678901234567890123456789012"}
${index}  {"name" : "testindex", "numShards": 5}
${settings}     {"indexedFields": ["str", "strlist", "facet", "facetlist", "spell", "id", "num", "numlist", "bool"], "facetFields": ["facet", "facetlist", "facetonly"] }

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
    POST         /1/indexes/testindex  ${testjson}[data]
    Integer     response status     200
    Sleep       2s

Test an empty query
    ${testjson}  Input  ${CURDIR}/../test.json
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"q": ""}
    Integer     response status     200
    Integer     $.totalHits         ${testjson}[count]

Delete the index
    Set Headers  ${appheader}
    DELETE      /1/indexes/testindex
    Integer     response status     200

Delete the application
    Set Headers  ${header}
    DELETE      /1/applications/appfortests
    Integer     response status     200
