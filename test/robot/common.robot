*** Settings ***
Library         REST    http://localhost:9002


*** Variables ***
${header}   {"x-marlin-application-id": "abcdefgh", "x-marlin-rest-api-key": "12345678901234567890123456789012"}
${appheader}  {"x-marlin-application-id": "aaaaaaaa", "x-marlin-rest-api-key": "12345678901234567890123456789012"}
${app}  {"name" : "appfortests", "appId": "aaaaaaaa", "apiKey": "12345678901234567890123456789012"}
${index}  {"name" : "testindex", "numShards": 5}
${settings}     {"indexedFields": ["str", "strlist", "facet", "facetlist", "spell", "id", "num", "numlist", "bool"], "facetFields": ["facet", "facetlist", "facetonly"] }

*** Keywords ***
No Jobs
    Set Headers  ${appheader}
    GET        /1/indexes/testindex/info
    Integer     $.numJobs           0

Clear Index
    Set Headers  ${appheader}
    POST        /1/indexes/testindex/clear
