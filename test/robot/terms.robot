*** Settings ***
Library         REST    http://localhost:9002


*** Variables ***
${header}   {"x-marlin-application-id": "abcdefgh", "x-marlin-rest-api-key": "12345678901234567890123456789012"}
${appheader}  {"x-marlin-application-id": "aaaaaaaa", "x-marlin-rest-api-key": "12345678901234567890123456789012"}
${app}  {"name" : "appfortests", "appId": "aaaaaaaa", "apiKey": "12345678901234567890123456789012"}
${index}  {"name" : "testindex", "numShards": 5}
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
    Sleep       2s

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

