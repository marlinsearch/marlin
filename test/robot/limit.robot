*** Settings ***
Resource  common.robot

*** Setting ***
Library    String

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
    ${setjson}  Input  ${CURDIR}/docset.json
    POST        /1/indexes/testindex/settings         ${setjson}
    Integer     response status     200

Load some data
    ${testjson}  Input  ${CURDIR}/doc.json
    Set Headers  ${appheader}
    POST         /1/indexes/testindex  ${testjson}
    Integer     response status     200
    Wait Until Keyword Succeeds	100x	10ms   No Jobs

Test an empty query
    Set Headers  ${appheader}
    POST         /1/indexes/testindex/query  {"q": ""}
    Integer     response status     200
    Integer     $.totalHits         1

Test this
    Set Headers  ${appheader}
    &{res}=     POST         /1/indexes/testindex/query  {"q": "this"}
    Integer     response status     200
    Integer     $.totalHits         1
    ${c}=       Evaluate    str(${res}[body][hits][0][_highlight]).count("<mark>") == 10
    Should be True      ${c}

Test this only word
    Set Headers  ${appheader}
    &{res}=     POST         /1/indexes/testindex/query  {"q": "this", "highlightFields": ["word"]}
    Integer     response status     200
    Integer     $.totalHits         1
    ${c}=       Evaluate    str(${res}[body][hits][0][_highlight]).count("<mark>") == 1
    Should be True      ${c}

Test this two words
    &{res}=     POST         /1/indexes/testindex/query  {"q": "this", "highlightFields": ["word", "object.word"]}
    Integer     response status     200
    ${c}=       Evaluate    str(${res}[body][hits][0][_highlight]).count("<mark>") == 2
    Should be True      ${c}
    &{res}=     POST         /1/indexes/testindex/query  {"q": "this", "highlightFields": ["objects.word"]}
    ${c}=       Evaluate    str(${res}[body][hits][0][_highlight]).count("<mark>") == 2
    Should be True      ${c}

Test this inner inner word
    &{res}=     POST         /1/indexes/testindex/query  {"q": "this", "highlightFields": ["object.iobject.iword"]}
    ${c}=       Evaluate    str(${res}[body][hits][0][_highlight]).count("<mark>") == 1
    Should be True      ${c}

Test highlight source
    &{res}=     POST         /1/indexes/testindex/query  {"q": "this", "highlightSource": true}
    Integer     response status     200
    Integer     $.totalHits         1
    ${c}=       Evaluate    str(${res}[body][hits][0]).count("<mark>") == 10
    Should be True      ${c}
    ${c}=       Evaluate    '_highlight' in ${res}[body][hits][0]
    Should be Equal     ${c}        ${FALSE}
    &{res}=     POST         /1/indexes/testindex/query  {"q": "this", "highlightSource": false}
    Integer     response status     200
    Integer     $.totalHits         1
    ${c}=       Evaluate    str(${res}[body][hits][0][_highlight]).count("<mark>") == 10
    Should be True      ${c}
    ${c}=       Evaluate    '_highlight' in ${res}[body][hits][0]
    Should be Equal     ${c}        ${TRUE}

Test highlight settings
    POST        /1/indexes/testindex/settings   {"highlightFields": ["word"]}
    Integer     response status     200
    &{res}=     POST         /1/indexes/testindex/query  {"q": "this"}
    ${c}=       Evaluate    str(${res}[body][hits][0][_highlight]).count("<mark>") == 1
    Should be True      ${c}
    POST        /1/indexes/testindex/settings   {"highlightFields": ["*"]}
    Integer     response status     200
    &{res}=     POST         /1/indexes/testindex/query  {"q": "this"}
    ${c}=       Evaluate    str(${res}[body][hits][0][_highlight]).count("<mark>") == 10
    Should be True      ${c}
    POST        /1/indexes/testindex/settings   {"highlightFields": null}
    Integer     response status     200
    &{res}=     POST         /1/indexes/testindex/query  {"q": "this"}
    ${c}=       Evaluate    str(${res}[body][hits][0]).count("<mark>") == 0
    Should be True      ${c}
    POST        /1/indexes/testindex/settings   {"highlightFields": ["*"]}
    Integer     response status     200

Test get fields
    Set Headers  ${appheader}
    &{res}=     POST         /1/indexes/testindex/query  {"q": "this", "getFields": ["word"]}
    Integer     response status     200
    Integer     $.totalHits         1
    ${c}=       Evaluate    str(${res}[body][hits][0][_highlight]).count("<mark>") == 1
    Should be True      ${c}
    &{res}=     POST         /1/indexes/testindex/query  {"q": "this", "getFields": ["word", "words", "object.word"]}
    ${c}=       Evaluate    str(${res}[body][hits][0][_highlight]).count("<mark>") == 3
    Should be True      ${c}

Test get inner fields
    &{res}=     POST         /1/indexes/testindex/query  {"q": "this", "getFields": ["word", "object.iobject.iword"]}
    ${c}=       Evaluate    str(${res}[body][hits][0][_highlight]).count("<mark>") == 2
    Should be True      ${c}
    &{res}=     POST         /1/indexes/testindex/query  {"q": "this", "getFields": ["word", "objects"]}
    ${c}=       Evaluate    str(${res}[body][hits][0][_highlight]).count("<mark>") == 5
    Should be True      ${c}
    &{res}=     POST         /1/indexes/testindex/query  {"q": "this", "getFields": ["objects.word"]}
    ${c}=       Evaluate    str(${res}[body][hits][0][_highlight]).count("<mark>") == 2
    Should be True      ${c}
    &{res}=     POST         /1/indexes/testindex/query  {"q": "this", "getFields": ["objects.object.iword", "objects.word"]}
    ${c}=       Evaluate    str(${res}[body][hits][0][_highlight]).count("<mark>") == 4
    Should be True      ${c}
    &{res}=     POST         /1/indexes/testindex/query  {"q": "this", "getFields": ["objects.object.inum", "objects.word", "objects.object.iword", "word", "object.word", "object.number", "object.iobject.iword", "bigobject.levelone.leveltwo.levelthree.number"]}
    #Output


Delete the index
    Set Headers  ${appheader}
    DELETE      /1/indexes/testindex
    Integer     response status     200

Delete the application
    Set Headers  ${header}
    DELETE      /1/applications/appfortests
    Integer     response status     200

