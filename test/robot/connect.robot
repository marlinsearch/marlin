*** Settings ***
Library         REST    http://localhost:9002

*** Variables ***
${invalid_header}   {"x-marlin-application-id": "abcdefgh", "x-marlin-rest-api-key": "blahlkjasdflkjasdflkjasdf"}
${header}   {"x-marlin-application-id": "abcdefgh", "x-marlin-rest-api-key": "12345678901234567890123456789012"}

*** Test Cases ***
Connect and execute a ping
    GET         /ping
    Integer     response status           200

Get marlin version without auth and invalid auth
    GET         /1/marlin
    Integer     response status           400

    GET         /1/marlin                 headers=${invalid_header}
    Integer     response status           400

Get marlin version with proper auth
    GET         /1/marlin                 headers=${header}
    Integer     response status           200
    String      $.version
