
Simple test suite to help create integration tests for a TAP service. This code now searches for
TAP endpoints using the optional interfaceType args supported by the cadc-registry library. This 
requires the servcie to implement the new TAP-1.1 interface types (uws:Async and uws:Sync) in the
VOSI capabilities resource.

