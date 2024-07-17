# youcat local integration tests

The integration tests lookup and test `ivo://opencadc.org/youcat` which should only
ever be found in a local DEVELOPMENT registry. It is not safe to run the tests against
a database where you care about the content!

The tests use a schema named `int_test_schema`.

The following x509 client certificates are found in `$A/test-certificates` and used to 
execute the tests:
- youcat-admin.pem  : used to create the test schema, same ident must be configured as the admin in the deployed service
- youcat-owner.pem  : the owner of the test schema
- youcat-member.pem : member of a group that you-cat-owner grants read-write access to

The tests are curently only runnable by CADC staff because of ard coded setup and assumptions about external
content (group in GMS). The test group is currently hard coded to one in the `ivo://cadc.nrc.ca/gms` GMS service. 

Tested: youcat-admin == personal account, youcat-owner == cadcauthtest1, youcat-member == cadcauthtest2

