# splunk
A collection of Splunk connectors and plugins.

Following plugins are available in this repository.

  * Splunk Batch Source
  * Splunk Streaming Source
  * Splunk Batch Sink

# Integration tests

By default all tests will be skipped, since Splunk credentials are needed.

Instructions to enable the tests:
 1. Create/use existing Splunk account.
 2. Create HEC Token for writing with separate index for testing.
 3. Create API Token for reading.
 4. Run the tests using the command below:

```
mvn clean test -Dsplunk.test.token.hec= -Dsplunk.test.token.api= -Dsplunk.test.url.write= -Dsplunk.test.url.read= -Dsplunk.test.index=
```
