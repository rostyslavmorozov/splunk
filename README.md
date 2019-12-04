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
**splunk.test.token.hec:** HEC Token for writing.

**splunk.test.token.api:** API Token for reading.

**splunk.test.url.write:** URL to point to the Splunk write endpoint. The format for URL: \<protocol>://\<host>:\<port> (ex: http://localhost:8088).

**splunk.test.url.read:** URL to point to the Splunk read endpoint. The format for URL: \<protocol>://\<host>:\<port> (ex: https://localhost:8089).

**splunk.test.index:** Splunk index with test data.
