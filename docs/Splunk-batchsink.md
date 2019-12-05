# Splunk Batch Source


Description
-----------
This source reads data source from Splunk Enterprise.

The data which should be read is specified using data source and filters for that data source.

Configuration
-------------

### Basic

**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**URL:** URL to point to the Splunk server. The format for URL: \<protocol>://\<host>:\<port> (ex: http://localhost:8088).

**Authentication Type:** Authentication method to access Splunk API. Defaults to Basic Authentication.
Query String Authentication can be used in Splunk Cloud only.

**HEC Token:** The value of token created for authentication to the Splunk API.

**Username:** Login name for authentication to the Splunk API.

**Endpoint:** Splunk endpoint to send data to.

**Batch Size:** The number of messages to batch before sending. Default is 1.

**Event Metadata:** Optional event metadata string in the JSON export for destination.

**Channel Identifier Header:** GUID for Splunk Channel.

### Advanced

**Connect Timeout:** The time in milliseconds to wait for a connection. Set to 0 for infinite. Defaults to 60000 (1 minute).

**Read Timeout:** The time in milliseconds to wait for a read. Set to 0 for infinite. Defaults to 60000 (1 minute).

**Number of Retries:** The number of times the request should be retried if the request fails. Defaults to 3.

**Max Retry Wait:** Maximum time in milliseconds retries can take. Set to 0 for infinite. Defaults to 60000 (1 minute).

**Max Retry Jitter Wait:** Maximum time in milliseconds added to retries. Defaults to 100.
