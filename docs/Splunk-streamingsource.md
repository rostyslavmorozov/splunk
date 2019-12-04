# Splunk Batch Source


Description
-----------
This source reads data source from Splunk Enterprise.

The data which should be read is specified using data source and filters for that data source.

Configuration
-------------

### Basic

**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Data Source URL:** URL to point to the Splunk server. The format for URL: \<protocol>://\<host>:\<port> (ex: https://localhost:8089).

**Authentication Type:** Authentication method to access Splunk API. Defaults to Basic Authentication.

**Token:** The value of token created for authentication to the Splunk API.

**Username:** Login name for authentication to the Splunk API.

**Password:** Password for authentication to the Splunk API.

**Execution Mode:** Defines the behaviour for the Splunk Search.
Valid values: (Blocking | Normal). Default is Normal.

If set to Normal, runs an asynchronous search.

If set to Blocking, returns the sid when the job is complete.

If set to Oneshot, returns results in the same call.

**Output Format:** Specifies the format for the returned output.
Valid values: (csv | json | xml). Default is xml.

**Search String:** Splunk Search String for retrieving results.
 
**Search Id:** Search Id for retrieving job results.

Search String or Search Id must be specified.

**Auto Cancel:** The job automatically cancels after this many seconds of inactivity.
0 means never auto-cancel. Default is 0.

**Earliest Time:** A time string. Sets the earliest (inclusive), respectively, time bounds for the search.
The time string can be either a UTC time (with fractional seconds), a relative time specifier (to now) or a formatted time string.
Refer to [Time modifiers for search](https://docs.splunk.com/Documentation/Splunk/7.3.1/SearchReference/SearchTimeModifiers)
for information and examples of specifying a time string.

**Latest Time:** A time string. Sets the latest (exclusive), respectively, time bounds for the search.
The time string can be either a UTC time (with fractional seconds), a relative time specifier (to now) or a formatted time string.
Refer to [Time modifiers for search](https://docs.splunk.com/Documentation/Splunk/7.3.1/SearchReference/SearchTimeModifiers)
for information and examples of specifying a time string. 

**Indexed Earliest Time:** A time string. Sets the earliest (inclusive), respectively, time bounds for the search, based on the index time bounds.
The time string can be either a UTC time (with fractional seconds), a relative time specifier (to now) or a formatted time string.
Refer to [Time modifiers for search](https://docs.splunk.com/Documentation/Splunk/7.3.1/SearchReference/SearchTimeModifiers)
for information and examples of specifying a time string. 

**Indexed Latest Time:** A time string. Sets the latest (exclusive), respectively, time bounds for the search, based on the index time bounds.
The time string can be either a UTC time (with fractional seconds), a relative time specifier (to now) or a formatted time string.
Refer to [Time modifiers for search](https://docs.splunk.com/Documentation/Splunk/7.3.1/SearchReference/SearchTimeModifiers)
for information and examples of specifying a time string. 

**Search Results Count:** The maximum number of results to return. If value is set to 0, then all available results are returned. Default is 0.

### Advanced

**Connect Timeout:** The time in milliseconds to wait for a connection. Set to 0 for infinite. Defaults to 60000 (1 minute).

**Read Timeout:** The time in milliseconds to wait for a read. Set to 0 for infinite. Defaults to 60000 (1 minute).

**Number of Retries:** The number of times the request should be retried if the request fails. Defaults to 3.

**Max Retry Wait:** Maximum time in milliseconds retries can take. Set to 0 for infinite. Defaults to 60000 (1 minute).

**Max Retry Jitter Wait:** Maximum time in milliseconds added to retries. Defaults to 100.

**Poll Interval:** The amount of time to wait between each poll in milliseconds. Defaults to 60000 (1 minute).
