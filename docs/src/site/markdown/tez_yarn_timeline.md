<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->
<head><title>Using YARN Timeline with Tez for History</title></head>

## YARN Timeline Background

Initial support for [YARN Timeline](https://hadoop.apache.org/docs/r2.4.0/hadoop-yarn/hadoop-yarn-site/TimelineServer.html) was introduced in Apache Hadoop 2.4.0. Support for ACLs in Timeline was introduced in Apache Hadoop 2.6.0. Support for Timeline was introduced in Tez in 0.5.x ( with some experimental support in 0.4.x ). However, Tez ACLs integration with Timeline is only available from Tez 0.6.0 onwards.

## How Tez Uses YARN Timeline

Tez uses YARN Timeline as its application history store. Tez stores most of its lifecycle information into this history store such as:
  - DAG information such as:
    - DAG Plan
    - DAG Submission, Start and End times
    - DAG Counters
    - Final status of the DAG and additional diagnostics
  - Vertex, Task and Task Attempt Information
    - Start and End times
    - Counters
    - Diagnostics

Using the above information, a user can analyze a Tez DAG while it is running and after it has completed.

## YARN Timeline and Hadoop Versions

Given that the support for YARN Timeline with full security was only realized in Apache Hadoop 2.6.0, some features may or may not be supported depending on which version of Apache Hadoop is used.


|  | Hadoop 2.2.x, 2.3.x | Hadoop 2.4.x, 2.5.x | Hadoop 2.6.x and higher |
| ------- | ----- | ----- | ----- |
| Timeline Support | No | Yes | Yes |
| Timeline v2 Support | No | No | Hadoop 3.x only |
| Timeline with ACLs Support | No | No | Yes |

## Configuring Tez to use YARN Timeline

By default, Tez writes its history data into a file on HDFS. To use Timeline, add the following property into your tez-site.xml:

> &lt;property&gt;<br/>
> &nbsp;&nbsp;&nbsp;&lt;name&gt;tez.history.logging.service.class&lt;/name&gt;<br/>
> &nbsp;&nbsp;&nbsp;&lt;value&gt;org.apache.tez.dag.history.logging.ats.ATSHistoryLoggingService&lt;/value&gt;<br/>
> &lt;/property&gt;<br/>

For Tez 0.4.x, the above property is not respected. For 0.4.x, please set the following property:

> &lt;property&gt;<br/>
> &nbsp;&nbsp;&nbsp;&lt;name&gt;tez.yarn.ats.enabled&lt;/name&gt;<br/>
> &nbsp;&nbsp;&nbsp;&lt;value&gt;true&lt;/value&gt;<br/>
> &lt;/property&gt;<br/>

When using Tez with Apache Hadoop 2.4.x or 2.5.x, given that these versions are not fully secure, the following property also needs to be enabled:

> &lt;property&gt;<br/>
> &nbsp;&nbsp;&nbsp;&lt;name&gt;tez.allow.disabled.timeline-domains&lt;/name&gt;<br/>
> &nbsp;&nbsp;&nbsp;&lt;value&gt;true&lt;/value&gt;<br/>
> &lt;/property&gt;<br/>

## Timeline Service v2

YARN Timeline Service v2 (ATSv2) replaces the single timeline server with a per-application
collector writing to HBase. It requires Apache Hadoop 3.x and is served by a separate history
logging service:

> &lt;property&gt;<br/>
> &nbsp;&nbsp;&nbsp;&lt;name&gt;tez.history.logging.service.class&lt;/name&gt;<br/>
> &nbsp;&nbsp;&nbsp;&lt;value&gt;org.apache.tez.dag.history.logging.ats.v2.ATSV2HistoryLoggingService&lt;/value&gt;<br/>
> &lt;/property&gt;<br/>

The cluster has to be configured for v2 as well: `yarn.timeline-service.enabled` set to true,
`yarn.timeline-service.version` set to `2.0`, the `timeline_collector` NodeManager auxiliary
service running, a storage backend configured through `yarn.timeline-service.writer.class`, and a
timeline reader for queries. The ATSv1 and ATSv1.5 logging services above stay inert when the
cluster runs v2; they log a warning instead of failing the application master.

Because the AM flushes its remaining history synchronously when it shuts down, and a publish to an
unreachable collector blocks for the full timeline client retry window, prefer a positive
`tez.yarn.ats.event.flush.timeout.millis` over the default of `-1` (wait forever).

### How the v2 entity model differs

|  | Timeline v1 / v1.5 | Timeline v2 |
| ------- | ----- | ----- |
| Indexed lookup fields | primary filters | none; every field is in `info`, queryable through `infofilters` |
| Configuration | a nested map under `otherinfo` | the `configs` field, queryable through `conffilters`, split across entities when large |
| Counters | a nested map under `otherinfo` | timeline metrics, plus the nested map on `TEZ_DAG_EXTRA_INFO` |
| Parent links | `relatedentities`, set once | `isRelatedTo`, repeated on every event, with the full ancestor chain |
| ACLs | timeline domains created by Tez | enforced by the timeline reader; Tez creates nothing |

Entity types are unchanged: `TEZ_APPLICATION`, `TEZ_APPLICATION_ATTEMPT`, `TEZ_CONTAINER_ID`,
`TEZ_DAG_ID`, `TEZ_DAG_EXTRA_INFO`, `TEZ_VERTEX_ID`, `TEZ_TASK_ID`, `TEZ_TASK_ATTEMPT_ID`.

Timeline v2 scopes entity queries by application, so DAG entities are additionally written to the
sub-application table, which is what makes a listing of DAGs across applications possible:
`/ws/v2/timeline/users/{user}/entities/TEZ_DAG_ID`. Set `tez.yarn.ats.v2.subapp.write` to false to
stop writing them.

**The Tez UI cannot read Timeline v2.** It issues ATSv1 requests, so enabling this service leaves
the UI empty. `tez.history.logging.service.class` names a single class and only one history
logging service runs, so the ATSv1 service cannot be kept alongside it: a cluster that needs the
UI has to stay on ATSv1 or ATSv1.5. History written to Timeline v2 is read through the timeline
reader's REST API instead.
