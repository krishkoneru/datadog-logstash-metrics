Purpose
=======

This datadog agent plugin will fetch metrics from Logstash usnig Monitoring API (https://www.elastic.co/guide/en/logstash/current/monitoring-logstash.html#monitoring) and push them into Datadog.

Metrics
-------

*Node Stats*
* `logstash.node_stats.jvm`
* `logstash.node_stats`

Checks
======

It will also push a check `logstash.can_connect` into Datadog to monitor the health of Burrow