Purpose
=======

This datadog agent plugin will fetch metrics from Logstash usnig Monitoring API (https://www.elastic.co/guide/en/logstash/current/monitoring-logstash.html#monitoring) and push them into Datadog.

Metrics
-------

*Node Stats*

* `logstash.pipeline.events.in`
* `logstash.pipeline.events.out`
* `logstash.pipeline.events.filtered`
* `logstash.pipeline.events.queue_push_duration_in_millis`
* `logstash.pipeline.events.duration_in_millis`
* `logstash.pipeline.queue.events`
* `logstash.pipeline.queue.data.free_space_in_bytes`
* `logstash.pipeline.queue.capacity.page_capacity_in_bytes`
* `logstash.pipeline.queue.capacity.max_queue_size_in_bytes`
* `logstash.pipeline.queue.capacity.max_unread_events`
* `logstash.pipeline.reloads.successes`
* `logstash.pipeline.reloads.failures`

TODO document pipeline plugin metrics.
TODO document jvm metrics

Checks
------

It will also push a check `logstash.can_connect` into Datadog to monitor the health of Logstash.