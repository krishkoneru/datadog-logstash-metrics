# stdlib
from urlparse import urljoin

# 3rd Party
import requests
import json

# project
from checks import AgentCheck

SERVICE_CHECK_NAME = 'logstash.can_connect'

DEFAULT_LOGSTASH_URI = 'http://localhost:9600'

NODE_STATS_ENDPOINT = '/_node/stats'

CHECK_TIMEOUT = 10

class LogstashCheck(AgentCheck):
    '''
    Get metrics from Logstash Monitoring API 
    '''
    def check(self, instance):
        logstash_address = instance.get("logstash_uri", DEFAULT_LOGSTASH_URI)

        self._check_logstash(logstash_address)

        self.log.debug("Collecting pipline metrics")
        self._collect_pipeline_metrics(logstash_address)

        self.log.debug("Collecting jvm metrics")
        self._collect_jvm_metrics(logstash_address)

    def _check_logstash(self, logstash_address):
        """
        Check a Logstash endpoint
        """
        url = urljoin(logstash_address, "/_node/pipeline")
        try:
            response = requests.get(url, timeout=CHECK_TIMEOUT)
            response.raise_for_status()
        except Exception as e:
            self.service_check(SERVICE_CHECK_NAME,
                               AgentCheck.CRITICAL, message=str(e))
            raise
        else:
            self.service_check(SERVICE_CHECK_NAME, AgentCheck.OK,
                               message='Connection to %s was successful' % url)

    def _collect_pipeline_metrics(self, logstash_address):
        '''
        :param logstash_address:
        :return:
        '''
        #TODO doco
        url = urljoin(logstash_address, NODE_STATS_ENDPOINT + "/pipeline")
        metric_names = ["pipeline.events.in",
                        "pipeline.events.out",
                        "pipeline.events.filtered",
                        "pipeline.events.queue_push_duration_in_millis",
                        "pipeline.events.duration_in_millis",
                        "pipeline.queue.events",
                        "pipeline.queue.data.free_space_in_bytes",
                        "pipeline.queue.capacity.page_capacity_in_bytes",
                        "pipeline.queue.capacity.max_queue_size_in_bytes",
                        "pipeline.queue.capacity.max_unread_events",
                        "pipeline.reloads.successes",
                        "pipeline.reloads.failures"]

        #TODO. add conf options for pipeline.plugin.[input,output,filters] metrics.

        self._send_metrics(metric_names, url)

    def _collect_jvm_metrics(self, logstash_address):
        '''
        :param logstash_address:
        :return:
        '''
        #TODO doco
        url = urljoin(logstash_address, NODE_STATS_ENDPOINT + "/jvm")
        metric_names = ["jvm.gc.collectors.old.collection_time_in_millis",
                        "jvm.gc.collectors.old.collection_count",
                        "jvm.gc.collectors.young.collection_time_in_millis",
                        "jvm.gc.collectors.young.collection_count"]

        self._send_metrics(metric_names, url)

    def _send_metrics(self, metric_names, endpoint):
        '''
        :param list of metric_names: takes metric name with dot notation. eg. ['pipeline.queue.data.free_space_in_bytes', ...]
        :param endpoint: URI which returns the above metrics.
        :return: sends metric to datadog.
        '''
        try:
            response = requests.get(endpoint, timeout=CHECK_TIMEOUT)
            response.raise_for_status()
        except Exception as e:
            self.log.error("Error failed to fetch ") #TODO
            return # _check_logstash would have already failed.So just return instead of immediately sending a fail..
        else:
            for metric_name in metric_names:
                value, found = self._get_from_dict(metric_name, response.json())
                if found:
                    self.gauge("logstash.%s" % metric_name, value)
                else:
                    self.log.error("Cannot find metric %s in %s response" % (metric_name, endpoint))
                    raise

    def _get_from_dict(self, key, dict):
        if "." in key:
            first, rest = key.split(".", 1)
            if first not in dict:
                return None, False
            else:
                return self._get_from_dict(rest, dict[first])
        elif key in dict:
            return dict[key], True
        else:
            return None, False
