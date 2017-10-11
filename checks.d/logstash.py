# stdlib
from urlparse import urljoin

# 3rd Party
import requests

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
        else:
            self.service_check(SERVICE_CHECK_NAME, AgentCheck.OK,
                               message='Connection to %s was successful' % url)

    def _collect_pipeline_metrics(self, logstash_address):
        '''
        Send pipeline metrics
        :param logstash_address: Logstash address
        :return:
        '''
        url = urljoin(logstash_address, NODE_STATS_ENDPOINT + "/pipeline")
        response_json = self._get_json_from_api(url)

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

        self._send_metrics("logstash", metric_names, response_json)

        self._send_pipeline_plugin_metrics(response_json)

    def _send_pipeline_plugin_metrics(self, payload_dict):
        '''
        Send metrics of each plugin (input, output, filter)
        :param payload_dict: response to pipeline api in json
        :return:
        '''
        input_metrics = ["events.out", "events.queue_push_duration_in_millis"]
        output_metrics = ["events.out", "events.in", "events.duration_in_millis"]
        filter_metrics = ["events.out", "events.in", "events.duration_in_millis", "matches"]

        inputs, found = self._get_from_dict("pipeline.plugins.inputs", payload_dict)
        for input in inputs:
            self._send_metrics("logstash.pipeline.plugins.inputs.%s" % input['name'], input_metrics, input)

        outputs, found = self._get_from_dict("pipeline.plugins.outputs", payload_dict)
        for output in outputs:
            self._send_metrics("logstash.pipeline.plugins.outputs.%s" % output['name'], output_metrics, output)

        filters, found = self._get_from_dict("pipeline.plugins.filters", payload_dict)
        for filter in filters:
            self._send_metrics("logstash.pipeline.plugins.filters.%s" % filter['name'], filter_metrics, filter)

    def _collect_jvm_metrics(self, logstash_address):
        '''
        :param logstash_address: Logstash address
        :return:
        '''
        url = urljoin(logstash_address, NODE_STATS_ENDPOINT + "/jvm")
        response_json = self._get_json_from_api(url)

        metric_names = ["jvm.gc.collectors.old.collection_time_in_millis",
                        "jvm.gc.collectors.old.collection_count",
                        "jvm.gc.collectors.young.collection_time_in_millis",
                        "jvm.gc.collectors.young.collection_count"]

        self._send_metrics(metric_names, response_json)

    def _send_metrics(self, namespace, metric_names, payload_dict):
        '''
        :param namespace/prefix of the metric
        :param list of metric_names: list of metric names with dot notation.
               eg. ['pipeline.queue.data.free_space_in_bytes', ...]
        :param payload_dict: payload dict containing above metrics.(response of rest api)
        :return: None. Sends metric to datadog.
        '''
        for metric_name in metric_names:
            value, found = self._get_from_dict(metric_name, payload_dict)
            if found:
                self.gauge("%s.%s" % (namespace, metric_name), value)
            else:
                self.log.error("Cannot find metric %s " % metric_name)
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

    def _get_json_from_api(self, endpoint):
        try:
            response = requests.get(endpoint, timeout=CHECK_TIMEOUT)
            response.raise_for_status()
        except Exception as e:
            self.log.error("Failed to fetch %s.Skipped sending metrics. %s" % (endpoint, str(e)))
            return # _check_logstash would have already failed.So just return instead of immediately sending a fail..
        else:
            return response.json()