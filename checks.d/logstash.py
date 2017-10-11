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