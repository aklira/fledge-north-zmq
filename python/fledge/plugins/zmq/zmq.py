# -*- coding: utf-8 -*-

# FLEDGE_BEGIN
# See: http://fledge-iot.readthedocs.io/
# FLEDGE_END

"""
"""

import zmq
import logging
import asyncio
import numpy as np

from fledge.common import logger
from fledge.plugins.north.common.common import *

__author__ = "Akli Rahmoun"
__copyright__ = "Copyright (c) 2020 RTE (https://www.rte-france.com)"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_LOGGER = logger.setup(__name__, level=logging.INFO)

_DEFAULT_CONFIG = {
    'plugin': {
        'description': 'ZMQ Subscriber North Plugin',
        'type': 'string',
        'default': 'zmq',
        'readonly': 'true'
    },
    'proxyHost': {
        'description': 'Hostname or IP address of the proxy to connect to',
        'type': 'string',
        'default': 'localhost',
        'order': '1',
        'displayName': 'ZMQ proxy host',
        'mandatory': 'true'
    },
    'proxyPort': {
        'description': 'The network port of the proxy to connect to',
        'type': 'integer',
        'default': '5559',
        'order': '2',
        'displayName': 'ZMQ proxy port',
        'mandatory': 'true'
    },
    'topic': {
        'description': 'The subscription topic to publish messages to',
        'type': 'string',
        'default': 'DEFAULT',
        'order': '3',
        'displayName': 'Topic To Publish',
        'mandatory': 'true'
    },
    'assetName': {
        'description': 'Name of Asset',
        'type': 'string',
        'default': 'zmq-',
        'order': '4',
        'displayName': 'Asset Name',
        'mandatory': 'true'
    }
}

def plugin_info():
    return {
        'name': 'ZMQ Publisher',
        'version': '0.0.1',
        'mode': 'async',
        'type': 'north',
        'interface': '1.0',
        'config': _DEFAULT_CONFIG
    }

def plugin_init(data):
    global zmq_north, config
    zmq_north = ZmqNorthPlugin()
    config = data
    return config


async def plugin_send(data, payload, stream_id):
    try:
        is_data_sent, new_last_object_id, num_sent = await zmq_north.send_payloads(payload)
    except asyncio.CancelledError:
        pass
    else:
        return is_data_sent, new_last_object_id, num_sent


def plugin_shutdown(data):
    _LOGGER.info('zmq north plugin shut down.')


def plugin_reconfigure():
    pass


class ZmqNorthPlugin(object):
    """ ZMQ North Plugin """

    def __init__(self):
        self.event_loop = asyncio.get_event_loop()
        self.proxy_host = config['proxyHost']['value']
        self.proxy_port = int(config['proxyPort']['value'])
        self.topic = config['topic']['value']
        self.asset = config['assetName']['value']

    async def send_payloads(self, payloads):
        is_data_sent = False
        last_object_id = 0
        num_sent = 0

        try:
            payload_block = list()
            for p in payloads:
                read = dict()
                read["asset"] = p['asset_code']
                read["timestamp"]=p['user_ts']
                read["content"]=p['reading']
                last_object_id = p["id"]
                for k, v in p['reading'].items():
                    if not isinstance(v, np.ndarray):
                        read["readings"] = p['reading']
                payload_block.append(read)
            num_sent = await self._send_payloads(payload_block)
            is_data_sent = True
        except Exception as ex:
            _LOGGER.exception(ex, "Data could not be sent!")
        return is_data_sent, last_object_id, num_sent

    async def _send_payloads(self, payload_block):
        """ send a list of block payloads"""
        num_count = 0
        try:
            context = zmq.Context()
            publisher = context.socket(zmq.PUB)
            publisher.connect('tcp://{}:{}'.format(self.proxy_host, self.proxy_port))
            await self._send(publisher, payload_block)
        except Exception as ex:
            _LOGGER.exception(f'Exception sending payloads: {ex}')
        else:
            num_count += len(payload_block)
        return num_count

    async def _send(self, publisher, payload):
        """ Send the payload, using provided publisher """
        publisher.send_string(payload)