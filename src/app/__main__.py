from email import message
import paho.mqtt.client as mqtt
from sparkplug_b import spb_dataclasses
from sparkplug_b import functions as spb
from sparkplug_b.enums import DataTypes, SpecialValues, MessageTypes
from random import randrange
import config as cfg

import json
import logging
import db_connect as db
import time
import datetime as dt
from sqlalchemy import desc





mqtt_client = mqtt.Client(client_id=cfg.MQTT_CLIENT_ID)
mqtt_client.username_pw_set(username=cfg.MQTT_USERNAME, password=cfg.MQTT_PASSWORD)
if cfg.MQTT_USE_TLS:
    mqtt_client.tls_set(cert_reqs=mqtt.ssl.CERT_REQUIRED)


def rebirth_message(topic):
    device_id = topic['device_id']

    topic_str = f"spBv1.0/{topic['group_id']}/{'DCMD' if device_id else 'NCMD'}/{topic['node_id']}"
    if device_id:
        topic_str += f"/{device_id}"

    return topic_str, spb_dataclasses.Payload(
        timestamp=spb.millis(),
        seq=0,
        metrics=[
            spb_dataclasses.Metric(
                timestamp=spb.millis(),
                name=f'{"Device" if device_id else "Node"} Control/Rebirth',
                datatype=DataTypes.Boolean,
                value=True
            )
        ]
    )


def decode_sparkplug_b_topic(topic: str):
    device_id = None
    topic = topic[topic.find('spBv1.0/')+8:].split('/')
    if len(topic) < 3:
        return {}
    
    group_id = topic[0]
    message_type = MessageTypes(topic[1])
    node_id = topic[2]
    if len(topic) > 3:
        device_id = topic[3]
    return dict(group_id=group_id, message_type=message_type, node_id=node_id, device_id=device_id)


class MetricNameCache:
    def __init__(self):
        self.__cache = {}

    @staticmethod
    def topic_id(group_id: str, node_id: str, device_id: str = None) -> str:
        if device_id:
            return f'{group_id}/{node_id}/{device_id}'
        return f'{group_id}/{node_id}'

    def set_metrics_names(self, group_id: str, node_id: str, metrics: list[dict], device_id: str = None):
        self.__cache[self.topic_id(group_id, node_id, device_id)] = {
            m['alias']: m['name'] for m in metrics if 'name' in m and 'alias' in m
        }

    @staticmethod
    def __get_metric_name(cache: dict, alias: int) -> str or None:
        return cache.get(alias)

    def topic_in_cache(self, group_id: str, node_id: str, device_id: str = None) -> bool:
        return self.__cache.get(self.topic_id(group_id, node_id, device_id)) is not None

    def add_names_to_metrics(self, group_id: str, node_id: str, metrics: list[dict], device_id: str = None):
        cache = self.__cache.get(self.topic_id(group_id, node_id, device_id))
        if cache is None:
            return None

        for m in metrics:
            if 'alias' not in m:
                continue
            name = self.__get_metric_name(cache, m['alias'])
            if name is None:
                logging.warning('name not found')
                continue
            # logging.warning(f'setting name for metric {name}')
            m['name'] = name


class DbIdCache:
    def __init__(self):
        self.__group_ids = {}
        self.__edge_nodes = {}
        self.__devices = {}

    def get_group_db_id(self, group_id: str, create: bool) -> int or None:
        group_db_id = self.__group_ids.get(group_id)
        if group_db_id is not None:
            return group_db_id
        group_db_id = db.session.query(db.SparkplugBGroup.id).filter(db.SparkplugBGroup.name == group_id).first()
        if group_db_id is not None:
            self.__group_ids[group_id] = group_db_id[0]
            return group_db_id[0]
        if not create:
            return None
        group = db.SparkplugBGroup(name=group_id)
        db.session.add(group)
        db.session.commit()
        self.__group_ids[group_id] = group.id
        return group.id

    def get_node_db_id(self, group_id: str, node_id: str, create: bool) -> int or None:
        cache_id = f'{group_id}/{node_id}'
        node_db_id = self.__edge_nodes.get(cache_id)
        if node_db_id is not None:
            return node_db_id
        group_db_id = self.get_group_db_id(group_id, create)
        node_db_id = db.session.query(db.SparkplugBEdgeNode.id).filter(
            db.SparkplugBEdgeNode.group_id == group_db_id,
            db.SparkplugBEdgeNode.name == node_id
        ).first()
        if node_db_id is not None:
            self.__edge_nodes[cache_id] = node_db_id[0]
            return node_db_id[0]
        if not create or group_db_id is None:
            return None
        node = db.SparkplugBEdgeNode(group_id=group_db_id, name=node_id)
        db.session.add(node)
        db.session.commit()
        self.__edge_nodes[cache_id] = node.id
        return node.id

    def get_device_db_id(self, group_id: str, node_id: str, device_id: str, create: bool) -> int or None:
        cache_id = f'{group_id}/{node_id}/{device_id}'
        device_db_id = self.__devices.get(cache_id)
        if device_db_id is not None:
            return device_db_id
        node_db_id = self.get_node_db_id(group_id, node_id, create)
        device_db_id = db.session.query(db.SparkplugBDevice.id).filter(
            db.SparkplugBDevice.edge_node_id == node_db_id,
            db.SparkplugBDevice.name == device_id
        ).first()
        if device_db_id is not None:
            self.__devices[cache_id] = device_db_id[0]
            return device_db_id[0]

        if not create or node_db_id is None:
            return None
        
        device = db.SparkplugBDevice(edge_node_id=node_db_id, name=device_id)
        db.session.add(device)
        db.session.commit()
        self.__devices[cache_id] = device.id
        return device.id

    def get_ids_from_topic(self, topic: dict, create: bool = True) -> tuple[int, int]:
        group_db_id = self.get_group_db_id(topic['group_id'], create)
        if group_db_id is None:
            return None, None

        edge_node_db_id = self.get_node_db_id(topic['group_id'], topic['node_id'], create)
        if edge_node_db_id is None:
            return None, None

        if topic['device_id'] is None:
            return edge_node_db_id, None

        device_db_id = self.get_device_db_id(topic['group_id'], topic['node_id'], topic['device_id'], create)
        if device_db_id is None:
            return None, None

        return edge_node_db_id, device_db_id


names_cache = MetricNameCache()
db_ids_cache = DbIdCache()


def save_payload(topic: dict, payload: dict, raw_payload: bytes, raw_topic: str):
    edge_node_db_id, device_db_id = db_ids_cache.get_ids_from_topic(topic)
    assert edge_node_db_id is not None
    if topic['device_id']:
        assert device_db_id is not None
    db_payload = db.SparkplugBPayload(
        edge_node_id=edge_node_db_id,
        edge_device_id=device_db_id,
        timestamp=payload['timestamp'],
        sequence=payload['seq'],
        message_type=topic['message_type'],
        raw_topic=raw_topic,
        raw_payload=raw_payload
    )
    db.session.add(db_payload)
    db.session.commit()
    
    return db_payload


def save_metrics(payload_db_id: int, metrics: list[dict]):
    def db_value_key(datatype: DataTypes):
        if 0 < datatype.value < 9:
            return 'integer_value'
        elif 8 < datatype.value < 11:
            return 'float_value'
        elif datatype.value == 11:
            return 'boolean_value'
        else:
            return 'string_value'

    def set_value_key(metric: dict):
        value = None
        for key in metric.keys():
            if '_value' in key:
                value = metric[key]
                metric.pop(key)
                break
        metric[db_value_key(metric['datatype'])] = value

    db_metrics = []
    for metric in metrics:
        if 'is_historical' in metric:
            if not metric['is_historical']:
                continue
            metric.pop('is_historical')
        metric["datatype"] = DataTypes[metric["datatype"]]
        set_value_key(metric)
        db_metrics.append(
            db.SparkplugBMetric(payload_id=payload_db_id, **metric)
        )
    db.session.add_all(db_metrics)
    db.session.commit()


def sparkplug_message(function):
    def load_message(client, userdata, message):
        try:
            topic = decode_sparkplug_b_topic(message.topic)
            payload = spb_dataclasses.Payload.from_mqtt_payload(message.payload).to_dict()

            db_payload = save_payload(topic, payload, message.payload, message.topic)

            function(client, topic, payload)

            if 'metrics' in payload:
                names_cache.add_names_to_metrics(
                    group_id=topic['group_id'],
                    node_id=topic['node_id'],
                    device_id=topic['device_id'],
                    metrics=payload['metrics']
                )

                save_metrics(db_payload.id, payload['metrics'])

            logging.warning(f'SUCCESSFULLY PROCESSED MESSAGE FROM: {message.topic}')
        except Exception as err:
            logging.warning(f'AN ERROR HAS OCCURED: topic: {message.topic} | error: {err}')
            try:
                error_log = db.ErrorLogs(
                    timestamp=spb.millis(),
                    error_string=str(err),
                    payload=message.payload,
                    topic=message.topic
                )
                db.session.add(error_log)
                db.session.commit()
            except Exception as err:
                logging.error(f'COULD NOT SAVE ERROR TO DATABASE: topic: {message.topic} | error: {err}')

    return load_message


@sparkplug_message
def on_birth_message(client, topic: dict, payload: dict):
    logging.warning(f'REBIRTH REVCIEVED: {topic["device_id"]}')
    if 'metrics' not in payload:
        return

    for metric in payload['metrics']:
        if 'alias' not in metric:
            logging.warning(f"{metric['name']} has no alias")

    names_cache.set_metrics_names(
        group_id=topic['group_id'],
        node_id=topic['node_id'],
        device_id=topic['device_id'],
        metrics=payload['metrics']
    )


@sparkplug_message
def on_death_message(client, topic: dict, payload: dict):
    logging.warning('DEATH')


@sparkplug_message
def on_data_message(client, topic: dict, payload: dict):

    if names_cache.topic_in_cache(topic['group_id'], topic['node_id'], topic['device_id']):
        return
    logging.warning('NAMES NOT IN CACHE, REBIRTH COMMAND')

    rebirth_topic, payload = rebirth_message(topic)
    client.publish(rebirth_topic, payload.serialize())


@sparkplug_message
def on_cmd_message(client, topic: dict, payload: dict):
    logging.warning('CMD MESSAGE')


def on_connect(client, userdata, flags, rc):
    client.subscribe(f'spBv1.0/+/+/#')


mqtt_client.on_connect = on_connect

mqtt_client.message_callback_add('spBv1.0/+/NBIRTH/+', on_birth_message)
mqtt_client.message_callback_add('spBv1.0/+/NDEATH/+', on_death_message)

mqtt_client.message_callback_add('spBv1.0/+/DBIRTH/+/+', on_birth_message)
mqtt_client.message_callback_add('spBv1.0/+/DDEATH/+/+', on_death_message)

mqtt_client.message_callback_add('spBv1.0/+/NDATA/+', on_data_message)
mqtt_client.message_callback_add('spBv1.0/+/DDATA/+/+', on_data_message)

mqtt_client.message_callback_add('spBv1.0/+/NCMD/#', on_cmd_message)
mqtt_client.message_callback_add('spBv1.0/+/DCMD/+/#', on_cmd_message)

mqtt_client.connect(host=cfg.MQTT_HOST, port=cfg.MQTT_PORT, keepalive=cfg.MQTT_KEEPALIVE)

logging.warning('STARTING MQTT CLIENT LOOP')

mqtt_client.loop_forever()