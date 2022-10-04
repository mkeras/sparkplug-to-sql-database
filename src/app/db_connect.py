from enum import unique
import config as cfg
from sqlalchemy import create_engine, Column, String, Integer, Float, Enum, Boolean, BINARY, LargeBinary, BLOB, BigInteger, ForeignKey, ForeignKeyConstraint, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.orm import sessionmaker, relationship, backref
from sparkplug_b.enums import DataTypes, MessageTypes
import logging
from datetime import datetime
import time

time.sleep(2)

engine = create_engine(cfg.SQL_DATABASE_URI, echo=False)

logging.warning(f'CONNECTING DATABASE {engine.url}')

if not database_exists(engine.url):
    logging.warning(f"CREATING DATABASE '{cfg.SQL_DATABASE_NAME}'...")
    create_database(engine.url)
    logging.warning(f"Done.")
else:
    logging.warning(f"DATABASE '{cfg.SQL_DATABASE_NAME}' ALREADY EXISTS")

base = declarative_base()


class SparkplugBGroup(base):
    __tablename__ = 'spb_v1_0_groups'

    id = Column(Integer, primary_key=True)

    name = Column(String(255), nullable=False, unique=True)


class SparkplugBEdgeNode(base):
    __tablename__ = 'spb_v1_0_edge_nodes'

    id = Column(Integer, primary_key=True)

    group_id = Column(ForeignKey('spb_v1_0_groups.id'), nullable=False)
    name = Column(String(255), nullable=False)

    __table_args__ = (
        UniqueConstraint('group_id', 'name', name='_edge_node_uc'),
    )


class SparkplugBDevice(base):
    __tablename__ = 'spb_v1_0_edge_devices'

    id = Column(Integer, primary_key=True)

    edge_node_id = Column(Integer, ForeignKey('spb_v1_0_edge_nodes.id'))

    name = Column(String(255), nullable=False)

    __table_args__ = (
        UniqueConstraint('edge_node_id', 'name', name='_device_node_uc'),
    )


class SparkplugBPayload(base):
    __tablename__ = 'spb_v1_0_payloads'

    id = Column(Integer, primary_key=True)

    edge_node_id = Column(ForeignKey('spb_v1_0_edge_nodes.id'), nullable=False)
    edge_device_id = Column(ForeignKey('spb_v1_0_edge_devices.id'), nullable=True)

    timestamp = Column(BigInteger, index=True)
    sequence = Column(Integer)
    message_type = Column(Enum(MessageTypes))

    raw_payload = Column(LargeBinary(26_214_400))  # Max 25MB size, MQTT Payloads can actually be up to 256MB


class SparkplugBMetric(base):
    __tablename__ = 'spb_v1_0_metrics'

    id = Column(Integer, primary_key=True)
    payload_id = Column(ForeignKey('spb_v1_0_payloads.id'), nullable=False)

    timestamp = Column(BigInteger, index=True)
    name = Column(String(256))
    alias = Column(Integer)

    datatype = Column(Enum(DataTypes))
    string_value = Column(String(2048))
    integer_value = Column(BigInteger)
    float_value = Column(Float)
    boolean_value = Column(Boolean)
    raw_value = Column(BINARY(255))

    def __repr__(self) -> str:
        string = '<SparkplugBMetric '
        for key, value in vars(self).items():
            if not key.startswith('_'):
                string += f' - {key}: {value}'
        return string + '>'


class ErrorLogs(base):
    __tablename__ = 'spb_v1_0_historian_error_logs'
    
    id = Column(Integer, primary_key=True)
    timestamp = Column(BigInteger, index=True)
    error_string = Column(String(2048))
    payload = Column(BLOB(2048))
    topic = Column(String(1024))

for cls in [
    SparkplugBGroup, SparkplugBDevice, SparkplugBEdgeNode, SparkplugBMetric, SparkplugBPayload, ErrorLogs
    ]:
    table_name = cls.__tablename__
    if engine.dialect.has_table(engine, table_name):
        logging.warning(f"TABLE '{table_name}' ALREADY EXISTS")
        continue

    logging.warning(f"CREATING TABLES...")
    base.metadata.create_all(engine)
    logging.warning(f"Done.")
    break


__Session = sessionmaker(bind=engine)
session = __Session()