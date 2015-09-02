#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-

__author__ = "Kyle Gordon"
__copyright__ = "Copyright (C) Kyle Gordon"

import os
import logging
import signal
import socket
import time
import sys
import csv

import mosquitto
import ConfigParser
import json
import setproctitle

from datetime import datetime, timedelta

# Read the config file
config = ConfigParser.RawConfigParser()
config.read("/etc/mqtt-aprs/mqtt-aprs.cfg")

# Use ConfigParser to pick out the settings
DEBUG = config.getboolean("global", "debug")
LOGFILE = config.get("global", "logfile")
MQTT_HOST = config.get("global", "mqtt_host")
MQTT_PORT = config.getint("global", "mqtt_port")
MQTT_TOPIC = config.get("global", "mqtt_topic")
MQTT_USER = config.get("global", "mqtt_user")
MQTT_PASS = config.get("global", "mqtt_pass")

APRS_SERVER = config.get("global", "aprs_server")
APRS_PORT = config.getint("global", "aprs_port")
APRS_CALLSIGN = config.get("global", "aprs_callsign")
APRS_SSID = config.get("global", "aprs_ssid")
APRS_PASS = config.get("global", "aprs_pass")
APRS_SYMB = config.get("global", "aprs_symb")
APRS_TABL = config.get("global", "aprs_tabl")

APPNAME = "mqtt-aprs"
PRESENCETOPIC = "clients/" + socket.getfqdn() + "/" + APPNAME + "/state"
setproctitle.setproctitle(APPNAME)
client_id = APPNAME + "_%d" % os.getpid()
mqttc = mosquitto.Mosquitto(client_id)

LOGFORMAT = "%(asctime)-15s %(message)s"

if DEBUG:
    logging.basicConfig(filename=LOGFILE,
                        level=logging.DEBUG,
                        format=LOGFORMAT)
else:
    logging.basicConfig(filename=LOGFILE,
                        level=logging.INFO,
                        format=LOGFORMAT)

logging.info("Starting " + APPNAME)
logging.info("INFO MODE")
logging.debug("DEBUG MODE")

# All the MQTT callbacks start here


def on_publish(mosq, obj, mid):
    """
    What to do when a message is published
    """
    logging.debug("MID " + str(mid) + " published.")


def on_subscribe(mosq, obj, mid, qos_list):
    """
    What to do in the event of subscribing to a topic"
    """
    logging.debug("Subscribe with mid " + str(mid) + " received.")


def on_unsubscribe(mosq, obj, mid):
    """
    What to do in the event of unsubscribing from a topic
    """
    logging.debug("Unsubscribe with mid " + str(mid) + " received.")


def on_connect(mosq, obj, result_code):
    """
    Handle connections (or failures) to the broker.
    This is called after the client has received a CONNACK message
    from the broker in response to calling connect().
    The parameter rc is an integer giving the return code:

    0: Success
    1: Refused – unacceptable protocol version
    2: Refused – identifier rejected
    3: Refused – server unavailable
    4: Refused – bad user name or password (MQTT v3.1 broker only)
    5: Refused – not authorised (MQTT v3.1 broker only)
    """
    logging.debug("on_connect RC: " + str(result_code))
    if result_code == 0:
        logging.info("Connected to %s:%s", MQTT_HOST, MQTT_PORT)
        # Publish retained LWT as per
        # http://stackoverflow.com/q/97694
        # See also the will_set function in connect() below
        mqttc.publish(PRESENCETOPIC, "1", retain=True)
        process_connection()
    elif result_code == 1:
        logging.info("Connection refused - unacceptable protocol version")
        cleanup()
    elif result_code == 2:
        logging.info("Connection refused - identifier rejected")
        cleanup()
    elif result_code == 3:
        logging.info("Connection refused - server unavailable")
        logging.info("Retrying in 30 seconds")
        time.sleep(30)
    elif result_code == 4:
        logging.info("Connection refused - bad user name or password")
        cleanup()
    elif result_code == 5:
        logging.info("Connection refused - not authorised")
        cleanup()
    else:
        logging.warning("Something went wrong. RC:" + str(result_code))
        cleanup()


def on_disconnect(mosq, obj, result_code):
    """
    Handle disconnections from the broker
    """
    if result_code == 0:
        logging.info("Clean disconnection")
    else:
        logging.info("Unexpected disconnection! Reconnecting in 5 seconds")
        logging.debug("Result code: %s", result_code)
        time.sleep(5)


def on_message(mosq, obj, msg):
    """
    What to do when the client recieves a message from the broker
    """
    logging.debug("Received: " + msg.payload +
                  " received on topic " + msg.topic +
                  " with QoS " + str(msg.qos))
    process_message(msg)


def on_log(mosq, obj, level, string):
    """
    What to do with debug log output from the MQTT library
    """
    logging.debug(string)

# End of MQTT callbacks


def process_connection():
    """
    What to do when a new connection is established
    """
    logging.debug("Processing connection")
    mqttc.subscribe(MQTT_TOPIC, 2)


def process_message(msg):
    """
    What to do with the message that's arrived.
    Parse out the JSON to get Lat & Long
    Post it to the APRS server
    """
    logging.debug("Processing : " + msg.topic)
    data = json.loads(msg.payload)
    
    if data['_type'] == 'location':
        address = APRS_CALLSIGN + '-' + APRS_SSID + '>APRS,TCPIP*:'
        lat = deg_to_dms(float(data['lat']),0)
        lon = deg_to_dms(float(data['lon']),1)
        position = "=" + lat + APRS_TABL + lon + APRS_SYMB

        packet = address + position + ' mqtt-aprs\n'
        logging.debug("Packet is %s", packet)
        send_packet(packet)
    else:
        logging.debug("Not a location message")


def deg_to_dms(deg, long_flag):
    """
    Convert degrees to degrees, minutes and seconds
    Taken from http://stackoverflow.com/questions/2056750/lat-long-to-minutes-and-seconds

    Checked with http://transition.fcc.gov/mb/audio/bickel/DDDMMSS-decimal.html

    For example: 40.058333 in decimal
    4903.50N is 49 degrees 3 minutes 30 seconds north.

    N.B. Seconds not used in APRS, see http://www.aprs.org/doc/APRS101.PDF page 23-24.
    """
    d = int(deg)
    md = round(abs(deg - d) * 60,2)
    m = int(md)
    hm = int((md - m) * 100)

    if long_flag:
        if d > 0:
            suffix = "E"
        if d < 0:
            suffix = "W"
        #d = str(d).strip('-')

        # Strip the sign, and pad to 3 characters
        aprsdms = str(d).strip('-').zfill(3) + str(m).zfill(2) + "." + str(hm).zfill(2) + suffix
        logging.debug("Computed longitude to be  : %s", aprsdms)
    else:
        if d > 0:
            suffix = "N"
        if d < 0:
            suffix = "S"

        # Strip the sign, and pad to 2 characters
        aprsdms = str(d).strip('-').zfill(2) + str(m).zfill(2) + "." + str(hm).zfill(2) + suffix
        logging.debug("Computed latitude to be : %s", aprsdms)

    return aprsdms

 
def send_packet(packet):
    """
    Create a socket, log on to the APRS server, and send the packet
    """
    logging.debug(APRS_SERVER + ":" + str(APRS_PORT))
    connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connection.connect((APRS_SERVER, APRS_PORT))
    
    # Log on to APRS server
    connection.send('user ' + APRS_CALLSIGN + ' pass ' + APRS_PASS + ' vers "mqtt-zabbix" \n')
    
    # Send APRS packet
    logging.debug("Sending %s", packet)
    connection.send(packet)
    logging.debug("Sent packet at: " + time.ctime() )
    
    # Close socket -- must be closed to avoidbuffer overflow
    time.sleep(15) # 15 sec. delay
    connection.shutdown(0)
    connection.close()


def cleanup(signum, frame):
    """
    Signal handler to ensure we disconnect cleanly
    in the event of a SIGTERM or SIGINT.
    """
    logging.info("Disconnecting from broker")
    # Publish a retained message to state that this client is offline
    mqttc.publish(PRESENCETOPIC, "0", retain=True)
    mqttc.disconnect()
    logging.info("Exiting on signal %d", signum)
    sys.exit(signum)


def connect():
    """
    Connect to the broker, define the callbacks, and subscribe
    This will also set the Last Will and Testament (LWT)
    The LWT will be published in the event of an unclean or
    unexpected disconnection.
    """
    logging.debug("Connecting to %s:%s", MQTT_HOST, MQTT_PORT)
    # Set the Last Will and Testament (LWT) *before* connecting
    mqttc.will_set(PRESENCETOPIC, "0", qos=0, retain=True)
    mqttc.username_pw_set(MQTT_USER, MQTT_PASS)
    result = mqttc.connect(MQTT_HOST, MQTT_PORT, 60, True)
    if result != 0:
        logging.info("Connection failed with error code %s. Retrying", result)
        time.sleep(10)
        connect()

    # Define the callbacks
    mqttc.on_connect = on_connect
    mqttc.on_disconnect = on_disconnect
    mqttc.on_publish = on_publish
    mqttc.on_subscribe = on_subscribe
    mqttc.on_unsubscribe = on_unsubscribe
    mqttc.on_message = on_message
    if DEBUG:
        mqttc.on_log = on_log

# Use the signal module to handle signals
signal.signal(signal.SIGTERM, cleanup)
signal.signal(signal.SIGINT, cleanup)

# Connect to the broker
connect()

# Try to loop_forever until interrupted
try:
    mqttc.loop_forever()
except KeyboardInterrupt:
    logging.info("Interrupted by keypress")
    sys.exit(0)
