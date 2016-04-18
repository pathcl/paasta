#!/usr/bin/env python
# Copyright 2015 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import argparse
import datetime
import json
from socket import gethostbyname

import requests
from dateutil import tz
from pytimeparse import timeparse

from paasta_tools.mesos_tools import get_mesos_leader
from paasta_tools.mesos_tools import MesosMasterConnectionError


PORT = 5050


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-d', '--duration',
        type=timedelta_type,
        default='1h',
        help="Duration of the maintenance window. Any pytimeparse unit is supported.",
    )
    parser.add_argument(
        '-s', '--start',
        default=now(),
        help="Time to start the maintenance window. Defaults to now.",
    )
    parser.add_argument(
        'action',
        choices=['drain', 'undrain', 'down', 'up', 'status'],
        help="Action to perform on the speicifed hosts",
    )
    parser.add_argument(
        'hostname',
        nargs='*',
        help="Hostname(s) of machine(s) to start draining.",
    )
    options = parser.parse_args()
    return options


def timedelta_type(value):
    """Return the delta in nanoseconds.
    :param value: a string containing a time format supported by :mod:`pytimeparse`
    """
    if value is None:
        return None
    return seconds_to_nanoseconds(timeparse.timeparse(value))


def datetime_seconds_from_now(seconds):
    return now() + datetime.timedelta(seconds=seconds)


def now():
    return datetime.datetime.now(tz.tzutc())


def seconds_to_nanoseconds(seconds):
    return seconds * 1000000000


def datetime_to_nanoseconds(dt):
    return seconds_to_nanoseconds(int(dt.strftime("%s")))


def build_start_maintenance_payload(hostnames):
    return get_machine_ids(hostnames)


def get_machine_ids(hostnames):
    machine_ids = []
    for hostname in hostnames:
        machine_id = dict()
        # This is to allow specifying a hostname as "hostname|ipaddr"a
        # to avoid querying DNS for the IP.
        if '|' in hostname:
            (host, ip) = hostname.split('|')
            machine_id['hostname'] = host
            machine_id['ip'] = ip
        else:
            machine_id['hostname'] = hostname
            machine_id['ip'] = gethostbyname(hostname)
        machine_ids.append(machine_id)
    return machine_ids


def build_maintenance_schedule_payload(leader, hostnames, start, duration, drain=True):
    schedule = get_maintenance_schedule(leader).json()
    machine_ids = get_machine_ids(hostnames)

    unavailability = dict()
    unavailability['start'] = dict()
    unavailability['start']['nanoseconds'] = int(start)
    unavailability['duration'] = dict()
    unavailability['duration']['nanoseconds'] = int(duration)

    window = dict()
    window['machine_ids'] = machine_ids
    window['unavailability'] = unavailability

    if schedule:
        for existing_window in schedule['windows']:
            for existing_machine_id in existing_window['machine_ids']:
                if existing_machine_id in machine_ids:
                    existing_window['machine_ids'].remove(existing_machine_id)
                    if not existing_window['machine_ids']:
                        schedule['windows'].remove(existing_window)
        if drain:
            windows = schedule['windows'] + [window]
        else:
            windows = schedule['windows']
    else:
        windows = [window]

    payload = dict()
    payload['windows'] = windows

    return payload


def get_maintenance_status(leader):
    credentials = load_credentials()
    endpoint = '/master/maintenance/status'
    url = "http://%s:%s%s" % (leader, PORT, endpoint)
    return requests.get(url, auth=credentials, timeout=15)


def get_maintenance_schedule(leader):
    credentials = load_credentials()
    endpoint = '/master/maintenance/schedule'
    url = "http://%s:%s%s" % (leader, PORT, endpoint)
    return requests.get(url, auth=credentials, timeout=15)


def load_credentials(mesos_secrets='/nail/etc/mesos-secrets'):
    with open(mesos_secrets) as data_file:
        data = json.load(data_file)
    username = data['credentials'][0]['principal']
    password = data['credentials'][0]['secret']
    return username, password


def send_payload(url, payload):
    credentials = load_credentials()
    return requests.post(url, data=json.dumps(payload), auth=credentials, timeout=15)


def drain(leader, hostnames, start, duration):
    payload = build_maintenance_schedule_payload(leader, hostnames, start, duration, drain=True)
    endpoint = '/master/maintenance/schedule'
    url = "http://%s:%s%s" % (leader, PORT, endpoint)
    print send_payload(url, payload).text


def undrain(leader, hostnames, start, duration):
    payload = build_maintenance_schedule_payload(leader, hostnames, start, duration, drain=False)
    endpoint = '/master/maintenance/schedule'
    url = "http://%s:%s%s" % (leader, PORT, endpoint)
    print send_payload(url, payload).text


def down(leader, hostnames):
    payload = build_start_maintenance_payload(hostnames)
    endpoint = '/master/machine/down'
    url = "http://%s:%s%s" % (leader, PORT, endpoint)
    print send_payload(url, payload).text


def up(leader, hostnames):
    payload = build_start_maintenance_payload(hostnames)
    endpoint = '/master/machine/up'
    url = "http://%s:%s%s" % (leader, PORT, endpoint)
    print send_payload(url, payload).text


def status(leader):
    status = get_maintenance_status(leader)
    print "%s:%s" % (status, status.text)


def schedule(leader):
    schedule = get_maintenance_schedule(leader)
    print "%s:%s" % (schedule, schedule.text)


def paasta_maintenance():
    """Manipulate the maintenance state of a PaaSTA host."""
    args = parse_args()

    action = args.action
    hostnames = args.hostname

    if action not in ['drain', 'undrain', 'down', 'up', 'status']:
        print "action must be 'drain', 'undrain', 'down', 'up', or 'status'"
        return

    if action != 'status' and not hostnames:
        print "You must specify one or more hostnames"
        return

    try:
        leader = get_mesos_leader()
    except MesosMasterConnectionError:
        print "Run this script from a mesos master!"
        return
    print "Leader: %s" % leader

    start = args.start.strftime("%s")
    duration = args.duration

    if action == 'drain':
        drain(leader, hostnames, start, duration)
    elif action == 'undrain':
        undrain(leader, hostnames, start, duration)
    elif action == 'down':
        down(leader, hostnames)
    elif action == 'up':
        up(leader, hostnames)
    elif action == 'status':
        status(leader)
        schedule(leader)
    else:
        print "Unknown Action: %s" % action


if __name__ == '__main__':
    paasta_maintenance()
