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

from dateutil import tz
from pytimeparse import timeparse
from requests import Request
from requests import Session

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


def base_api():
    leader = get_mesos_leader()

    def execute_request(method, endpoint, **kwargs):
        url = "http://%s:%d/%s" % (leader, PORT, endpoint)
        timeout = 15
        s = Session()
        req = Request(method, url, **kwargs)
        prepared = s.prepare_request(req)
        try:
            resp = s.send(prepared,
                          timeout=timeout,
                          auth=load_credentials(),
                          )
            resp.raise_for_status()
        except Exception as e:
            print("Error executing API request calling %s. Got response code %d with error %s" %
                  (endpoint, resp.status_code, e))
    return execute_request


def master_api():
    def execute_master_api_request(method, endpoint, **kwargs):
        base_api_client = base_api()
        return base_api_client(method, "master/%s" % endpoint, **kwargs)
    return execute_master_api_request


def maintenance_api():
    def execute_schedule_api_request(method, endpoint, **kwargs):
        master_api_client = master_api()
        return master_api_client(method, "maintenance/%s" % endpoint, **kwargs)
    return execute_schedule_api_request


def get_schedule_client():
    def execute_schedule_api_request(method, endpoint, **kwargs):
        maintenance_api_client = maintenance_api()
        return maintenance_api_client(method, "schedule/%s" % endpoint, **kwargs)
    return execute_schedule_api_request


def get_maintenance_schedule():
    client_fn = get_schedule_client()
    return client_fn(method="GET", endpoint="")


def get_maintenance_status():
    client_fn = get_schedule_client()
    return client_fn(method="GET", endpoint="status")


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


def build_maintenance_schedule_payload(hostnames, start, duration, drain=True):
    schedule = get_maintenance_schedule().json()
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
    elif drain:
        windows = [window]
    else:
        windows = []

    payload = dict()
    payload['windows'] = windows

    return payload


def load_credentials(mesos_secrets='/nail/etc/mesos-secrets'):
    with open(mesos_secrets) as data_file:
        data = json.load(data_file)
    username = data['credentials'][0]['principal']
    password = data['credentials'][0]['secret']
    return username, password


def drain(hostnames, start, duration):
    payload = build_maintenance_schedule_payload(hostnames, start, duration, drain=True)
    client_fn = get_schedule_client()
    print client_fn(method="POST", endpoint="", data=json.dumps(payload)).text


def undrain(hostnames, start, duration):
    payload = build_maintenance_schedule_payload(hostnames, start, duration, drain=False)
    client_fn = get_schedule_client()
    print client_fn(method="POST", endpoint="", data=json.dumps(payload)).text


def down(hostnames):
    payload = build_start_maintenance_payload(hostnames)
    client_fn = master_api()
    print client_fn(method="POST", endpoint="machine/down", data=json.dumps(payload)).text


def up(hostnames):
    payload = build_start_maintenance_payload(hostnames)
    client_fn = master_api()
    print client_fn(method="POST", endpoint="machine/up", data=json.dumps(payload)).text


def status():
    status = get_maintenance_status()
    print "%s:%s" % (status, status.text)


def schedule():
    schedule = get_maintenance_schedule()
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
        drain(hostnames, start, duration)
    elif action == 'undrain':
        undrain(hostnames, start, duration)
    elif action == 'down':
        down(hostnames)
    elif action == 'up':
        up(hostnames)
    elif action == 'status':
        status()
        schedule()
    else:
        print "Unknown Action: %s" % action


if __name__ == '__main__':
    paasta_maintenance()
