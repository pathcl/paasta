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
import datetime
import json

import mock
from dateutil import tz

from paasta_tools.cli.cmds.maintenance import build_start_maintenance_payload
from paasta_tools.cli.cmds.maintenance import datetime_seconds_from_now
from paasta_tools.cli.cmds.maintenance import datetime_to_nanoseconds
from paasta_tools.cli.cmds.maintenance import get_machine_ids
from paasta_tools.cli.cmds.maintenance import get_maintenance_schedule
from paasta_tools.cli.cmds.maintenance import get_maintenance_status
from paasta_tools.cli.cmds.maintenance import load_credentials
from paasta_tools.cli.cmds.maintenance import PORT
from paasta_tools.cli.cmds.maintenance import seconds_to_nanoseconds
from paasta_tools.cli.cmds.maintenance import send_payload
from paasta_tools.cli.cmds.maintenance import timedelta_type


def test_timedelta_type_one():
    assert timedelta_type(value=None) is None


def test_timedelta_type():
    assert timedelta_type(value='1 hour') == 3600 * 1000000000


@mock.patch('paasta_tools.cli.cmds.maintenance.now')
def test_datetime_seconds_from_now(
    mock_now,
):
    mock_now.return_value = datetime.datetime(2016, 4, 16, 0, 23, 25, 157145, tzinfo=tz.tzutc())
    expected = datetime.datetime(2016, 4, 16, 0, 23, 40, 157145, tzinfo=tz.tzutc())
    assert datetime_seconds_from_now(15) == expected


def test_seconds_to_nanoseconds():
    assert seconds_to_nanoseconds(60) == 60 * 1000000000


def test_datetime_to_nanoseconds():
    dt = datetime.datetime(2016, 4, 16, 0, 23, 40, 157145, tzinfo=tz.tzutc())
    expected = 1460766220000000000
    assert datetime_to_nanoseconds(dt) == expected


@mock.patch('paasta_tools.cli.cmds.maintenance.gethostbyname')
def test_build_start_maintenance_payload(
    mock_gethostbyname,
):
    ip = '169.254.121.212'
    mock_gethostbyname.return_value = ip
    hostname = 'fqdn1.example.org'
    hostnames = [hostname]

    assert build_start_maintenance_payload(hostnames) == get_machine_ids(hostnames)


@mock.patch('paasta_tools.cli.cmds.maintenance.gethostbyname')
def test_get_machine_ids_one_host(
    mock_gethostbyname,
):
    ip = '169.254.121.212'
    mock_gethostbyname.return_value = ip
    hostname = 'fqdn1.example.org'
    hostnames = [hostname]
    expected = [
        {
            'hostname': hostname,
            'ip': ip
        }
    ]
    assert get_machine_ids(hostnames) == expected


@mock.patch('paasta_tools.cli.cmds.maintenance.gethostbyname')
def test_get_machine_ids_multiple_hosts(
    mock_gethostbyname,
):
    ip1 = '169.254.121.212'
    ip2 = '169.254.121.213'
    ip3 = '169.254.121.214'
    mock_gethostbyname.side_effect = [ip1, ip2, ip3]
    hostname1 = 'fqdn1.example.org'
    hostname2 = 'fqdn2.example.org'
    hostname3 = 'fqdn3.example.org'
    hostnames = [hostname1, hostname2, hostname3]
    expected = [
        {
            'hostname': hostname1,
            'ip': ip1
        },
        {
            'hostname': hostname2,
            'ip': ip2
        },
        {
            'hostname': hostname3,
            'ip': ip3
        }
    ]
    assert get_machine_ids(hostnames) == expected


def test_get_machine_ids_multiple_hosts_ips():
    ip1 = '169.254.121.212'
    ip2 = '169.254.121.213'
    ip3 = '169.254.121.214'
    hostname1 = 'fqdn1.example.org'
    hostname2 = 'fqdn2.example.org'
    hostname3 = 'fqdn3.example.org'
    hostnames = [hostname1 + '|' + ip1, hostname2 + '|' + ip2, hostname3 + '|' + ip3]
    expected = [
        {
            'hostname': hostname1,
            'ip': ip1
        },
        {
            'hostname': hostname2,
            'ip': ip2
        },
        {
            'hostname': hostname3,
            'ip': ip3
        }
    ]
    assert get_machine_ids(hostnames) == expected


def test_build_maintenance_schedule_payload():
    pass


@mock.patch('paasta_tools.cli.cmds.maintenance.open', create=True)
def test_load_credentials(
    mock_open,
):
    credentials = {
        'credentials': [
            {
                'principal': 'username',
                'secret': 'password'
            }
        ]
    }

    mock_open.side_effect = mock.mock_open(read_data=json.dumps(credentials))

    assert load_credentials() == ('username', 'password')


@mock.patch('paasta_tools.cli.cmds.maintenance.requests.get')
@mock.patch('paasta_tools.cli.cmds.maintenance.load_credentials')
def test_get_maintenance_status(
    mock_load_credentials,
    mock_get,
):
    leader = 'some.leader.org'
    credentials = ('username', 'password')
    mock_load_credentials.return_value = credentials
    get_maintenance_status(leader)
    endpoint = '/master/maintenance/status'
    url = 'http://%s:%s%s' % (leader, PORT, endpoint)
    assert mock_get.call_count == 1
    assert mock_get.call_args == mock.call(url, auth=credentials, timeout=15)


@mock.patch('paasta_tools.cli.cmds.maintenance.requests.get')
@mock.patch('paasta_tools.cli.cmds.maintenance.load_credentials')
def test_get_maintenance_schedule(
    mock_load_credentials,
    mock_get,
):
    leader = 'some.leader.org'
    credentials = ('username', 'password')
    mock_load_credentials.return_value = credentials
    get_maintenance_schedule(leader)
    endpoint = '/master/maintenance/schedule'
    url = 'http://%s:%s%s' % (leader, PORT, endpoint)
    assert mock_get.call_count == 1
    assert mock_get.call_args == mock.call(url, auth=credentials, timeout=15)


@mock.patch('paasta_tools.cli.cmds.maintenance.requests.post')
@mock.patch('paasta_tools.cli.cmds.maintenance.load_credentials')
def test_send_payload(
    mock_load_credentials,
    mock_post,
):
    credentials = ('username', 'password')
    mock_load_credentials.return_value = credentials
    url = 'http://some.leader.org:1234/some/endpoint'
    payload = {'fake_key': 'fake_value'}
    send_payload(url, payload)
    assert mock_post.call_count == 1
    assert mock_post.call_args == mock.call(url, data=json.dumps(payload), auth=credentials, timeout=15)
