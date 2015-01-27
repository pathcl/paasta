import sys
import time

import mock

sys.path.append('../')
import paasta_tools
from paasta_tools import paasta_serviceinit

@when(u'we run the job test-service.main')
def step_impl(context):
    trivial_app_config = {
        'id': 'test-service.main',
        'cmd': '/bin/sleep 1m',
    }
    with mock.patch('paasta_tools.bounce_lib.create_app_lock'):
        paasta_tools.bounce_lib.create_marathon_app('test-service.main', trivial_app_config, context.client)

@when(u'we wait for it to be deployed')
def step_impl(context):
    print "Sleeping 10 seconds to wait for test-service to be deployed."
    time.sleep(10)

@then(u'paasta_serviceinit status_marathon_job should return "Healthy"')
def step_impl(context):
    normal_instance_count = 1
    client = context.client
    app_id = 'test-service.main'
    service = 'test-service'
    instance = 'main'

    output = paasta_serviceinit.status_marathon_job(service, instance, app_id, normal_instance_count, client)
    assert "Healthy" in output

@then(u'paasta_serviceinit restart should get new task_ids')
def step_impl(context):
    normal_instance_count = 1
    client = context.client
    app_id = 'test-service.main'
    service = 'test-service'
    instance = 'main'

    old_tasks = context.client.get_app(app_id).tasks
    paasta_serviceinit.restart_marathon_job(service, instance, app_id, normal_instance_count, client)
    print "Sleeping 5 seconds to wait for test-service to be restarted."
    time.sleep(5)
    new_tasks = context.client.get_app(app_id).tasks
    print "Tasks before the restart: %s" % old_tasks
    print "Tasks after  the restart: %s" % new_tasks
    assert old_tasks != new_tasks

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4