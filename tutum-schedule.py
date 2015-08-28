import functools
import schedule
import time
import tutum
import os

def catch_exceptions(job_func):
    @functools.wraps(job_func)
    def wrapper(*args, **kwargs):
        try:
            job_func(*args, **kwargs)
        except:
            import traceback
            print(traceback.format_exc())
    return wrapper

@catch_exceptions
def start_service(uuid):
    """
    Wrapper function to start a stopped Tutum Service by its UUID.
    """
    service = tutum.Service.fetch(uuid)
    service.start()

@catch_exceptions
def create_service(**kwargs):
    """
    Wrapper function to create a new Tutum Service.

    For possible parameters, see https://docs.tutum.co/v2/api/?python#create-a-new-service.
    """
    service = tutum.Service.create(**kwargs)
    service.save()
    service.start()

def get_uuid(uri):
    return uri.rsplit('/')[-2:-1][0]

def backup_volumes():
    service_api_uri = os.environ['TUTUM_SERVICE_API_URI']
    service_uuid = get_uuid(service_api_uri)

    service = tutum.Service.fetch(service_uuid)
    stack_uri = service.stack
    stack_uuid = get_uuid(stack_uri)
    stack = tutum.Stack.fetch(stack_uuid)

    for s in stack.services:
        uuid = get_uuid(s)
        service = tutum.Service.fetch(uuid)

        for container_uri in service.containers:
            container_uuid = get_uuid(container_uri)
            container = tutum.Container.fetch(container_uuid)
            time.sleep(10)
            if container.image_name == 'tutum.co/mhubig/scheduler:latest':
                continue
            if container.image_name == 'tutum/mysql:5.5':
                dump_sql(service, container)
            paths_to_backup = ''
            
            for binding in container.bindings:
                paths_to_backup += binding['container_path'] + ' '

            backup_name = 'backup-' + container.name

            container_envvars = [
            {
                "key": "AWS_ACCESS_KEY_ID",
                "value": os.environ["AWS_ACCESS_KEY_ID"]
            },
            {
                "key": "AWS_SECRET_ACCESS_KEY",
                "value": os.environ["AWS_SECRET_ACCESS_KEY"]
            },
            {
                "key": "AWS_DEFAULT_REGION",
                "value": "eu-central-1"
            },
            {
                "key": "BACKUP_NAME",
                "value": backup_name
            },
            {
                "key": "PATHS_TO_BACKUP",
                "value": paths_to_backup[:-1]
            },
            {
                "key": "S3_BUCKET_NAME",
                "value": 'backups.imko.de'
            }
            ]

            mybinding = [{"host_path": None,
                         "container_path": None,
                         "rewritable": True,
                         "volumes_from": service.resource_uri}]

            dockup_service = tutum.Service.create(
                                autodestroy="ALWAYS",
                                image="tutum.co/mhubig/dockup:latest",
                                name='dockup-' + service.name,
                                target_num_containers=1,
                                container_envvars=container_envvars,
                                bindings=mybinding
            )

            dockup_service.save()
            dockup_service.start()
            while dockup_service.state != 'Not running':
                dockup_service = tutum.Service.fetch(dockup_service.uuid)
                time.sleep(10)
            dockup_service.delete()


def dump_sql(service, container):

    mybinding = [{"host_path": None,
                  "container_path": None,
                  "rewritable": True,
                  "volumes_from": service.resource_uri}]

    dump_service = tutum.Service.create(
                        autodestroy="OFF",
                        image="tutum/mysql:5.5",
                        name='dump-' + container.name,
                        target_num_containers=1,
                        bindings=mybinding
    )

    dump_service.save()

    dump_service = tutum.Service.fetch(dump_service.uuid)

    linked_to_service = [{
      "from_service": dump_service.resource_uri,
      "name": service.name,
      "to_service": service.resource_uri
    }]

    dump_service.linked_to_service = linked_to_service

    dump_service.save()

    cmd = 'sh -c "mysqldump -h$DB_1_ENV_TUTUM_CONTAINER_HOSTNAME -u$DB_1_ENV_MYSQL_USER -p$DB_1_ENV_MYSQL_PASS -P$DB_1_PORT_3306_TCP_PORT -t -n -B --all-databases > /etc/mysql/backup.sql"'

    dump_service = tutum.Service.fetch(dump_service.uuid)
    dump_service.run_command = cmd
    dump_service.save()

    dump_service.start()
    while dump_service.state != 'Stopped':
        dump_service = tutum.Service.fetch(dump_service.uuid)
        time.sleep(10)
    dump_service.delete()
    

if __name__ == "__main__":
    """
    Add your own scheduled jobs here.
    See https://github.com/dbader/schedule for schedule syntax.

    Examples:

    If you have already created a Service on Tutum with the UUID of 
    '2463a0c3-bacd-4195-8493-bcbb49681f4a', you can start it every
    hour with:
    schedule.every().hour.do(start_service, '2463a0c3-bacd-4195-8493-bcbb49681f4a')

    If you would like to create a Service to be run every day at 2:15 AM, set
    the schedule with:
    schedule.every(5).day.at("2:15").do(create_service, 
                                        image='tutum.co/user/my-job', 
                                        name='created',
                                        autodestroy="ALWAYS")
    """

    schedule.every().day.at("11:45").do(backup_volumes)

    while True:
        schedule.run_pending()
        time.sleep(1)
