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
            if container.image_name == 'tutum.co/mhubig/dockup:latest':
                continue
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

            dockup_service = tutum.Service.create(
                                autodestroy="ALWAYS",
                                image="tutum.co/mhubig/dockup:latest",
                                name='dockup-' + service.name,
                                target_num_containers=1,
                                container_envvars=container_envvars,
                                bindings=container.bindings
            )

            dockup_service.save()
            time.sleep(10)
            dockup_service.start()
            time.sleep(10)
            while dockup_service.state != 'Not running':
                dockup_service = tutum.Service.fetch(dockup_service.uuid)
                time.sleep(10)
            dockup_service.delete()
            time.sleep(30)
    

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

    schedule.every(1).minutes.do(backup_volumes) # change timing

    while True:
        schedule.run_pending()
        time.sleep(1)
