from kubernetes import client as k8s_client, config
from flytekit import workflow, task
from flytekit.configuration import SerializationSettings
from flytekit.exceptions import user as _user_exceptions
from flytekitplugins.job.task import Job, JobFunctionTask  
# Import from the plugin file
@task
def create_job_spec() -> Job:
    job_spec = k8s_client.V1JobSpec(
        template=k8s_client.V1PodTemplateSpec(
            spec=k8s_client.V1PodSpec(
                containers=[
                    k8s_client.V1Container(
                        name="example",
                        image="busybox",
                        command=["sleep", "100s"]
                    )
                ],
                restart_policy="Never"
            )
        )
    )

    metadata = k8s_client.V1ObjectMeta(
        name="example-job",
        labels={"example-label": "example-value"},
        annotations={"example-annotation": "example-value"}
    )

    return Job(job_spec=job_spec.to_dict(), metadata=metadata.to_dict())
def dummy_task_function():
    # some dummy task function
    print("Executing dummy task function")

@workflow
def my_workflow():
    job_config = create_job_spec()
    job_task = JobFunctionTask(task_config=job_config, task_function=dummy_task_function)
    job_task()

if __name__ == "__main__":
    my_workflow()