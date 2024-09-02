from kubernetes import client as k8s_client, config
from flytekit import workflow
from flytekit.configuration import SerializationSettings
from flytekit.exceptions import user as _user_exceptions
from flytekitplugins.job.task import Job, JobFunctionTask  # Import from the plugin file
# Step 1: Define the Job Specification
job_spec = k8s_client.V1Job(
    api_version="batch/v1",
    kind="Job",
    metadata=k8s_client.V1ObjectMeta(name="example-job"),
    spec=k8s_client.V1JobSpec(
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
)

# Step 2: Define Metadata
metadata = k8s_client.V1ObjectMeta(
    name="example-job",
    labels={"example-label": "example-value"},
    annotations={"example-annotation": "example-value"}
)

# Step 3: Create Job Configuration
job_config = Job(job_spec=job_spec, metadata=metadata)

# Step 4: Create Job Function Task
def sample_task_function():
    pass

job_task = JobFunctionTask(task_config=job_config, task_function=sample_task_function)

# Step 5: Get Custom Job Dictionary
serialization_settings = SerializationSettings()
job_dict = job_task.get_custom(serialization_settings)

print(job_dict)


# from kubernetes import client as k8s_client, config
# from flytekit import workflow
# from flytekit.configuration import SerializationSettings
# from flytekit.exceptions import user as _user_exceptions
# from flytekitplugins.job.task import Job, JobFunctionTask  # Import from the plugin file

# # Load Kubernetes configuration
# config.load_kube_config()

# # Define the Kubernetes Job specification
# job_spec = k8s_client.V1Job(
#     api_version="batch/v1",
#     kind="Job",
#     metadata=k8s_client.V1ObjectMeta(name="example-job"),
#     spec=k8s_client.V1JobSpec(
#         template=k8s_client.V1PodTemplateSpec(
#             spec=k8s_client.V1PodSpec(
#                 containers=[
#                     k8s_client.V1Container(
#                         name="example",
#                         image="busybox",
#                         command=["sleep", "100s"]
#                     )
#                 ],
#                 restart_policy="Never"
#             )
#         )
#     )
# )

# # Create the JobSpec instance
# job_spec_config = Job(
#     job_spec=job_spec,
#     labels={"example-label": "value"},
#     annotations={"example-annotation": "value"}
# )

# # Define the task function
# def my_task_function():
#     print("Kubernetes Job has been scheduled.")

# # Define the Flyte task using the JobFunctionTask
# job_task = JobFunctionTask(
#     task_config=job_spec_config,
#     task_function=my_task_function
# )
# # Define the Flyte workflow
# @workflow
# def my_workflow():
#     job_task()

# if __name__ == "__main__":
#     my_workflow()
