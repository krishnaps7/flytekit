from dataclasses import dataclass
from typing import Callable, Dict, Optional, Any

from flytekit import PythonFunctionTask
from flytekit.configuration import SerializationSettings
from flytekit.exceptions import user as _user_exceptions
from flytekit.extend import TaskPlugins
from kubernetes import client as k8s_client

@dataclass
class Job:
    job_spec: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if not self.job_spec:
            raise _user_exceptions.FlyteValidationException("A job spec cannot be undefined")

class JobFunctionTask(PythonFunctionTask[Job]):
    def __init__(self, task_config: Job, task_function: Callable, namespace: str = "default", **kwargs):
        super(JobFunctionTask, self).__init__(
            task_config=task_config,
            task_type="job",
            task_function=task_function,
            **kwargs
        )
        self.namespace = namespace

    def get_custom(self, settings: SerializationSettings) -> Optional[Dict[str, Any]]:
        job_dict = self.task_config.job_spec
        if self.task_config.metadata:
            job_dict['metadata'] = self.task_config.metadata
        return job_dict

TaskPlugins.register_pythontask_plugin(Job, JobFunctionTask)     
  
# @dataclass
# class Job:
#     """
#     This class defines the custom options available for a Job task.
#     """
#     job_spec: k8s_client.V1JobSpec
#     metadata: Optional[k8s_client.V1ObjectMeta] = None

#     def __post_init__(self):
#         if not self.job_spec:
#             raise _user_exceptions.FlyteValidationException("A job spec cannot be undefined")

# class JobFunctionTask(PythonFunctionTask[Job]):
#     def __init__(self, task_config: Job, task_function: Callable, namespace: str = "default", **kwargs):
#         super(JobFunctionTask, self).__init__(
#             task_config=task_config,
#             task_type="job",
#             task_function=task_function,
#             **kwargs
#         )
#         self.namespace = namespace

#     def get_custom(self, settings: SerializationSettings) -> Optional[Dict[str, Any]]:
#         # Serialize the job spec to a dictionary
#         job_dict = self.task_config.job_spec.to_dict()

#         # Add metadata if it exists
#         if self.task_config.metadata:
#             job_dict['metadata'] = self.task_config.metadata.to_dict()

#         return job_dict

# # Register the custom task plugin
# TaskPlugins.register_pythontask_plugin(Job, JobFunctionTask)

# from dataclasses import dataclass
# from typing import Callable, Dict, Optional

# from flytekit import PythonFunctionTask
# from flytekit.exceptions import user as _user_exceptions
# from flytekit.extend import TaskPlugins
# from kubernetes import client as k8s_client

# @dataclass
# class Job:
#     """
#     This class defines the custom options available for a Job task.
#     """
#     job_spec: k8s_client.V1Job
#     labels: Optional[Dict[str, str]] = None
#     annotations: Optional[Dict[str, str]] = None

#     def __post_init__(self):
#         if not self.job_spec:
#             raise _user_exceptions.FlyteValidationException("A job spec cannot be undefined")

# class JobFunctionTask(PythonFunctionTask[Job]):
#     def __init__(self, task_config: Job, task_function: Callable, namespace: str = "default", **kwargs):
#         super(JobFunctionTask, self).__init__(
#             task_config=task_config,
#             task_type="job",
#             task_function=task_function,
#             **kwargs
#         )
#         self.namespace = namespace

#     def execute(self, **kwargs):
#         # Create the Kubernetes Job
#         api_instance = k8s_client.BatchV1Api()
#         try:
#             api_instance.create_namespaced_job(
#                 body=self.task_config.job_spec,
#                 namespace=self.namespace
#             )
#             print(f"Job {self.task_config.job_spec.metadata.name} created in namespace {self.namespace}")
#         except k8s_client.exceptions.ApiException as e:
#             raise _user_exceptions.FlyteExecutionException(f"Exception when creating Kubernetes Job: {e}")

# # Register the custom task plugin
# TaskPlugins.register_pythontask_plugin(Job, JobFunctionTask)