import unittest
from unittest.mock import patch, MagicMock
from kubernetes import client as k8s_client
from flytekit.exceptions import user as _user_exceptions

# Import the Job and JobFunctionTask classes
from flytekitplugins.job.task import Job, JobFunctionTask

# Define a dummy task function at the module level
def dummy_task_function():
    pass

class TestJobFunctionTask(unittest.TestCase):

    @patch('kubernetes.client.BatchV1Api.create_namespaced_job')
    def test_job_function_task(self, mock_create_job):
        # Mock the Kubernetes Job spec
        job_spec = k8s_client.V1Job(
            metadata=k8s_client.V1ObjectMeta(name="test-job"),
            spec=k8s_client.V1JobSpec(
                template=k8s_client.V1PodTemplateSpec(
                    spec=k8s_client.V1PodSpec(
                        containers=[k8s_client.V1Container(name="test-container", image="busybox")],
                        restart_policy="Never"
                    )
                )
            )
        )

        # Create the Job configuration
        job_config = Job(job_spec=job_spec)

        # Create an instance of JobFunctionTask
        job_task = JobFunctionTask(task_config=job_config, task_function=dummy_task_function)

        # Execute the task
        job_task.execute()

        # Verify that the Kubernetes API call was made
        mock_create_job.assert_called_once_with(
            body=job_spec,
            namespace="default"
        )

if __name__ == '__main__':
    unittest.main()
# import unittest
# from unittest.mock import patch, MagicMock
# from kubernetes import client as k8s_client
# from flytekit.testing import task_mock
# from flytekit.exceptions import user as _user_exceptions

# # Import the Job and JobFunctionTask classes
# from flytekitplugins.job.task import Job, JobFunctionTask

# # Define a dummy task function at the module level
# def dummy_task_function():
#     pass

# class TestJobFunctionTask(unittest.TestCase):

#     @patch('kubernetes.client.BatchV1Api.create_namespaced_job')
#     def test_job_function_task(self, mock_create_job):
#         # Mock the Kubernetes Job spec
#         job_spec = k8s_client.V1Job(
#             metadata=k8s_client.V1ObjectMeta(name="test-job"),
#             spec=k8s_client.V1JobSpec(
#                 template=k8s_client.V1PodTemplateSpec(
#                     spec=k8s_client.V1PodSpec(
#                         containers=[k8s_client.V1Container(name="test-container", image="busybox")],
#                         restart_policy="Never"
#                     )
#                 )
#             )
#         )

#         # Create the Job configuration
#         job_config = Job(job_spec=job_spec)

#         # Create an instance of JobFunctionTask
#         job_task = JobFunctionTask(task_config=job_config, task_function=dummy_task_function)

#         # Mock the task execution
#         with task_mock(job_task) as mock:
#             mock.return_value = None
#             job_task.execute()

#         # Verify that the Kubernetes API call was made
#         mock_create_job.assert_called_once_with(
#             body=job_spec,
#             namespace="default"
#         )

# if __name__ == '__main__':
#     unittest.main()

# import unittest
# from unittest.mock import patch, MagicMock
# from kubernetes import client as k8s_client
# from flytekit.testing import task_mock
# from flytekit.exceptions import user as _user_exceptions

# # Import the Job and JobFunctionTask classes
# from flytekitplugins.job.task import Job, JobFunctionTask

# class TestJobFunctionTask(unittest.TestCase):

#     @patch('kubernetes.client.BatchV1Api.create_namespaced_job')
#     def test_job_function_task(self, mock_create_job):
#         # Mock the Kubernetes Job spec
#         job_spec = k8s_client.V1Job(
#             metadata=k8s_client.V1ObjectMeta(name="test-job"),
#             spec=k8s_client.V1JobSpec(
#                 template=k8s_client.V1PodTemplateSpec(
#                     spec=k8s_client.V1PodSpec(
#                         containers=[k8s_client.V1Container(name="test-container", image="busybox")],
#                         restart_policy="Never"
#                     )
#                 )
#             )
#         )

#         # Create the Job configuration
#         job_config = Job(job_spec=job_spec)

#         # Define a dummy task function
#         def dummy_task_function():
#             pass

#         # Create an instance of JobFunctionTask
#         job_task = JobFunctionTask(task_config=job_config, task_function=dummy_task_function)

#         # Mock the task execution
#         with task_mock(job_task) as mock:
#             mock.return_value = None
#             job_task.execute()

#         # Verify that the Kubernetes API call was made
#         mock_create_job.assert_called_once_with(
#             body=job_spec,
#             namespace="default"
#         )

# if __name__ == '__main__':
#     unittest.main()