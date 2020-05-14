import os
import re
import sys
import ssl
import json
import shutil
import time
import yaml
import zipfile
import urllib3
import logging
import requests
import subprocess
import http.client
import pywren_ibm_cloud
from urllib.parse import urlparse
from kubernetes import client, config, watch
from pywren_ibm_cloud.utils import version_str
from pywren_ibm_cloud.version import __version__
from pywren_ibm_cloud.config import CACHE_DIR, load_yaml_config, dump_yaml_config
from . import config as cr_config

urllib3.disable_warnings()
logging.getLogger('kubernetes').setLevel(logging.CRITICAL)
logging.getLogger('urllib3.connectionpool').setLevel(logging.CRITICAL)

logger = logging.getLogger(__name__)


class CloudRunServingBackend:
    """
    A wrap-up around Cloud Run Serving APIs.
    """

    def __init__(self, cloudrun_config):
        self.log_level = os.getenv('PYWREN_LOGLEVEL')
        self.name = 'cloudrun'
        self.cloudrun_config = cloudrun_config
        self.region = self.cloudrun_config.get('region')
        self.namespace = self.cloudrun_config.get('namespace', 'default')
        self.cluster = self.cloudrun_config.get('cluster', 'default')
        self.workers = self.cloudrun_config.get('workers')

    def _format_service_name(self, runtime_name, runtime_memory):
        runtime_name = runtime_name.replace('/', '--').replace(':', '--')
        return '{}--{}mb'.format(runtime_name, runtime_memory)

    def _unformat_service_name(self, service_name):
        runtime_name, memory = service_name.rsplit('--', 1)
        image_name = runtime_name.replace('--', '/', 1)
        image_name = image_name.replace('--', ':', -1)
        return image_name, int(memory.replace('mb', ''))

    def _get_default_runtime_image_name(self):
        project_id = self.cloudrun_config['project_id']
        python_version = version_str(sys.version_info).replace('.', '')
        revision = 'latest' if 'SNAPSHOT' in __version__ else __version__.replace('.', '')
        return '{}/{}-v{}:{}'.format(project_id, cr_config.RUNTIME_NAME_DEFAULT, python_version, revision)

    def _get_service_host(self, service_name):
        """
        gets the service host needed for the invocation
        """
        logger.debug('Getting service host for: {}'.format(service_name))

        cmd = 'gcloud run services describe {} --platform=managed --region={} --format=json'.format(service_name, self.region)
        out = subprocess.check_output(cmd, shell=True).decode("ascii")
        service_host = json.loads(out)["status"]["url"][8:]

        logger.debug('Service host: {}'.format(service_host))
        return service_host

    def _build_default_runtime(self, default_runtime_img_name):
        """
        Builds the default runtime
        """
        location = 'https://raw.githubusercontent.com/tomwhite/pywren-ibm-cloud/master/runtime/cloudrun'
        python_version = version_str(sys.version_info).replace('.', '')
        resp = requests.get('{}/Dockerfile.python{}'.format(location, python_version))
        dockerfile = "Dockerfile"
        if resp.status_code == 200:
            with open(dockerfile, 'w') as f:
                f.write(resp.text)
            self.build_runtime(default_runtime_img_name, dockerfile)
            os.remove(dockerfile)
        else:
            msg = 'There was an error fetching the default runtime Dockerfile: {}'.format(resp.text)
            logger.error(msg)
            exit()

    def _create_service(self, docker_image_name, runtime_memory, timeout):

        service_name = self._format_service_name(docker_image_name, runtime_memory)
        
        # TODO: --no-allow-unauthenticated
        cmd = 'gcloud run deploy --allow-unauthenticated --platform=managed --region={} --image gcr.io/{} --max-instances={} --memory={} --timeout={} {}'.format(
            self.region, docker_image_name, self.workers, '{}Mi'.format(runtime_memory), timeout, service_name
        )

        if not self.log_level:
            cmd = cmd + " >/dev/null 2>&1"

        res = os.system(cmd)
        if res != 0:
            raise Exception('There was an error creating the service')

    def _generate_runtime_meta(self, docker_image_name, memory):
        """
        Extract installed Python modules from docker image
        """
        payload = {}

        payload['service_route'] = "/preinstalls"
        logger.debug("Extracting Python modules list from: {}".format(docker_image_name))
        try:
            runtime_meta = self.invoke(docker_image_name, memory, payload, return_result=True)
        except Exception as e:
            raise Exception("Unable to invoke 'modules' action: {}".format(e))

        if not runtime_meta or 'preinstalls' not in runtime_meta:
            raise Exception('Failed getting runtime metadata: {}'.format(runtime_meta))

        return runtime_meta

    def create_runtime(self, docker_image_name, memory, timeout=cr_config.RUNTIME_TIMEOUT_DEFAULT):

        default_runtime_img_name = self._get_default_runtime_image_name()
        if docker_image_name in ['default', default_runtime_img_name]:
            docker_image_name = default_runtime_img_name
            self._build_default_runtime(default_runtime_img_name)

        self._create_service(docker_image_name, memory, timeout)
        runtime_meta = self._generate_runtime_meta(docker_image_name, memory)

        return runtime_meta

    def _create_function_handler_zip(self):
        logger.debug("Creating function handler zip in {}".format(cr_config.FH_ZIP_LOCATION))

        def add_folder_to_zip(zip_file, full_dir_path, sub_dir=''):
            for file in os.listdir(full_dir_path):
                full_path = os.path.join(full_dir_path, file)
                if os.path.isfile(full_path):
                    zip_file.write(full_path, os.path.join('pywren_ibm_cloud', sub_dir, file))
                elif os.path.isdir(full_path) and '__pycache__' not in full_path:
                    add_folder_to_zip(zip_file, full_path, os.path.join(sub_dir, file))

        try:
            with zipfile.ZipFile(cr_config.FH_ZIP_LOCATION, 'w', zipfile.ZIP_DEFLATED) as ibmcf_pywren_zip:
                current_location = os.path.dirname(os.path.abspath(__file__))
                module_location = os.path.dirname(os.path.abspath(pywren_ibm_cloud.__file__))
                main_file = os.path.join(current_location, 'entry_point.py')
                ibmcf_pywren_zip.write(main_file, 'pywrenproxy.py')
                add_folder_to_zip(ibmcf_pywren_zip, module_location)
        except Exception as e:
            raise Exception('Unable to create the {} package: {}'.format(cr_config.FH_ZIP_LOCATION, e))

    def _delete_function_handler_zip(self):
        os.remove(cr_config.FH_ZIP_LOCATION)

    def build_runtime(self, docker_image_name, dockerfile):
        """
        Builds a new runtime from a Docker file and pushes it to the Docker hub
        """
        logger.info('Building a new docker image from Dockerfile')
        logger.info('Docker image name: {}'.format(docker_image_name))

        # Project ID can contain '-'
        expression = '^([-a-z0-9]+)/([-a-z0-9]+)(:[a-z0-9]+)?'
        result = re.match(expression, docker_image_name)

        if not result or result.group() != docker_image_name:
            raise Exception("Invalid docker image name: '.' or '_' characters are not allowed")

        self._create_function_handler_zip()

        # Dockerfile has to be called "Dockerfile" (and in cwd) for 'gcloud builds submit' to work
        if dockerfile != "Dockerfile":
            shutil.copyfile(dockerfile, "Dockerfile")
        cmd = 'gcloud builds submit -t gcr.io/{}'.format(docker_image_name)

        if not self.log_level:
            cmd = cmd + " >/dev/null 2>&1"

        res = os.system(cmd)
        if res != 0:
            raise Exception('There was an error building the runtime')

        self._delete_function_handler_zip()

    def delete_runtime(self, docker_image_name, memory):
        service_name = self._format_service_name(docker_image_name, memory)
        logger.info('Deleting runtime: {}'.format(service_name))

        cmd = 'gcloud run services delete {} --platform=managed --region={} --quiet'.format(service_name, self.region)

        if not self.log_level:
            cmd = cmd + " >/dev/null 2>&1"

        res = os.system(cmd)
        if res != 0:
            raise Exception('There was an error deleting the runtime')

    def delete_all_runtimes(self):
        """
        Deletes all runtimes deployed
        """
        runtimes = self.list_runtimes()
        for docker_image_name, memory in runtimes:
            self.delete_runtime(docker_image_name, memory)

    def list_runtimes(self, docker_image_name='all'):
        """
        List all the runtimes deployed
        return: list of tuples [docker_image_name, memory]
        """
        runtimes = []

        cmd = 'gcloud run services list --platform=managed --region={} --format=json'.format(self.region)
        out = subprocess.check_output(cmd, shell=True).decode("ascii")
        json_out = json.loads(out)
        for service in json_out:
            runtime_name = service['metadata']['name']
            if '--' not in runtime_name:
                continue
            image_name, memory = self._unformat_service_name(runtime_name)
            if docker_image_name == image_name or docker_image_name == 'all':
                runtimes.append((image_name, memory))

        return runtimes

    def invoke(self, docker_image_name, memory, payload, return_result=False):
        """
        Invoke -- return information about this invocation
        """
        service_name = self._format_service_name(docker_image_name, memory)
        service_host = self._get_service_host(service_name)

        headers = {}

        endpoint = 'https://{}'.format(service_host)

        exec_id = payload.get('executor_id')
        call_id = payload.get('call_id')
        job_id = payload.get('job_id')
        route = payload.get("service_route", '/')

        try:
            parsed_url = urlparse(endpoint)

            if endpoint.startswith('https'):
                ctx = ssl._create_unverified_context()
                conn = http.client.HTTPSConnection(parsed_url.netloc, context=ctx)
            else:
                conn = http.client.HTTPConnection(parsed_url.netloc)

            conn.request("POST", route, body=json.dumps(payload), headers=headers)

            if exec_id and job_id and call_id:
                logger.debug('ExecutorID {} | JobID {} - Function call {} invoked'
                             .format(exec_id, job_id, call_id))
            elif exec_id and job_id:
                logger.debug('ExecutorID {} | JobID {} - Function invoked'
                             .format(exec_id, job_id))
            else:
                logger.debug('Function invoked')

            resp = conn.getresponse()
            resp_status = resp.status
            resp_data = resp.read().decode("utf-8")
            conn.close()
        except Exception as e:
            raise e

        if resp_status in [200, 202]:
            data = json.loads(resp_data)
            if return_result:
                return data
            return data["activationId"]
        elif resp_status == 404:
            raise Exception("PyWren runtime is not deployed in your k8s cluster")
        else:
            logger.debug('ExecutorID {} | JobID {} - Function call {} failed ({}). Retrying request'
                         .format(exec_id, job_id, call_id, resp_data.replace('.', '')))

    def get_runtime_key(self, docker_image_name, runtime_memory):
        """
        Method that creates and returns the runtime key.
        Runtime keys are used to uniquely identify runtimes within the storage,
        in order to know which runtimes are installed and which not.
        """
        service_name = self._format_service_name(docker_image_name, runtime_memory)
        runtime_key = os.path.join(self.cluster, self.namespace, service_name)

        return runtime_key
