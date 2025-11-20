import subprocess
import re
import logging
import time
import numpy as np

from traffic_data.enums import City, Service
from traffic_data.utils import CityDimensions

logger = logging.getLogger('run_cluster_jobs')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)


def get_city_runtime_and_mem(c: City):
    # This function estimates the runtime and memory requirements for a job based on the city.
    # The values are based on the Paris city as a reference which was evaluated by testing on Paris and Facebook. 

    paris_time = 15800
    paris_mem = 18000
    paris_dims = CityDimensions.get_city_dim(city=City.PARIS)
    paris_dim = paris_dims[0] * paris_dims[1]

    city_dims = CityDimensions.get_city_dim(city=c)
    city_dim = city_dims[0] * city_dims[1]

    city_time_precise = paris_time * np.power(city_dim / paris_dim, 1.5)
    city_mem_precise = paris_mem * city_dim / paris_dim

    city_time = int(np.ceil(1.5 * city_time_precise / 60)) # in minutes
    city_time_str = convert_minutes(city_time)
    city_mem = int(np.ceil(1.5 * city_mem_precise)) # in MB

    return city_time_str, city_mem

def convert_minutes(minutes):
    total_seconds = int(minutes * 60)
    hrs = total_seconds // 3600
    mins = (total_seconds % 3600) // 60
    secs = total_seconds % 60
    return f"{hrs:02d}:{mins:02d}:{secs:02d}"

def run_cluster_jobs():
    ntasks = 1
    script_file_path = '/cluster/home/anmusso/Projects/NetMobV2/netmob/main.py'
    cities = [City.TOURS, City.NANCY]
    services = [s for s in Service]

    for c in cities:
        for s in services:
            logger.info(f"Submitting job for city {c.value} and service {s.value}")
            sbatch_output_file = f"slurm-aggr-{c.value.lower()}-{s.value.lower()}-%j.out"
            run_time, mem_per_cpu = get_city_runtime_and_mem(c=c)
            cmd_args = [c.value, s.value]
            cmd = f"sbatch --time={run_time} --mem-per-cpu={mem_per_cpu} --ntasks={ntasks} --output={sbatch_output_file} --error={sbatch_output_file} --wrap='python {script_file_path} {' '.join(cmd_args)}'"
            result = subprocess.run(cmd, shell=True, text=True, capture_output=True)
            job_id = re.search(r'\d+', result.stdout).group()
            logger.info(f"Job {job_id} submitted for city {c.value} and service {s.value}")
            time.sleep(1)


if __name__ == '__main__':
    run_cluster_jobs()