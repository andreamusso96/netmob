import os
import pandas as pd


def check_cluster_jobs():
    dir_path = '/Users/andrea/Desktop/Temp/job_netmob_2025_11_11/'
    files = [f for f in os.listdir(dir_path) if f.endswith('.out')]
    job_names_not_finished = []
    for file in files:
        job_name = file.split('.')[0]
        finished = False
        with open(os.path.join(dir_path, file), 'r') as f:
            for line in f:
                if '@@ Zonal statistics job finished @@' in line:
                    finished = True
                    break
        if not finished:
            city_name = job_name.split('-')[2]
            service_name = job_name.split('-')[3]
            job_names_not_finished.append({
                'job_name': job_name,
                'city': city_name,
                'service': service_name
            })

    job_names_not_finished = pd.DataFrame(job_names_not_finished)
    print(job_names_not_finished.head())
    print('Number of jobs not finished:', len(job_names_not_finished))
    print('Number of cities not finished:', len(job_names_not_finished['city'].unique()))
    print('Number of services not finished:', len(job_names_not_finished['service'].unique()))
    print('Cities not finished:', job_names_not_finished['city'].unique())
    print('Services not finished:', job_names_not_finished['service'].unique())
                


if __name__ == '__main__':
    check_cluster_jobs()