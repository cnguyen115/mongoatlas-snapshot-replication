
import os
import requests
from requests.auth import HTTPDigestAuth
import shutil
import subprocess
import tarfile
import yaml

def get_snapshot_id(username, password, group_id, cluster_name):
    """Return snapshot id calling Mongo Atlas API: Get All Snapshots"""
    url = ("https://cloud.mongodb.com/api/atlas/v1.0/groups/{}/clusters/{}/"
           "snapshots?itemsPerPage=1".format(group_id, cluster_name))

    r = requests.get(url, auth=HTTPDigestAuth(username, password))
    r.raise_for_status()

    # Get snapshot id from response
    return r.json()['results'][0]['id']

def restore_jobs(username, password, group_id, cluster_name, snapshot_id):
    """Return a link for the snapshot calling Mongo Atlas API: Restore Jobs"""
    payload = {
      "delivery" : {
        "methodName" : "HTTP"
      },
      "snapshotId" : snapshot_id
    }
    url = ("https://cloud.mongodb.com/api/atlas/v1.0/groups/{}/clusters/{}/"
           "restoreJobs".format(group_id, cluster_name))

    r = requests.post(url, json=payload,
                      auth=HTTPDigestAuth(username, password))
    r.raise_for_status()

    return r.json()['results'][0]['delivery']['url']

def download_file(url):
    """Download file from specified url onto current pwd"""
    local_filename = 'mongodb.tar.gz'
    with requests.get(url, stream=True) as r, open(local_filename, 'wb') as f:
        shutil.copyfileobj(r.raw, f)

    return local_filename

def tar_extract(filename, filepath):
    """Nuke directory and extract tar into it"""
    if os.path.exists(filepath):
        shutil.rmtree(filepath)
        os.makedirs(filepath)

    with tarfile.open(filename) as t:
        t.extractall(path=filepath)

    dir_path = '{filepath}/{dir}'.format(filepath=filepath,
                                         dir=os.listdir(path=filepath)[0])

    # Delete tar file to save space
    os.remove(filename)

    return dir_path

def stop_mongod_service():
    """Stop mongod service"""
    return subprocess.run(['systemctl', 'stop', 'mongod'])

def restore_mongo_file_system(snapshot_path, path_to_mongodb):
    """Copy mongo snapshot into mongodb"""
    if os.path.exists(path_to_mongodb):
        shutil.rmtree(path_to_mongodb)

    shutil.copytree(snapshot_path, path_to_mongodb)

def change_permissions(user, group, path):
    """Change mongodb file/database owner to mongodb"""
    for root, dirs, files in os.walk(path):
        for dir in dirs:
            shutil.chown(os.path.join(root, dir), user=user, group=group)
        for file in files:
            shutil.chown(os.path.join(root, file), user=user, group=group)

def start_mongod_service():
    """Start mongod service"""
    return subprocess.run(['systemctl', 'start', 'mongod'])

def main():
    """Summon the :party-wizard:"""
    config = yaml.safe_load(open('config.yml'))
    path_to_mongodb = '/var/lib/mongodb'

    print("Getting snapshot id...")
    snapshot_id = get_snapshot_id(config['username'], config['password'],
                                  config['group_id'], config['cluster_name'])
    print("Snapshot ID: {}".format(snapshot_id))
    download_url = restore_jobs(config['username'], config['password'],
                                config['group_id'], config['cluster_name'],
                                snapshot_id)
    print("Starting Download...\nDownload URL: {}".format(download_url))
    filename = download_file(download_url)
    print("Download Completed. Extracting {}...".format(filename))
    directory = tar_extract(filename, 'mongodb')
    print("Extraction completed.")
    print("Stopping mongod service...")
    stop_mongod_service()
    print("Restoring mongod file system...")
    restore_mongo_file_system(directory, path_to_mongodb)
    print("Fixing permissions...")
    change_permissions('mongodb', 'nogroup', path_to_mongodb)
    print("Starting mongod service...")
    start_mongod_service()
    print("Script completed.")

if __name__ == '__main__':
    main()
