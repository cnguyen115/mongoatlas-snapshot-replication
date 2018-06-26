import datetime
import os
import requests
from requests.auth import HTTPDigestAuth
import shutil
import subprocess
import tarfile
import yaml

class MongoAPI:
    def __init__(self, username, password, group_id, cluster_name):
        self.username = username
        self.password = password
        self.base_url = ("https://cloud.mongodb.com/api/atlas/v1.0/groups/"
                         "{}/clusters/{}").format(group_id, cluster_name)

    def get_snapshot_id(self):
        """Return snapshot id calling Mongo Atlas API: Get All Snapshots"""
        print("Getting snapshot id...")
        url = "{}/snapshots?itemsPerPage=1".format(self.base_url)

        r = requests.get(url, auth=HTTPDigestAuth(self.username,
                                                  self.password))
        r.raise_for_status()

        # Get snapshot id from response
        snapshot_id = r.json()['results'][0]['id']
        print("Snapshot ID: {}".format(snapshot_id))
        return snapshot_id

    def restore_jobs(self, snapshot_id):
        """Return a link for the snapshot calling Mongo Atlas API: Restore Jobs"""
        print("Initialize download url via restore jobs...")
        payload = {
          "delivery" : {
            "methodName" : "HTTP"
          },
          "snapshotId" : snapshot_id
        }
        url = "{}/restoreJobs".format(self.base_url)

        r = requests.post(url, json=payload,
                          auth=HTTPDigestAuth(self.username,
                                              self.password))
        r.raise_for_status()

        return r.json()['results'][0]['delivery']['url']

def download_file(url, filename):
    """Download file from specified url onto current pwd"""
    print("Starting Download...\nDownload URL: {}".format(url))
    with requests.get(url, stream=True) as r, open(filename, 'wb') as f:
        shutil.copyfileobj(r.raw, f)

    print("Download Completed.")
    return filename

def tar_extract(filename, filepath):
    """Nuke directory and extract tar into it"""
    print("Extracting {}...".format(filename))
    if os.path.exists(filepath):
        shutil.rmtree(filepath)
        os.makedirs(filepath)

    with tarfile.open(filename) as t:
        t.extractall(path=filepath)

    dir_path = '{filepath}/{dir}'.format(filepath=filepath,
                                         dir=os.listdir(path=filepath)[0])

    # Delete tar file to save space
    os.remove(filename)
    print("Extraction completed.")
    return dir_path

def stop_mongod_service():
    """Stop mongod service"""
    print("Stopping mongod service...")
    return subprocess.run(['systemctl', 'stop', 'mongod'])

def restore_mongo_file_system(snapshot_path, path_to_mongodb):
    """Copy mongo snapshot into mongodb"""
    print("Restoring mongod file system...")
    if os.path.exists(path_to_mongodb):
        shutil.rmtree(path_to_mongodb)

    shutil.copytree(snapshot_path, path_to_mongodb)

def change_permissions(user, group, path):
    """Change mongodb file/database owner to mongodb"""
    print("Fixing permissions...")
    return subprocess.run(['chown', '-R',
                           '{}:{}'.format(user, group),
                           path])

def start_mongod_service():
    """Start mongod service"""
    print("Starting mongod service...")
    return subprocess.run(['systemctl', 'start', 'mongod'])

def main():
    """Summon the :party-wizard:"""
    config = yaml.safe_load(open('config.yml'))
    path_to_mongodb = '/var/lib/mongodb'

    print("{datetime}: Starting script...".format(
        datetime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    mongo_api = MongoAPI(config['username'], config['password'],
                         config['group_id'], config['cluster_name'])
    snapshot_id = mongo_api.get_snapshot_id()
    download_url = mongo_api.restore_jobs(snapshot_id)
    filename = download_file(download_url, 'mongodb.tar.gz')
    directory = tar_extract(filename, 'mongodb')
    stop_mongod_service()
    restore_mongo_file_system(directory, path_to_mongodb)
    change_permissions('mongodb', 'mongodb', path_to_mongodb)
    start_mongod_service()
    print("{datetime}: Script Completed".format(
        datetime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

if __name__ == '__main__':
    main()
