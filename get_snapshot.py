import grp
import os
import pwd
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

    return filepath

def stop_mongod_service():
    """Stop mongod service"""
    subprocess.run(["systemctl"], ["stop"], ["mongod"])

def restore_mongo_file_system(snapshot_path, path_to_mongodb):
    """Make a backup of current mongodb file/database and restore snapshot"""
    if os.path.exists(path_to_mongodb):
        if os.path.exists('{path}_backup'.format(path=path_to_mongodb)):
            shutil.rmtree('{path}_backup'.format(path=path_to_mongodb))
        shutil.move(path_to_mongodb,
                    '{path}_backup'.format(path=path_to_mongodb))

    shutil.move(snapshot_path, path_to_mongodb)

def change_permissions(user, group, path):
    """Change mongodb file/database owner to mongodb"""
    uid = pwd.getpwnam(user).pw_uid
    gid = grp.getgrnam(group).gr_gid

    os.chown(path, uid, gid)

def start_mongod_service():
    """Start mongod service"""
    subprocess.run(["systemctl"], ["start"], ["mongod"])

def main():
    """Summon the :party-wizard:"""
    config = yaml.safe_load(open('config.yml'))

    snapshot_id = get_snapshot_id(config['username'], config['password'],
                                  config['group_id'], config['cluster_name'])
    download_url = restore_jobs(config['username'], config['password'],
                                config['group_id'], config['cluster_name'],
                                snapshot_id)
    filename = download_file(download_url)
    directory = tar_extract(filename, 'mongodb')
    stop_mongod_service()
    path_to_mongodb = restore_mongo_file_system(directory, 'var/lib/mongodb')
    change_permission('mongodb', 'mongodb', path_to_mongodb)
    start_mongod_service()

if __name__ == '__main__':
    main()
