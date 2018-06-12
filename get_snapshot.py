import requests
from requests.auth import HTTPDigestAuth
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

def main():
    """Summon the :party-wizard:"""
    config = yaml.safe_load(open("config.yml"))

    snapshot_id = get_snapshot_id(config['username'], config['password'],
                                  config['group_id'], config['cluster_name'])
    print(restore_jobs(config['username'], config['password'],
                                config['group_id'], config['cluster_name'],
                                snapshot_id))

if __name__ == '__main__':
    main()
