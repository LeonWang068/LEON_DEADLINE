import warnings
from retrying import retry

from batchcompute import (
    Client, ClientError, JsonError
)
from bcs_config import *


def is_retry_error(exception):
    if isinstance(exception, ClientError) and exception.get_status_code()/100 == 5:
        return True
    elif isinstance(exception, JsonError):
        return True
    else:
        return False

def get_bcs_client():
    Client.register_region(REGION, ENDPOINT)

    return Client(ENDPOINT, ACCESS_ID, ACCESS_KEY, SECURITY_TOKEN)

def nodes_in_cluster_group(cluster_id, group_name):
    client = get_bcs_client()

    @retry(stop_max_attempt_number=MAX_RETRY_COUNT,
        stop_max_delay=MAX_RETRY_TIME,
        retry_on_exception=is_retry_error,
        wait_random_min=WAIT_RANDOM_MIN,
        wait_random_max=WAIT_RANDOM_MAX)
    def list_wrapper(cluster_id, group_name, next_marker, max_item_count):
        return client.list_cluster_instances(cluster_id, group_name, next_marker, max_item_count)

    round = 1
    max_item_count = 100
    next_marker = ""
    while next_marker or round == 1:
        round += 1
        response = list_wrapper(cluster_id, group_name, next_marker, max_item_count)
        next_marker = response.NextMarker
        for item in response.Items:
                yield item

def get_node_from_ip(cluster_id, group_name, ipaddr):
    for item in nodes_in_cluster_group(cluster_id, group_name):
        if ipaddr == item.IpAddress:
            return item.Id
        else:
            continue
    else:
        return ""

def delete_node_by_ip(cluster_id, group_name, ipaddr):
    node_id = get_node_from_ip(cluster_id, group_name, ipaddr)
    if not node_id:
        warnings.warn("IP %s not found in group(%s) of cluster(%s)" % (ipaddr, group_name, cluster_id), RuntimeWarning)
        return

    client = get_bcs_client()

    @retry(stop_max_attempt_number=MAX_RETRY_COUNT,
        stop_max_delay=MAX_RETRY_TIME,
        retry_on_exception=is_retry_error,
        wait_random_min=WAIT_RANDOM_MIN,
        wait_random_max=WAIT_RANDOM_MAX)
    def delete_wrapper(cluster_id, group_name, node_id):
        try:
            print "haha"
            return client.delete_cluster_instance(cluster_id, group_name, node_id)
        except ClientError, e:
            if e.get_status_code() == 409:
                warnings.warn("IP %s in group(%s) of cluster(%s) have been already deleted" % (ipaddr, group_name, cluster_id), RuntimeWarning)
                return
            else:
                raise e

    delete_wrapper(cluster_id, group_name, node_id)


if __name__ == '__main__':
    cluster_id = "cls-6kip6824almskjp5smc00k"
    group_name = "group"
    ip = "10.28.151.20"
    print get_node_from_ip(cluster_id, group_name, ip)
    delete_node_by_ip(cluster_id, group_name, ip)
