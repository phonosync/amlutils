from azureml.core import Workspace, ComputeTarget
from azureml.core.compute import AmlCompute
from azureml.core.authentication import InteractiveLoginAuthentication


def get_ws(tenant=None):
    """
    :param tenant: optional, your tenant id. Only needed when you work on different tenants
    See https://github.com/Azure/MachineLearningNotebooks/issues/131
    :return: azureml Workspace instance
    """
    if tenant:
        auth = InteractiveLoginAuthentication(tenant_id = tenant)
        ws = Workspace.from_config(auth = auth)
    else:
        ws = Workspace.from_config()
    return ws


def supported_vm_sizes(ws):
    """
    Get vm sizes available for your region
    :param ws: azureml Workspace instance
    :return: list
    """
    return [size for size in AmlCompute.supported_vmsizes(workspace=ws)]


def compute_targets(ws):
    """
    Get list of compute targets in workspace
    :param ws: azureml Workspace instance
    :return: list of tuples with target name, vm size and state
    """
    return [(name, ct.vm_size, ct.provisioning_state) for name, ct in ws.compute_targets.items()]


def delete_compute_target_by_name(ws, name):
    """
    Delete a compute target
    :param ws: azureml Workspace instance
    :param name: String commpute target name
    :return:
    """
    ws.compute_targets[name].delete()


def prepare_remote_compute(ws, compute_name, compute_min_nodes=0, compute_max_nodes=4, compute_vm_size='STANDARD_D2_V2'):
    """
    :param ws: azureml Workspace instance
    :param compute_name: String with name for compute target
    :param compute_min_nodes: minimum number of nodes
    :param compute_max_nodes: maximum number of nodes
    :param compute_vm_size: vm size for compute target
    :return:
    """

    if compute_name in ws.compute_targets:
        compute_target = ws.compute_targets[compute_name]
        if compute_target and type(compute_target) is AmlCompute:
            print('Found compute target: ' + compute_name + ' of size: ' + compute_target.vm_size + '. Using it. ')
            print('For a different size create a new target with different name!')
            # TODO: Handle case if compute_name exists, but is not active!
    else:
        print('creating a new compute target...')
        provisioning_config = AmlCompute.provisioning_configuration(vm_size = compute_vm_size,
                                                                    min_nodes = compute_min_nodes,
                                                                    max_nodes = compute_max_nodes)
        # create the cluster
        compute_target = ComputeTarget.create(ws, compute_name, provisioning_config)

        # can poll for a minimum number of nodes and for a specific timeout.
        # if no min node count is provided it will use the scale settings for the cluster
        compute_target.wait_for_completion(show_output=True, min_node_count=None, timeout_in_minutes=20)

        # For a more detailed view of current AmlCompute status, use get_status()
        print(compute_target.get_status().serialize())

    return compute_target