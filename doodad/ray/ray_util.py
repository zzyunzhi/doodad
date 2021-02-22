import subprocess
import sys
import time
import os


template_file = os.path.join(os.path.dirname(__file__), 'slurm_template.sh')
JOB_NAME = "${JOB_NAME}"
NUM_NODES = "${NUM_NODES}"
NUM_GPUS_PER_NODE = "${NUM_GPUS_PER_NODE}"
PARTITION_OPTION = "${PARTITION_OPTION}"
COMMAND_PLACEHOLDER = "${COMMAND_PLACEHOLDER}"
GIVEN_NODE = "${GIVEN_NODE}"
LOAD_ENV = "${LOAD_ENV}"


def create_ray_slurm_script(
        exp_name, command,
        num_nodes=1, node='', num_gpus=0,
        partition='', load_env='',
):
    if node:
        # assert args.num_nodes == 1
        node_info = "#SBATCH -w {}".format(node)
    else:
        node_info = ""

    job_name = "{}_{}".format(exp_name,
                              time.strftime("%m%d-%H%M", time.localtime()))

    partition_option = "#SBATCH --partition={}".format(
        partition) if partition else ""

    # ===== Modified the template script =====
    with open(template_file, "r") as f:
        text = f.read()
    text = text.replace(JOB_NAME, job_name)
    text = text.replace(NUM_NODES, str(num_nodes))
    text = text.replace(NUM_GPUS_PER_NODE, str(num_gpus))
    text = text.replace(PARTITION_OPTION, partition_option)
    text = text.replace(COMMAND_PLACEHOLDER, str(command))
    text = text.replace(LOAD_ENV, str(load_env))
    text = text.replace(GIVEN_NODE, node_info)
    text = text.replace(
        "# THIS FILE IS A TEMPLATE AND IT SHOULD NOT BE DEPLOYED TO "
        "PRODUCTION!",
        "# THIS FILE IS MODIFIED AUTOMATICALLY FROM TEMPLATE AND SHOULD BE "
        "RUNNABLE!")

    # ===== Save the script =====
    script_file = "{}.sh".format(job_name)
    with open(script_file, "w") as f:
        f.write(text)

    return script_file

    # ===== Submit the job =====
    print("Starting to submit job!")
    subprocess.Popen(["sbatch", script_file])
    print(
        "Job submitted! Script file is at: <{}>. Log file is at: <{}>".format(
            script_file, "{}.log".format(job_name)))
    sys.exit(0)
