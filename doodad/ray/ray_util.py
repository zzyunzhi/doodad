import subprocess
import sys
import os


template_file = os.path.join(os.path.dirname(__file__), 'slurm_template.sh')


def create_ray_slurm_script(
        job_name, command,
        base_log_dir,
        n_cpus_per_task,
        partition, time_in_mins,
        num_nodes=1, num_gpus=0,
        load_env='',
        extra_flags='',
):
    # job_name = "{}_{}".format(exp_name,
    #                           time.strftime("%m%d-%H%M", time.localtime()))

    # ===== Modified the template script =====
    with open(template_file, "r") as f:
        text = f.read()
    text = text.replace('${JOB_NAME}', str(job_name))
    text = text.replace('${CPUS_PER_TASKS}', str(n_cpus_per_task))
    text = text.replace('${PARTITION}', str(partition))

    output_file = os.path.join(base_log_dir, f'%j-%x-node-%n-task-%t.out')
    text = text.replace("${OUTPUT}", output_file)

    text = text.replace("${NUM_NODES}", str(num_nodes))
    text = text.replace("${TIME_IN_MINS}", str(time_in_mins))

    if num_nodes > 1:
        node_info = f"#SBATCH --exclude=iris-hp-z8"
    else:
        node_info = ""
    text = text.replace("${NODE_INFO}", node_info)

    if num_gpus > 0:
        gpu_info = f"#SBATCH --gres=gpu:{num_gpus}"
    else:
        gpu_info = ""
    text = text.replace("${GPU_INFO}", gpu_info)

    if extra_flags:
        extra_flags = f"#SBATCH {extra_flags}"
    else:
        extra_flags = ""
    text = text.replace("${EXTRA_FLAGS}", extra_flags)

    text = text.replace("${LOAD_ENV}", str(load_env))

    text = text.replace("${RAY_TEMP_DIR}", '/iris/u/yzzhang/tmp/ray/')
    text = text.replace("${COMMAND_PLACEHOLDER}", str(command))

    # ===== Save the script =====
    script_file = "{}.sh".format(job_name)
    script_file = os.path.join(base_log_dir, script_file)
    with open(script_file, "w") as f:
        f.write(text)

    print('launching', script_file)
    # print('outputs will be saved to:')
    # print(output_file)

    return script_file

    # ===== Submit the job =====
    print("Starting to submit job!")
    subprocess.Popen(["sbatch", script_file])
    print(
        "Job submitted! Script file is at: <{}>. Log file is at: <{}>".format(
            script_file, "{}.log".format(job_name)))
    sys.exit(0)
