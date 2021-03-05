import subprocess
import sys
import os


template_file = os.path.join(os.path.dirname(__file__), 'slurm_template.sh')


def parse_flags_dict(flag_dict):
    flags = '\n'.join([f"#SBATCH --{k.replace('_', '-')}={v}" for k, v in flag_dict.items()])
    return flags


def create_ray_slurm_script(
        job_name, command,
        base_log_dir,
        n_cpus_per_task,
        partition, time_in_mins,
        extra_flags,
        num_nodes=1, num_gpus=0,
        load_env='',
):
    output_file = os.path.join(base_log_dir, f'%j-%x-node-%n-task-%t.out')
    flags = dict(
        job_name=job_name,
        cpus_per_task=n_cpus_per_task,
        partition=partition,
        output=output_file,
        nodes=num_nodes,
        ntasks_per_node=1,
        time=time_in_mins,
        **extra_flags,
    )

    if num_gpus > 0:
        flags.update(
            gpu_info=f"gpu:{num_gpus}"
        )

    flags = parse_flags_dict(flags)

    with open(template_file, "r") as f:
        text = f.read()
    text = text.replace("${XX_FLAGS}", flags)

    text = text.replace("${XX_LOAD_ENV}", str(load_env))

    text = text.replace("${XX_COMMAND_PLACEHOLDER}", str(command))

    # ===== Save the script =====
    script_file = "{}.sh".format(job_name)
    script_file = os.path.join(base_log_dir, script_file)
    with open(script_file, "w") as f:
        f.write(text)

    # print('launching', script_file)
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
