import math


class SlurmConfig(object):
    """
    Wrap a command around a call to sbatch

    Usage:
    ```
    generator = SBatchGenerator()
    cmd = "echo 'Hello, world!'"
    sbatch_cmd = generator.wrap(cmd)
    """
    def __init__(
            self,
            account_name,
            partition,
            time_in_mins,
            max_num_cores_per_node,
            n_gpus=0,
            n_cpus_per_task=1,
    ):
        if n_gpus > 0 and n_cpus_per_task < 2:
            raise ValueError("Must have at least 2 cpus per GPU task")
        self.account_name = account_name
        self.partition = partition
        self.time_in_mins = time_in_mins
        self.n_nodes = 0
        self.n_tasks = 0
        self.n_gpus = n_gpus
        self.n_cpus_per_task = n_cpus_per_task
        self._max_num_cores_per_node = max_num_cores_per_node

    def add_job(self):
        self.n_tasks += 1
        num_cpus = self.n_tasks * self.n_cpus_per_task
        self.n_nodes = math.ceil(num_cpus / self._max_num_cores_per_node)


def wrap_command_with_sbatch(cmd, slurm_config):
    """
    Wrap a command around a call to sbatch

    Usage:
    ```
    config = SlurmConfig()
    cmd = "echo 'Hello, world!'"
    sbatch_cmd = wrap_command_with_sbatch(cmd, config)
    """
    cmd = cmd.replace("'", "\\'")
    if slurm_config.n_gpus > 0:
        full_cmd = (
            "sbatch -A {account_name} -p {partition} -t {time}"
            " -N {nodes} -n {n_tasks} --cpus-per-task={cpus_per_task}"
            " --gres=gpu:{n_gpus} --wrap=$'{cmd}'".format(
                account_name=slurm_config.account_name,
                partition=slurm_config.partition,
                time=slurm_config.time_in_mins,
                nodes=slurm_config.n_nodes,
                n_tasks=slurm_config.n_tasks,
                cpus_per_task=slurm_config.n_cpus_per_task,
                n_gpus=slurm_config.n_gpus,
                cmd=cmd,
            )
        )
    else:
        full_cmd = (
            "sbatch -A {account_name} -p {partition} -t {time}"
            " -N {nodes} -n {n_tasks} --cpus-per-task={cpus_per_task}"
            " --wrap=$'{cmd}'".format(
                account_name=slurm_config.account_name,
                partition=slurm_config.partition,
                time=slurm_config.time_in_mins,
                nodes=slurm_config.n_nodes,
                cpus_per_task=slurm_config.n_cpus_per_task,
                n_tasks=slurm_config.n_tasks,
                cmd=cmd,
            )
        )
    return full_cmd
