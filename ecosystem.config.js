module.exports = {
  apps: [{
    name: "crossroad-api",
    script: "/home/jnarayan/miniforge3/envs/cr/bin/python",
    args: "-m crossroad.cli.main --api", // Arguments passed to the interpreter
    cwd: "/home/jnarayan/Workspace_PRANJAL/crossroadnew/crossroad", // The working directory
    env_local: {
      "CROSSROAD_EXECUTION_MODE": "local",
      "CROSSROAD_JOB_DIR": "/home/jnarayan/Workspace_PRANJAL/crossroadnew/crossroad/jobOut"
    },
    env_slurm: {
      "CROSSROAD_EXECUTION_MODE": "slurm",
      "CROSSROAD_JOB_DIR": "/home/jnarayan/Workspace_PRANJAL/crossroadnew/crossroad/jobOut",
      "CROSSROAD_SLURM_CONDA_ENV": "cr",
      "CROSSROAD_SLURM_MEMORY": "300GB",
      "CROSSROAD_SLURM_PYTHON_PATH": "/home/jnarayan/miniforge3/envs/cr/bin/python",
      "CROSSROAD_SLURM_QOS": "common",
      "CROSSROAD_SLURM_ACCOUNT": "common",
      "CROSSROAD_SLURM_NODES": 1,
      "CROSSROAD_SLURM_NTASKS_PER_NODE": 1
    },
    watch_options: {
      "ignore_watch": ["jobOut"]
    }
  }]
}
