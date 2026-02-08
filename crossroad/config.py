import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from a .env file if it exists
load_dotenv()

# --- Project Root ---
# Assumes this file is located at /crossroad/config.py
ROOT_DIR = Path(__file__).resolve().parents[1]

# --- Job Output Directory ---
# The root directory for all job outputs.
# Can be overridden by the 'CROSSROAD_JOB_DIR' environment variable.
# Defaults to 'jobOut' inside the project's root directory.
JOB_OUTPUT_DIR = Path(os.getenv("CROSSROAD_JOB_DIR", ROOT_DIR / "jobOut"))

# --- API Execution Mode ---
# Determines how the API processes jobs.
# 'local': Use the internal asyncio background worker queue.
# 'slurm': Delegate job submission to a Slurm cluster.
# Can be overridden by the 'CROSSROAD_EXECUTION_MODE' environment variable.
EXECUTION_MODE = os.getenv("CROSSROAD_EXECUTION_MODE", "local").lower()
if EXECUTION_MODE not in ["local", "slurm"]:
    raise ValueError(f"Invalid EXECUTION_MODE: '{EXECUTION_MODE}'. Must be 'local' or 'slurm'.")

# --- Concurrency Settings ---
# The maximum number of jobs to run concurrently in 'local' mode.
# Can be overridden by the 'CROSSROAD_MAX_JOBS' environment variable.
try:
    MAX_CONCURRENT_JOBS = int(os.getenv("CROSSROAD_MAX_JOBS", "2"))
except ValueError:
    print("Warning: Invalid value for CROSSROAD_MAX_JOBS. Using default of 2.")
    MAX_CONCURRENT_JOBS = 2

# --- Slurm Configuration (for 'slurm' mode) ---
# Default Slurm partition to use for job submission.
# Can be overridden by the 'CROSSROAD_SLURM_PARTITION' environment variable.
SLURM_PARTITION = os.getenv("CROSSROAD_SLURM_PARTITION", "compute")

# Default time limit for Slurm jobs (e.g., "1-00:00:00" for 1 day).
# Can be overridden by the 'CROSSROAD_SLURM_TIME_LIMIT' environment variable.
SLURM_TIME_LIMIT = os.getenv("CROSSROAD_SLURM_TIME_LIMIT", "1-00:00:00")

# Maximum CPUs per task for Slurm jobs. This acts as a cap.
# Can be overridden by the 'CROSSROAD_SLURM_MAX_CPUS' environment variable.
try:
    SLURM_MAX_CPUS_PER_TASK = int(os.getenv("CROSSROAD_SLURM_MAX_CPUS", "40"))
except ValueError:
    print("Warning: Invalid value for CROSSROAD_SLURM_MAX_CPUS. Using default of 40.")
    SLURM_MAX_CPUS_PER_TASK = 40

# Default number of nodes for Slurm jobs.
# Can be overridden by the 'CROSSROAD_SLURM_NODES' environment variable.
try:
    SLURM_NODES = int(os.getenv("CROSSROAD_SLURM_NODES", "1"))
except ValueError:
    print("Warning: Invalid value for CROSSROAD_SLURM_NODES. Using default of 1.")
    SLURM_NODES = 1

# Default ntasks-per-node for Slurm jobs.
# Can be overridden by the 'CROSSROAD_SLURM_NTASKS_PER_NODE' environment variable.
try:
    SLURM_NTASKS_PER_NODE = int(os.getenv("CROSSROAD_SLURM_NTASKS_PER_NODE", "1"))
except ValueError:
    print("Warning: Invalid value for CROSSROAD_SLURM_NTASKS_PER_NODE. Using default of 1.")
    SLURM_NTASKS_PER_NODE = 1

# Name of the Conda environment to activate for Slurm jobs.
# If left empty or None, no conda activation command will be added.
# Can be overridden by the 'CROSSROAD_SLURM_CONDA_ENV' environment variable.
SLURM_CONDA_ENV = os.getenv("CROSSROAD_SLURM_CONDA_ENV", None)

# memory allocation for Slurm jobs (e.g., "300GB", "4G").
# If left empty or None, the --mem flag will not be added to the sbatch script.
# Can be overridden by the 'CROSSROAD_SLURM_MEMORY' environment variable.
SLURM_MEMORY = os.getenv("CROSSROAD_SLURM_MEMORY", None)

# Default QOS for Slurm jobs.
# If left empty or None, the --qos flag will not be added to the sbatch script.
# Can be overridden by the 'CROSSROAD_SLURM_QOS' environment variable.
SLURM_QOS = os.getenv("CROSSROAD_SLURM_QOS", None)

# Default Account for Slurm jobs.
# If left empty or None, the --account flag will not be added to the sbatch script.
# Can be overridden by the 'CROSSROAD_SLURM_ACCOUNT' environment variable.
SLURM_ACCOUNT = os.getenv("CROSSROAD_SLURM_ACCOUNT", None)

# Absolute path to the Python executable to be used in Slurm jobs.
# If provided, this will be used instead of relying on `conda activate`.
# e.g., /home/user/miniconda3/envs/myenv/bin/python
# Can be overridden by the 'CROSSROAD_SLURM_PYTHON_PATH' environment variable.
SLURM_PYTHON_PATH = os.getenv("CROSSROAD_SLURM_PYTHON_PATH", None)


# --- Ensure Directories Exist ---
def initialize_directories():
    """Creates necessary directories defined in the config."""
    try:
        JOB_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        print(f"Error creating job output directory at {JOB_OUTPUT_DIR}: {e}")
        # Depending on severity, you might want to exit or raise the exception
        raise

# You can call this on application startup, e.g., in the lifespan manager of the API
# or at the beginning of the CLI script.
# initialize_directories()
