import json
import logging
import subprocess
import textwrap
from pathlib import Path
from typing import Dict, Any

# Import centralized configuration
from crossroad import config

logger = logging.getLogger(__name__)

class SlurmManager:
    """
    Manages the creation and submission of Slurm jobs for the Crossroad pipeline.
    """

    def __init__(self, job_id: str, task_params: Dict[str, Any]):
        """
        Initializes the SlurmManager.

        Args:
            job_id: The unique identifier for the job.
            task_params: A dictionary of parameters for the analysis task.
        """
        self.job_id = job_id
        self.task_params = task_params
        self.job_dir = Path(task_params['job_dir']) # job_dir is already in task_params

    def _prepare_job_files(self) -> Path:
        """
        Prepares the necessary files for a Slurm job submission.

        - Creates a JSON file with all task parameters.
        - Creates the sbatch submission script.

        Returns:
            The path to the generated sbatch script.
        """
        # 1. Write parameters to a JSON file
        params_file_path = self.job_dir / f"{self.job_id}_params.json"
        # Convert Path objects and other non-serializable objects to strings
        serializable_params = self._make_params_serializable(self.task_params)
        
        with open(params_file_path, 'w') as f:
            json.dump(serializable_params, f, indent=4)
        logger.info(f"Task parameters for job {self.job_id} saved to {params_file_path}")

        # 2. Create the sbatch script
        sbatch_script_path = self.job_dir / f"{self.job_id}_submit.sh"
        script_content = self._generate_sbatch_script(params_file_path)
        
        with open(sbatch_script_path, 'w') as f:
            f.write(script_content)
        logger.info(f"sbatch script for job {self.job_id} created at {sbatch_script_path}")

        return sbatch_script_path

    def _generate_sbatch_script(self, params_file: Path) -> str:
        """Generates the content of the sbatch submission script."""
        slurm_log_path = self.job_dir / f"slurm_{self.job_id}.log"
        
        # Path to the slurm_runner.py script
        runner_script_path = config.ROOT_DIR / "crossroad" / "core" / "slurm_runner.py"

        # Cap the requested threads to the configured maximum
        requested_cpus = self.task_params["perf_params"].thread
        cpus_to_request = min(requested_cpus, config.SLURM_MAX_CPUS_PER_TASK)
        if requested_cpus > config.SLURM_MAX_CPUS_PER_TASK:
            logger.warning(
                f"Requested CPU count ({requested_cpus}) for job {self.job_id} "
                f"exceeds the configured maximum ({config.SLURM_MAX_CPUS_PER_TASK}). "
                f"Using {cpus_to_request} instead."
            )

        script = f"""\
        #!/bin/bash
        #SBATCH --job-name=crossroad_{self.job_id}
        #SBATCH --output={slurm_log_path}
        #SBATCH --error={slurm_log_path}
        #SBATCH --partition={config.SLURM_PARTITION}
        #SBATCH --time={config.SLURM_TIME_LIMIT}
        #SBATCH --nodes={config.SLURM_NODES}
        #SBATCH --ntasks-per-node={config.SLURM_NTASKS_PER_NODE}
        #SBATCH --cpus-per-task={cpus_to_request}
        {f"#SBATCH --mem={config.SLURM_MEMORY}" if config.SLURM_MEMORY else ""}
        {f"#SBATCH --qos={config.SLURM_QOS}" if config.SLURM_QOS else ""}
        {f"#SBATCH --account={config.SLURM_ACCOUNT}" if config.SLURM_ACCOUNT else ""}

        echo "Starting CrossRoad Slurm job..."
        echo "Job ID: {self.job_id}"
        echo "Host: $(hostname)"
        echo "Time: $(date)"

        # Determine the python executable
        # Use the specific python path if provided, otherwise fall back to conda activation
        PYTHON_CMD="{config.SLURM_PYTHON_PATH or 'python'}"
        if [ -z "{config.SLURM_PYTHON_PATH}" ] && [ -n "{config.SLURM_CONDA_ENV}" ]; then
            echo "Activating Conda environment: {config.SLURM_CONDA_ENV}"
            eval "$(conda shell.bash hook)"
            conda activate "{config.SLURM_CONDA_ENV}"
        elif [ -n "{config.SLURM_PYTHON_PATH}" ]; then
            echo "Using specific Python executable: {config.SLURM_PYTHON_PATH}"
        else
            echo "Using default 'python' command."
        fi

        # Set PYTHONPATH to include the project root
        export PYTHONPATH={config.ROOT_DIR}:$PYTHONPATH

        # Execute the runner script
        $PYTHON_CMD -u "{runner_script_path}" \\
            --job-id "{self.job_id}" \\
            --params-file "{params_file}" \\
            --root-dir "{config.ROOT_DIR}"

        echo "CrossRoad Slurm job finished."
        echo "Time: $(date)"
        """
        return textwrap.dedent(script)

    def submit(self) -> str:
        """
        Prepares files and submits the job to Slurm using sbatch.

        Returns:
            The Slurm job ID (as a string) if submission is successful.
        
        Raises:
            RuntimeError: If sbatch submission fails.
        """
        sbatch_script_path = self._prepare_job_files()

        try:
            logger.info(f"Submitting job {self.job_id} to Slurm...")
            result = subprocess.run(
                ["sbatch", str(sbatch_script_path)],
                capture_output=True,
                text=True,
                check=True
            )
            # sbatch output is typically "Submitted batch job <slurm_job_id>"
            slurm_job_id = result.stdout.strip().split()[-1]
            logger.info(f"Successfully submitted job {self.job_id} to Slurm. Slurm Job ID: {slurm_job_id}")
            return slurm_job_id
        except FileNotFoundError:
            logger.error("sbatch command not found. Is Slurm installed and in the system's PATH?")
            raise RuntimeError("sbatch command not found. Cannot submit job to Slurm.")
        except subprocess.CalledProcessError as e:
            logger.error(f"sbatch submission failed for job {self.job_id}. Return code: {e.returncode}")
            logger.error(f"sbatch stderr: {e.stderr}")
            raise RuntimeError(f"Slurm submission failed: {e.stderr}")

    @staticmethod
    def _make_params_serializable(params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Converts a dictionary of parameters to a JSON-serializable format.
        Handles Path objects, Pydantic models, and other non-serializable types.
        """
        serializable = {}
        for key, value in params.items():
            if isinstance(value, Path):
                serializable[key] = str(value)
            elif hasattr(value, 'model_dump'): # For Pydantic models
                serializable[key] = value.model_dump()
            elif key in ['logger', 'loop']: # Exclude non-serializable objects
                continue
            else:
                serializable[key] = value
        return serializable
