# Use a minimal base image with Ubuntu
FROM ubuntu:22.04

# Install basic dependencies and curl for micromamba
RUN apt-get update && apt-get install -y \
    curl \
    bzip2 \
    && rm -rf /var/lib/apt/lists/*

# Install micromamba
RUN curl -Ls https://micro.mamba.pm/api/micromamba/linux-64/latest | tar -xvj -C /usr/local/bin/ bin/micromamba

# Set up micromamba environment
ENV MAMBA_ROOT_PREFIX=/opt/conda
RUN micromamba shell init -s bash -p /opt/conda

# Create a conda environment and install crossroad
RUN micromamba create -n crossroad_env python=3.12 -y && \
    micromamba run -n crossroad_env micromamba install -c jitendralab -c bioconda -c conda-forge crossroad -y

# Set working directory
WORKDIR /app

# Copy application code
COPY . /app

# Create and set permissions for jobOut directory
RUN mkdir -p /app/jobOut && \
    chmod -R 777 /app/jobOut

# Expose port for FastAPI
EXPOSE 8000

# Set environment to activate conda
ENV PATH=/opt/conda/envs/crossroad_env/bin:$PATH

# Command to run the FastAPI application
CMD ["micromamba", "run", "-n", "crossroad_env", "uvicorn", "crossroad.api.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]