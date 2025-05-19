# Use an image with mamba/conda pre-installed
FROM mambaforge/mambaforge:latest

# Set working directory
WORKDIR /app

# Copy the entire application code into the container
# This includes your 'crossroad' module, setup.py, etc.
COPY . /app/

# Install Python and other dependencies using Mamba
# Dependencies are based on your recipe.yaml
RUN mamba install -y \
    -c jitendralab \
    -c bioconda \
    -c conda-forge \
    python=3.12 \
    numpy \
    fastapi \
    uvicorn \
    python-multipart \
    pandas \
    pydantic \
    requests \
    plotly \
    pyarrow \
    bioconda::seqkit \
    bioconda::seqtk \
    bioconda::bedtools \
    jitendralab::perf_ssr \
    jitendralab::plotly-upset-hd \
    rich \
    rich-argparse \
    typer \
    'rich-click>=1.7.0' \
    argcomplete \
    upsetplot \
    && mamba clean -afy

# Expose the port your FastAPI app will run on
EXPOSE 8000
ENV CROSSROAD_ROOT=/app

# Default command to start Uvicorn serving your FastAPI app
# This assumes your FastAPI app instance is named 'app' in 'crossroad/api/main.py'
CMD ["uvicorn", "crossroad.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
