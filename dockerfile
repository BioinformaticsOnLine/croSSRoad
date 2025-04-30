# Use an image with mamba/conda pre-installed
FROM mambaforge/mambaforge:latest

# Set working dir
WORKDIR /app

# Install your crossroad tool into the base env
# (uses the same channels as your conda command)
RUN mamba install -y \
      -c jitendralab \
      -c bioconda \
      -c conda-forge \
      crossroad

# Copy your code (if you also need the repo in the container)
# COPY . .

# Expose the port your FastAPI app listens on
EXPOSE 8000

# Default command: start Uvicorn serving your FastAPI app
# adjust module path if different
CMD ["uvicorn", "crossroad.api.main:app", "--host", "0.0.0.0", "--port", "8000"]