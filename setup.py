from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="crossroad-cli",
    version="0.1.3",
    packages=["crossroad", "crossroad.cli", "crossroad.api", "crossroad.core"],  # Explicitly list packages
    package_dir={"": "."},  # Root directory for packages
    package_data={
        'crossroad': ['**/*.py'],  # Include all Python files
    },
    include_package_data=True,
    install_requires=[
        "fastapi",
        "uvicorn",
        "python-multipart",
        "pandas",
        "pydantic",
        "requests"
    ],
    entry_points={
        "console_scripts": [
            "crossroad=crossroad.cli.main:main",
        ],
    },
    author="Pranjal Pruthi, Preeti Agarwal",
    author_email="your.email@igib.res.in",
    description="A tool for analyzing SSRs in genomic data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/pranjalpruthi/crossroad",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)