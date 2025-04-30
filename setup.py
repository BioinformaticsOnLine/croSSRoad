from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="crossroad-cli",
    version="0.2.2",
    packages=["crossroad", "crossroad.cli", "crossroad.api", "crossroad.core"],
    package_dir={"": "."},
    package_data={
        'crossroad': ['**/*.py'],
    },
    include_package_data=True,
    license="MIT",  # Only one license parameter here
    install_requires=[
        "fastapi",
        "uvicorn",
        "python-multipart",
        "pandas",
        "pydantic",
        "requests",
        "perf_ssr",
        "plotly>=5.18.0",
        "plotly-upset-hd>=0.0.2",
        "typer[all]>=0.9.0",      # Typer CLI framework
        "rich-click>=1.3.0",      # Rich-enhanced Click help
        "argcomplete>=3.1.1",     # Shell tab-completion
    ],
    entry_points={  # updated to use Typer app entry point
        "console_scripts": [
            "crossroad=crossroad.cli.main:app",
        ],
    },
    author="Pranjal Pruthi, Preeti Agarwal",
    author_email="your.email@igib.res.in",
    description="A tool for analyzing SSRs in genomic data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/BioinformaticsOnLine/croSSRoadd",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)