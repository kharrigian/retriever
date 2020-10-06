## Imports
from setuptools import setup, find_packages

## README
with open("README.md", "r") as fh:
    long_description = fh.read()

## Requirements
with open("requirements.txt", "r") as r:
    requirements = [i.strip() for i in r.readlines()]

## Run Setup
setup(
    name="retriever",
    version="0.0.1",
    author="Keith Harrigian",
    author_email="kharrigian@jhu.edu",
    description="Assorted API wrappers (and wrappers around wrappers)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/kharrigian/retriever",
    packages=find_packages(),
    python_requires='>=3.7',
    install_requires=requirements,
    include_package_data=True,
    package_data={
        "retriever":["config.json"]
    },
    exclude_package_data={
                "":["*README.md"]
    }
)