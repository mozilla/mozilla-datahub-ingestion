from setuptools import find_namespace_packages, setup

setup(
    name="datahub-sync",
    version="0.0",
    packages=find_namespace_packages(include=["sync*"]),
    install_requires=["google-cloud-bigquery"],
)
