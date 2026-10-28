from setuptools import find_namespace_packages, setup

setup(
    name="datahub-sync",
    version="0.0",
    packages=find_namespace_packages(include=["sync*"]),
    install_requires=[
        "google-cloud-bigquery",
        "mozilla-metric-config-parser==2024.8.1",  # same as requirements.in
        # Same major version as requirements.txt. Not an exact pin, which could conflict
        # with the sqlglot pin of the DataHub CLI version the Acryl executor installs.
        "sqlglot>=30.12,<31",
    ],
)
