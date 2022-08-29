from setuptools import setup

setup(
    name='datahub-sync',
    version='0.0',
    py_modules=['sync'],
    entry_points='''
        [console_scripts]
        sync=sync.cli:cli
    ''',
)