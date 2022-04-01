import os
from setuptools import setup

setup(
    name='tweettemp451',
    version='0.1.0',
    description="Collection and averages temperature data for twitter's sample stream.",
    packages=['tweettemp451'],
    package_dir={'': 'src'},
    install_requires=[
        'httpx==0.22.0',
        'python-dateutil>=2.8',
        'tenacity>=8.0',
        'trio==0.19.0',
        ],
    )
