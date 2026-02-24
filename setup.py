# setup.py
from setuptools import setup, find_packages

setup(
    name="wb_api_client",
    version="0.1.9",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[
        "httpx[socks]>=0.28.1",
        "msgspec>=0.18.6",
        "coloredlogs>=15.0.1",
        "redis>=5.0.1"
    ],
    python_requires=">=3.9",
    author="Your Team Name",
    description="Wildberries API Client with proxy support",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: Other/Proprietary License",
        "Operating System :: OS Independent",
    ],
)