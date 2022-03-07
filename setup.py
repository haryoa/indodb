"""
Setup initial code
"""

from setuptools import setup, find_packages

setup(
    name="indodb",
    version="0.0.0",
    description="Trial INdo DB",
    author="Haryo A W",
    author_email="haryo@coleaves.ai",
    url="https://github.com/haryoa/indodb",
    download_url="https://github.com/haryoa/indodb",
    packages=find_packages(exclude=["tests"]),
    python_requires=">=3.7",
    install_requires=[
        "pandas>=1.3.4",
        "rich>=11.2.0",
    ],
    setup_requires=[],
)
