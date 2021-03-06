from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name = "Utility_Functions",
    author="Allison Wu",
    author_email="allison.wu@thermofisher.com",
    description="Utility functions",
    notes = "Add in stats_functions",
    version = "0.3.1",
    packages = find_packages(),
    long_description=long_description,
    classifiers = ['Programming Language :: Python :: 3.7'],
    )
