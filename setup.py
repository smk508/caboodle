from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='the-whole-caboodle',
    version='0.0.1',
    packages=find_packages(),
    author_email="skhan8@mail.einstein.yu.edu",
    description="Utilities for artifact management for data science in the cloud.",
    long_description=open('README.md').read(),
    url="https://github.com/smk508/caboodle",
    install_requires=requirements,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)