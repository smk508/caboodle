# caboodle
[![Build Status](https://travis-ci.org/smk508/caboodle.svg?branch=master)](https://travis-ci.org/smk508/caboodle)
[![PyPI version](https://badge.fury.io/py/the-whole-caboodle.svg)](https://badge.fury.io/py/the-whole-caboodle)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![codecov](https://codecov.io/gh/smk508/caboodle/branch/master/graph/badge.svg)](https://codecov.io/gh/smk508/caboodle)
[![Documentation Status](https://readthedocs.org/projects/caboodle/badge/?version=latest)](https://caboodle.readthedocs.io/en/latest/?badge=latest)

Are you tired of futzing with your cloud provider's storage API? Keeping track
of file formats, serializing/deserializing, etc.? You just want
to upload and download files to a bucket?

Caboodle abstracts away the details of artifact management in Python. It handles file
syncing, authentication, and serialization/de-serialization and it does so in a
cloud-provider agnostic manner.

# Installation

    pip install the-whole-caboodle    

# Getting Started

See [documentation](https://caboodle.readthedocs.io/en/latest/?badge=latest) and
the concepts section below for more details.

The intended use case is some sort of cloud-based data science workload which
has three steps:
1) **Download input data** (artifacts) that you want to operate on. This may be a single file in
   bucket storage or an entire folder, and you want to operate on each file in
   that folder.
2) **Perform your computations**.
3) **Upload your results** (also artifacts) to the remote storage.

This three-step process could be part of a single step in a larger pipeline.
Caboodle is meant to simplify steps (1) and (3).

## 1) Download Input Artifacts

To download all files from a folder in a remote bucket:

    from caboodle import gcs
    from caboodle.coffer import GCSCoffer

    client = gcs.get_storage_client()
    my_coffer = GCSCoffer("gs://mybucket/path/in/bucket", storage_client=client)
    my_artifacts = my_coffer.download()
    python_objects = [m.content for m in my_artifacts]

`my_artifacts` is a list containing `Artifact` objects which have a
`deserialize` method which is used to read the raw data into python objects. The
`Coffer` will attempt to infer file type and perform the appropriate
serialization.

## 2) Perform Computations on Data

This is your job. Godspeed.

## 3) Upload Output Artifacts

To upload all files in a folder to a remote bucket:

    from caboodle import gcs
    from caboodle.coffer import GCSCoffer

    client = gcs.get_storage_client()
    my_coffer = GCSCoffer("gs://mybucket/path/in/bucket", storage_client=client)
    my_coffer.upload(list_of_artifacts)

`list_of_artifacts` should be a list of `Artifact`s. See below for more information.

# Concepts

Your data is serialized using the `Artifact` class, and these are stored in a `Coffer`
which syncs that data with a remote storage provider. You use these two objects
to upload and download various types of files from whatever cloud storage you use.

## Artifacts

An artifact represents a blob of data that could be the input or output of some
workload. An `Artifact` object contains logic for storing, serializing, and
deserializing its contents. This gives you a single
interface for saving data regardless of its type.

    my_data = [1,2,3] # Some picklable python object
    my_artifact = PickleArtifact("mydata", my_data)
    my_artifact.serialize("my_data.pickle")

In this example, we created some object and save it using Python's
[`pickle`](https://docs.python.org/3/library/pickle.html) module to the file
`"my_data.pickle"`. Additionally, `Artifact`s have a key which 'Coffer's
(discussed below) use to automatically make a filename when saving data. 

You can analogously read in data from a file using the appropriate `Artifact`
class:

    my_data2 = my_artifact.deserialize("my_data.pickle")
    print(my_data2)
    >>> [1,2,3]

Currently, the following types of `Artifact`s have been implemented: pickle, [Apache Avro](https://avro.apache.org/), [Fireworks](https://github.com/kellylab/Fireworks), and binary.

## Coffers

Whereas `Artifact`s handle serialization logic, a `Coffer` handles the logic of
uploading `Artifact`s to a remote storage location:

    client = caboodle.gcs.get_storage_client()
    my_coffer = GCSCoffer("gs://mybucket/path/in/bucket", storage_client=client)
    my_coffer.upload([my_artifact1, my_artifact2]) 

Analogously, you can download `Artifact`s:

    downloaded_artifacts = my_coffer.download()
    python_objects = [m.content for m in downloaded_artifacts]

The `Coffer` will attempt to infer filetype and construct the appropriate
filetype for each `Artifact` (defaulting to binary). Thus, 'python_objects' is a
list containing the deserialized artifacts that you initially uploaded.

Currently, only the `GCSCoffer` has been implemented, but in the future
we will
have analogous ones for AWS, Azure, and any other storage system.

## Cloud-specific Utilities

`caboodle.gcs` contains helper functions for operations like uploading and
downloading folders to Google Cloud Storage. It uses the official
[google-cloud-storage](https://googleapis.dev/python/storage/latest/index.html)
API. 

Right now, only support for Google Cloud has been implemented. AWS and Azure will be added in the future.

See [documentation](https://caboodle.readthedocs.io/en/latest/?badge=latest) for
more details.

# Contributing

Pull requests, questions, comments, and issues are welcome. See the issues tab
for current tasks that need to be done. 
You can also reach me directly at skhan8@mail.einstein.yu.edu