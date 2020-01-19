from google.cloud import storage
from typing import List, Tuple, Union
from caboodle import gcs, artifacts
from caboodle.artifacts import Artifact
import pickle
import abc
import io
import random
import os
from typing import Union, Type, Dict

try:
    import fireworks
    fireworks_installed = True
except ModuleNotFoundError:
    fireworks_installed = False

suffixes = { # File suffixes are used to automatically read in artifacts from file into the correct format.
    'bin': artifacts.BinaryArtifact,
    'pickle': artifacts.PickleArtifact
}

if fireworks_installed:
    suffixes['fireworks'] = artifacts.FireworksArtifact

def infer_type(name):
    """
    Returns the artifact type to use for a given filename.
    """
    suffix = name.split('.')[-1]
    try:
        return suffixes[suffix]
    except KeyError:
        raise KeyError
        ("Could not infer or parse file type for {1}. Must be one of: {0}".format(
            ", ".join(".{0}".format(s, name) for s in suffixes.keys())
            )
        )

class Coffer(metaclass=abc.ABCMeta):
    """
    Represents multiple artifacts stored in a single location (GCS bucket, etc.) by the output of or input to a pipeline step on Argo / Kubeflow.
    """

    @abc.abstractmethod
    def upload(self, artifacts: List[Artifact]):
        """
        Uploads the artifacts provided to the coffer.
        """
        pass

    @abc.abstractmethod
    def download(self, local_path:str):
        """
        Downloads the Artifacts in the coffer to a local path.
        """
        pass
    
    def serialize_artifacts(self, artifacts: List[Artifact]) -> Dict[str, bytes]:
        """
        Serializes a list of artifacts and returns a dictionary mapping their keys to their
        binary representations.
        """
        artifact_dict = {}
            
        for artifact in artifacts:
            buffer = io.BytesIO()
            artifact.serialize(buffer)
            buffer.seek(0)
            artifact_dict[artifact.key] = buffer
                
        return artifact_dict            

    def save_artifacts(self, path:str, artifacts: List[Artifact]) -> str:
        """
        Serializes a list of artifacts into local disc under a folder at the given path.
        Returns the name of the randomly seeded subfolder containing the saved artifacts.
        """
        seed = str(random.randint(0,1000))
        serial_path = os.path.join(path, seed)
        try:
            os.mkdir(serial_path)
        except:
            pass

        artifact_dict = self.serialize_artifacts(artifacts)            
        for key, artifact in artifact_dict.items():
            path = os.path.join(serial_path, key)
            artifact.serialize(path)
        
        return serial_path    

class DebugCoffer(Coffer):
    """
    This coffer saves artifacts in memory and is useful for testing.
    """
    def __init__(self):
        self.artifacts = []

    def upload(self, artifacts: List[Type[Artifact]]):
        self.artifacts.extend(artifacts)

    def download(self) -> List[Type[Artifact]]:
        return self.artifacts

class GCSCoffer(Coffer):
    """
    Represents multiple artifacts stored in a folder in a GCS bucket.
    """
    def __init__(self, gcs_path, storage_client=None):

        bucket_name, path = gcs.parse_gcs_path(gcs_path)
        self.bucket_name = bucket_name
        self.path = path
        self.storage_client = storage_client or gcs.get_storage_client()
    
    def upload(self, artifacts: List[Type[Artifact]]):
        buffer_dict = self.serialize_artifacts(artifacts)
        bucket = self.storage_client.get_bucket(self.bucket_name)
        for key, buffer in buffer_dict.items():
            blob = bucket.blob(os.path.join(self.path, key))
            blob.upload_from_string(buffer.read())

    def download(self) -> List[Type[Artifact]]:
        bucket = self.storage_client.get_bucket(self.bucket_name)
        blobs = bucket.list_blobs(prefix=self.path)
        artifacts = []
        for blob in blobs:
            try:
                artifact_type = infer_type(blob.name)
                buffer = io.BytesIO(blob.download_as_string())
                key = blob.name.split('/')[-1]
                artifact = artifact_type(key, buffer, deserialize=True)
                artifacts.append(artifact)
            except KeyError:
                pass
        
        return artifacts

    def get(self, artifact_name) -> Type[Artifact]:
        """
        Returns an artifact by name.
        """
        bucket = self.storage_client.get_bucket(self.bucket_name)
        blob = bucket.get_blob(os.path.join(self.path, artifact_name))
        if blob is None:
            raise ValueError(
                "Could not find artifact {0} at location gs://{1}/{2}".format(
                    artifact_name,
                    self.bucket_name,
                    self.path,
                )
            )
        artifact_type = infer_type(blob.name)
        buffer = io.BytesIO(blob.download_as_string())
        key = blob.name.split('/')[-1]
        artifact = artifact_type(key, buffer, deserialize=True)        

        return artifact
