from google.cloud import storage
from typing import List, Tuple, Union
from caboodle import gcs
import pickle
import abc
import io
try:
    import fireworks
    fireworks_installed = True
except ModuleNotFoundError:
    fireworks_installed = False

# Instantiates a client
try:
    storage_client = storage.Client()
except Exception as e:
    print("{0}\n\n Try setting the environment variable GOOGLE_APPLICATION_CREDENTIALS to point to the file containing your service account key.".format(e))
    raise e
import os
import random
from typing import Union, Type, Dict

PathOrBuffer = Union[str, Type[io.BufferedIOBase]]
file_codes = {
        'read': 'rb',
        'write': 'wb',
    }

class get_buffer():
    """
    Given an object of type PathOrBuffer, returns a BytesIO buffer either by opening the file
    or returning the original argument if it is already a BytesIO. 
    Additionally, the direction can be set to 'read' or 'write' to specify how the file should
    be opened.
    """

    def __init__(self, path_or_buffer:PathOrBuffer, direction='read'): 
        
        self.path_or_buffer = path_or_buffer
        if type(path_or_buffer) is str: # Is a path
            self.type = 'string'
        elif isinstance(path_or_buffer, io.BufferedIOBase):
            self.type = 'buffer'
        else:
            raise TypeError("Argument {0} with type {1} is not a string or bytes buffer.")
        self.direction = direction    

    def __enter__(self) -> Type[io.BufferedIOBase]:
        if self.type == 'string':
            self.f = open(self.path_or_buffer, file_codes[self.direction])
            return self.f
        elif self.type == 'buffer':
            self.path_or_buffer.seek(0)
            return self.path_or_buffer
    
    def __exit__(self, type, value, traceback):
        if self.type == 'string':
            self.f.close()


class Artifact(metaclass=abc.ABCMeta):
    """
    Represents an artifact which can be passed between steps in a distributed workflow. In general, an artifact can be any object, but
    we add additional metadata so standardize the serialization / deserialization process. An artifact has a key, which is its name, and
    content, which is the actual data to be stored. The key is used to refer to the artifact in the storage system.
    """
    artifact_type = object
    def __init__(self, key:str, content:artifact_type, deserialize=False):
        self.key = key
        self.content = content
        if deserialize:
            self.content = self.deserialize(content)

    @abc.abstractmethod
    def serialize(self, path_or_buffer:PathOrBuffer):
        pass
    
    @abc.abstractmethod
    def deserialize(self, path:PathOrBuffer):
        """
        Loads the artifact from a given file path.
        """
        pass

    def __str__(self):
        return "{0} for {1}".format(str(self.__class__).split('.')[-1].rstrip("'>"), self.key)

if fireworks_installed:
    
    class FireworksArtifact(Artifact):
        """
        Represents a Fireworks Message as an artifact.
        """
        artifact_type = fireworks.Message

        def serialize(self, path_or_buffer:PathOrBuffer):
            with get_buffer(path_or_buffer, direction = 'write') as f:
                self.content.save(f)

        def deserialize(self, path_or_buffer:PathOrBuffer):
            with get_buffer(path_or_buffer, direction = 'read') as f:
                return fireworks.Message.load(f)

class PickleArtifact(Artifact):
    """
    Represens a Pickled object as an artifact.
    """
    artifact_type = object
    def serialize(self, path_or_buffer:PathOrBuffer):
        with get_buffer(path_or_buffer, direction = 'write') as f:
            pickle.dump(self.content, f)
    
    def deserialize(self, path_or_buffer:PathOrBuffer):
        with get_buffer(path_or_buffer, direction = 'read') as f:
            response = pickle.load(f)

        return response

class BinaryArtifact(Artifact):
    """
    Serializes binary directly to file.
    """
    artifact_type = bytes
    def serialize(self, path_or_buffer:PathOrBuffer):
        with get_buffer(path_or_buffer, direction = 'write') as f:
            f.write(self.content)

    def deserialize(self, path_or_buffer:PathOrBuffer):
        with get_buffer(path_or_buffer, direction = 'read') as f:
            response = f.read()
        
        return response

class AvroArtifact(Artifact):
    """
    Serializes an Avro object to file.
    """
    artifact_type = object
    pass

suffixes = { # File suffixes are used to automatically read in artifacts from file into the correct format.
    'bin': BinaryArtifact,
    'pickle': PickleArtifact
}

if fireworks_installed:
    suffixes['fireworks']: FireworksArtifact

def infer_type(name):
    """
    Returns the artifact type to use for a given filename.
    """
    suffix = name.split('.')[-1]
    try:
        return suffixes[suffix]
    except KeyError:
        raise KeyError
        ("Could not infer or parse file type. Must be one of: {0}".format(
            ", ".join(".{0}".format(s) for s in suffixes.keys())
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
        for key, buffer in artifact_dict.items():
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
    def __init__(self, bucket_name, path):
        
        self.bucket_name = bucket_name
        self.path = path
    
    def upload(self, artifacts: List[Type[Artifact]]):
        buffer_dict = self.serialize_artifacts(artifacts)
        bucket = storage_client.get_bucket(self.bucket_name)
        for key, buffer in buffer_dict.items():
            blob = bucket.blob(os.path.join(self.path, key))
            blob.upload_from_string(buffer.read())

    def download(self) -> List[Type[Artifact]]:
        bucket = storage_client.get_bucket(self.bucket_name)
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
