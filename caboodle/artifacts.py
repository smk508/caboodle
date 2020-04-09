from google.cloud import storage
from typing import List, Tuple, Union
from caboodle import gcs
import pickle
import abc
import io
import os
from typing import Union, Type, Dict
try:
    import fireworks
    fireworks_installed = True
except ModuleNotFoundError:
    fireworks_installed = False

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
