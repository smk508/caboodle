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
    def __init__(self, key:str, content:artifact_type = None, deserialize=False, path_or_buffer=None):
        self.key = key
        self._content = content
        self.path_or_buffer = path_or_buffer
        if content is None and deserialize:
            self._content = self.deserialize(self.path_or_buffer)

    def load(self):
        """
        De-serializes data from path_or_buffer and returns content.
        """
        if self._content is not None:
            return self._content
        if self.path_or_buffer is not None:
            self._content = self.deserialize(self.path_or_buffer)

        return self._content

    def save(self):
        """
        Serializes content and saves it to local path.
        """
        if self.path_or_buffer is None:
            raise AttributeError("There is no path_or_buffer attribute set for saving to.")
        self.serialize(self.path_or_buffer)

    def close(self):
        """
        Re-serializes data to path_or_buffer
        """
        if self.path_or_buffer is not None:
            self._content = None

    def __enter__(self):
        """
        Context manager deserializes file on entry and reserializes on exit.
        """
        return self.load()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def data(self):
        """
        This will deserialize data from file if necessary.
        """
        if self._content:
            return self._content

        elif self.path_or_buffer is not None:
            return self.load()

    @abc.abstractmethod
    def serialize(self, path_or_buffer: PathOrBuffer):
        pass
    
    @abc.abstractmethod
    def deserialize(self, path_or_buffer: PathOrBuffer):
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
                self.data.save(f)

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
            pickle.dump(self.data, f)
    
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
        data = self.data
        if type(data) is io.BytesIO:
            data = data.read()
        with get_buffer(path_or_buffer, direction = 'write') as f:
            f.write(data)

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
