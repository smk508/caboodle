import caboodle
from caboodle import artifacts
from fireworks import Message
import pickle
import torch
import os
import io

base_path = os.path.join(os.getcwd(), 'artifacts')
try:
    os.mkdir(base_path)
except:
    pass

class DummyFile():

    def __init__(self, base_folder):
        self.base_folder = base_folder
    
    def __enter__(self):
        self.clear()
        os.mkdir(self.base_folder)

    def __exit__(self, exc_type, exc_value, tb):
        self.clear()

    def clear(self):
        try:
            files = os.listdir(self.base_folder)
            for filename in files:
                os.remove(os.path.join(self.base_folder, filename))
            os.rmdir(self.base_folder)
        except FileNotFoundError:
            pass

def test_open_close():

    p = [1,2,3,4,'hii']
    path = os.path.join(base_path, 'test')
    with DummyFile(path):
        art = artifacts.PickleArtifact('test', p, path_or_buffer=os.path.join(path, 'open.pickle'))
        art.save()
        art2 = artifacts.PickleArtifact('test', path_or_buffer=os.path.join(path, 'open.pickle'))
        assert art2._content == None
        with art2 as x:
            assert art2._content == p
            assert p == x
        assert art2._content == None
        art3 = artifacts.PickleArtifact('test', path_or_buffer=os.path.join(path, 'open.pickle'))
        assert art2._content == None
        art3.load()
        assert art3._content == p
        art3.close()
        assert art3._content == None
        art4 = artifacts.PickleArtifact('test', path_or_buffer=os.path.join(path, 'open.pickle'))
        assert art4._content == None
        assert art4.data == p
        assert art4._content == p
        art4.close()
        assert art4._content == None

def test_FireworksArtifact():
    
    return 
    m = Message({'a': [1,2,3], 'b': torch.tensor([4,5,6])})
    art = artifacts.FireworksArtifact('test', m)
    path = os.path.join(base_path, 'fireworks')
    with DummyFile(path):
        file_path = os.path.join(path,'test.fireworks')
        art.serialize(file_path)
        m2 = art.deserialize(file_path)
        assert type(m2) is Message
        assert m2 == m
    buffer = io.BytesIO()
    art.serialize(buffer)
    m3 = art.deserialize(buffer)
    assert m3 == m

def test_PickleArtifact():
    p = [1,2,3,4,'hii']
    art = artifacts.PickleArtifact('test', p)
    path = os.path.join(base_path, 'pickle')
    with DummyFile(path):
        file_path = os.path.join(path,'test.pickle')
        art.serialize(file_path)
        p2 = art.deserialize(file_path)
        assert p2 == p
    buffer = io.BytesIO()
    art.serialize(buffer)
    p3 = art.deserialize(buffer)
    assert p3 == p

def test_BinaryArtifact():
    b = b'hohohooh'
    art = artifacts.BinaryArtifact('test', b)
    path = os.path.join(base_path, 'binary')
    with DummyFile(path):
        file_path = os.path.join(path,'test.bin')
        art.serialize(file_path)
        b2 = art.deserialize(file_path)
        assert b2 == b
    buffer = io.BytesIO()
    art.serialize(buffer)
    b3 = art.deserialize(buffer)
    assert b3 == b
