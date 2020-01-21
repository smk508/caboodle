import caboodle
from caboodle import artifacts, coffer
from fireworks import Message
import pickle
import torch
import os
import io

def test_Coffer():

    m = Message({'a': [1,2,3], 'b': torch.tensor([4,5,6])})
    art1 = artifacts.FireworksArtifact('test.fireworks', m)
    p = [1,2,3,4,'hii']
    art2 = artifacts.PickleArtifact('test.pickle', p)
    b = b'hohohooh'
    art3 = artifacts.BinaryArtifact('test.bin', b)

    coffee = coffer.DebugCoffer()
    art_gallery = [art1, art2, art3]
    coffee.upload(art_gallery)
    fart_gallery = coffee.download()
    art_gallery_dict = {
        'fireworks': art1.content,
        'pickle': art2.content,
        'binary': art3.content
    }
    fart_gallery_dict = {}
    for artifact in fart_gallery:
        if artifact.key.endswith('fireworks'):
            fart_gallery_dict['fireworks'] = artifact.content
        if artifact.key.endswith('pickle'):
            fart_gallery_dict['pickle'] = artifact.content
        if artifact.key.endswith('binary'):
            fart_gallery_dict['binary'] = artifact.content                        

    for art, fart in zip(art_gallery_dict.values(), fart_gallery_dict.values()):
        assert art == fart
