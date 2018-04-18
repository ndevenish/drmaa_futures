#!/bin/env python3

import zmq
import dill as pickle
import os
import pprint

c = zmq.Context()
s = c.socket(zmq.REP)
s.bind("tcp://*:5555")

print("Recieved: " + s.recv().decode("ascii"))
s.send(b"HAY")

print("Recieved: " + s.recv().decode("ascii"))
s.send(b"PLZ DO " + pickle.dumps((0, lambda: os.environ)))

result = s.recv()
s.send(b"THX")
print("Got message " + result[:4].decode("ascii"))
res_id, result = pickle.loads(result[4:])
print("Environment: ", result)
pprint.pprint(result)

print("Recieved: " + s.recv().decode("ascii"))
print("Sending quit")
s.send(b"PLZ GOWAY")
