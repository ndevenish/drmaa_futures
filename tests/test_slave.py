#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for drmaa_futures slave."""

from __future__ import print_function

import sys
import time
import pytest
import subprocess
from contextlib import contextmanager
import dill as pickle # Allow e.g. pickling of lambdas
# import pickle as pickle

from drmaa_futures.slave import run_slave

import zmq

@contextmanager
def slave(url=None, id=None, timeout=None):
  url = url or "tcp://127.0.0.1:5555"
  id = id if id is not None else "0"
  """Run a slave as a context manager"""
  # proc = Process(target=run_slave, args=("tcp://127.0.0.1:5555", "0"))
  # proc.start()
  timeoutl = [] if timeout is None else ["--timeout={}".format(timeout)]
  proc = subprocess.Popen([sys.executable, "-m", "drmaa_futures", "-v", "slave"] + timeoutl + [url, id])
  try:
    yield proc
  finally:
    proc.kill()

def test_launch_slave_subprocess():
  slave = subprocess.Popen([sys.executable, "-m", "drmaa_futures", "--help"])
  assert slave.wait() == 0

def test_slave_hello():
  c = zmq.Context()
  socket = c.socket(zmq.REP)
  socket.bind("tcp://127.0.0.1:5555")
  socket.RCVTIMEO=200
  with slave():
    assert socket.recv().decode("utf-8") == "HELO IAM 0"
    socket.send(b"HAY")
    time.sleep(0.2)

def test_slave_timeout():
  """Test that the slave times out whilst waiting with nothing to do"""
  c = zmq.Context()
  socket = c.socket(zmq.REP)
  socket.bind("tcp://127.0.0.1:5555")
  socket.RCVTIMEO=200
  test_timeout = 3
  with slave(timeout=test_timeout) as proc:
    start = time.time()
    sent = 0
    # Do hello negotiation
    socket.recv()
    socket.send(b"HAY")
    # Now wait for job requests
    while time.time()-start < test_timeout+2:
      try:
        assert socket.recv().startswith(b"IZ BORED")
        socket.send(b"PLZ WAIT")
        sent += 1
      except zmq.error.Again:
        pass
    # Make sure this terminated and we had at least one communication
    assert sent > 0
    assert proc.poll() is not None

def _basic_running_timer():
  start = time.time()
  time.sleep(1.0)
  return time.time()-start

def test_basic_slave_quit():
  c = zmq.Context()
  socket = c.socket(zmq.REP)
  socket.bind("tcp://127.0.0.1:5555")
  socket.RCVTIMEO=5000
  # test_timeout = 3
  with slave() as proc:
    socket.recv()
    socket.send(b"HAY")
    assert socket.recv().startswith(b"IZ BORED")
    socket.send(b"PLZ GOWAY")
    time.sleep(0.2)
    assert proc.poll() == 0

def test_basic_slave_task():
  c = zmq.Context()
  socket = c.socket(zmq.REP)
  socket.bind("tcp://127.0.0.1:5555")
  socket.RCVTIMEO=5000
  # test_timeout = 3
  with slave() as proc:
    # start = time.time()
    # sent = 0
    # Do hello negotiation
    socket.recv()
    socket.send(b"HAY")
    # Now wait for job requests
    assert socket.recv().startswith(b"IZ BORED")
    socket.send(b"PLZ DO " + pickle.dumps((0, _basic_running_timer)))
    result = socket.recv()
    assert result.startswith(b"YAY")
    res_id, result = pickle.loads(result[4:])
    # print(result)
    assert res_id == 0
    assert result > 1.0
    socket.send(b"THX")

    # And again
    assert socket.recv().startswith(b"IZ BORED")
    socket.send(b"PLZ DO " + pickle.dumps((4, lambda: time.sleep(1.2))))
    res_id, result = pickle.loads(socket.recv()[4:])
    assert res_id == 4
    assert result is None
    socket.send(b"THX")
