#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for drmaa_futures slave."""

from __future__ import print_function

import os
import sys
import time
import pytest
import signal
import subprocess
from contextlib import contextmanager
import logging
import dill as pickle # Allow e.g. pickling of lambdas
# import pickle as pickle

from drmaa_futures.slave import run_slave, TaskSystemExit, ExceptionPicklingError

import zmq

logger = logging.getLogger(__name__)

@contextmanager
def server(url=None):
  url = url or "tcp://127.0.0.1:5555"
  c = zmq.Context()
  socket = c.socket(zmq.REP)
  socket.RCVTIMEO = 500
  socket.bind("tcp://127.0.0.1:5555")
  try:
    yield socket
  finally:
    socket.close()
    c.term()

@contextmanager
def slave(url=None, id="0", timeout=None):
  # We need to update the environment to include this file, so that we can unpickle it's functions
  new_env = dict(os.environ)
  new_env["PYTHONPATH"] = ":".join(new_env.get("PYTHONPATH", "").split(":") + [os.path.dirname(__file__)])

  url = [url or "tcp://127.0.0.1:5555"]
  id = [id] if id is not None else []
  """Run a slave as a context manager"""
  # proc = Process(target=run_slave, args=("tcp://127.0.0.1:5555", "0"))
  # proc.start()
  timeoutl = [] if timeout is None else ["--timeout={}".format(timeout)]
  proc = subprocess.Popen([sys.executable, "-m", "drmaa_futures", "-v", "slave"] + timeoutl + url + id, env=new_env)
  try:
    yield proc
  finally:
    try:
      # Kill in a gentle way
      os.kill(proc.pid, signal.SIGINT)
      proc.wait()
    except OSError:
      # On python2 trying to kill something that has just died seems to error
      pass

def test_launch_slave_subprocess():
  slave = subprocess.Popen([sys.executable, "-m", "drmaa_futures", "--help"])
  assert slave.wait() == 0
  # And, without a command
  slave = subprocess.Popen([sys.executable, "-m", "drmaa_futures"])
  assert slave.wait() != 0


def test_slave_hello():
  with server() as socket, slave():
    socket.RCVTIMEO=500
    assert socket.recv().decode("utf-8") == "HELO IAM 0"
    socket.send(b"HAY")
    time.sleep(0.2)

def test_slave_hello_timeout():
  """Test that the slave times out whilst waiting for a handshake"""
  # First test timing out without even recieving it's message
  logger.debug("Testing no message at all")
  with slave(timeout=0.3) as proc:
    time.sleep(0.4)
    assert proc.poll is not None
  # .. and test not getting a response
  logger.info("Testing the slave not getting a response")
  with server() as socket, slave(timeout=0.3) as proc:
    socket.RCVTIMEO=500
    assert socket.recv().decode("utf-8") == "HELO IAM 0"
    logger.info("Recieved initial message, waiting to see if it dies")
    time.sleep(0.4)
    assert proc.poll is not None

def test_slave_timeout():
  """Test that the slave times out whilst waiting with nothing to do"""
  test_timeout = 3
  with server() as socket, slave(timeout=test_timeout) as proc:
    socket.RCVTIMEO=500
    start = time.time()
    sent = 0
    # Do hello negotiation
    socket.recv()
    socket.send(b"HAY")
    # Now wait for job requests
    while time.time()-start < test_timeout+2:
      try:
        msg = socket.recv()
        if msg.startswith(b"IZ BORED"):
          socket.send(b"PLZ WAIT")
        elif msg.startswith(b"IGIVEUP"):
          socket.send(b"BYE")
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
  with server() as socket, slave() as proc:
    socket.RCVTIMEO=5000
    socket.recv()
    socket.send(b"HAY")
    assert socket.recv().startswith(b"IZ BORED")
    socket.send(b"PLZ GOWAY")
    time.sleep(0.2)
    assert proc.poll() == 0

def test_basic_slave_task():
  with server() as socket, slave() as proc:
    socket.RCVTIMEO=5000
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
    res = socket.recv()
    assert res.startswith(b"YAY")
    res_id, result = pickle.loads(res[4:])
    assert res_id == 4
    assert result is None
    socket.send(b"THX")

def test_failed_slave_task():
  with server() as socket, slave() as proc:
    socket.recv()
    socket.send(b"HAY")

    def send_task(func):
      """Reusable function to send task and raise the exception it returns"""
      assert socket.recv().startswith(b"IZ BORED")
      socket.send(b"PLZ DO " + pickle.dumps((0, func)))
      res = socket.recv()
      assert res.startswith(b"ONO ")
      res_id, trace, exc = pickle.loads(res[4:])
      socket.send(b"THX")
      raise exc

    # Test something that raises an exception
    def _raise_exception():
      raise RuntimeError("Testing failure")
    with pytest.raises(RuntimeError, message="Testing failure"):
      send_task(_raise_exception)

    # Test something that calls sys.exit()
    with pytest.raises(TaskSystemExit):
      send_task(lambda: sys.exit())


    # Try simulating an exception that can't be pickled
    def _raise_pickleerror():
      class UnpickleableException(Exception):
        def __getstate__(self):
          assert False
      raise UnpickleableException()
    with pytest.raises(ExceptionPicklingError):
      send_task(_raise_pickleerror)

    # Simulate the worker process getting a KeyboardInterrupt
    def _raise_interrupt():
      raise KeyboardInterrupt()
    with pytest.raises(KeyboardInterrupt):
      send_task(_raise_interrupt)
    # Check that the worker immediately quits
    assert socket.recv().startswith(b"IGIVEUP")
    socket.send(b"BYE")
    # Give it time to be handled
    time.sleep(0.3)

def test_slave_self_assign_name():
  with server() as socket:
    with slave(id=None) as proc:
      hello = socket.recv()
      socket.send(b"HAY")
      wid = hello[9:].decode("UTF-8")
      assert len(wid) == 32

    os.environ["JOB_ID"] = "1337"
    os.environ["SGE_TASK_ID"] = "undefined"
    with slave(id=None) as proc:
      hello = socket.recv()
      socket.send(b"HAY")
      wid = hello[9:].decode("UTF-8")
      assert wid == "1337"

    os.environ["SGE_TASK_ID"] = "5"
    with slave(id=None) as proc:
      hello = socket.recv()
      socket.send(b"HAY")
      wid = hello[9:].decode("UTF-8")
      assert wid == "1337.5"
