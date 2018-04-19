#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests of the zeromq master loop."""

import threading
import traceback
from contextlib import contextmanager

import dill as pickle
from drmaa_futures.master import ZeroMQListener
import zmq

import pytest
import logging

# logging.basicConfig()#level=logging.DEBUG)
logger = logging.getLogger(__name__)

@pytest.fixture
def loop():
  zmq = ZeroMQListener(endpoint="inproc://test")
  run = True
  def _do_thread():
    try:
      while run:
        zmq.process_messages()
    except Exception as e:
      logger.error("Got exception in worker thread: %s", e)
      traceback.print_exc()
  thread = threading.Thread(target=_do_thread)
  # Allow loose test threads?
  # thread.daemon = True
  thread.start()
  yield zmq
  run = False
  thread.join()

@pytest.fixture
def client(loop):
  s = loop._context.socket(zmq.REQ)
  s.RCVTIMEO = 500
  s.connect("inproc://test")
  yield s
  s.close()

def test_hello_and_goodbye(loop, client):
  client.send(b"HELO IAM 0")
  assert client.recv() == b"HAY"
  assert loop._workers
  client.send(b"HELO IAM C3PO")
  assert client.recv() == b"HAY"
  assert "C3PO" in loop._workers
  assert loop.active_workers == 2
  client.send(b"IGIVEUP 0")
  assert client.recv() == b"BYE"
  assert loop.active_workers == 1

def test_no_tasks(loop, client):
  client.send(b"HELO IAM 0")
  client.recv()
  client.send(b"IZ BORED 0")
  assert client.recv() == b"PLZ WAIT"

def test_simple_task(loop, client):
  # Two workers
  client.send(b"HELO IAM 0")
  client.recv()
  client.send(b"HELO IAM 1")
  client.recv()
  # Put a task onto the queue
  task = loop.enqueue_task(lambda: 42)
  assert not task.running()
  assert not task.done()
  client.send(b"IZ BORED 0")
  # Fetch the task
  response = client.recv()
  assert task.running()
  assert response.startswith(b"PLZ DO ")
  (given_id, given_func) = pickle.loads(response[7:])
  logger.info("Got task generated with ID %s", given_id)
  assert given_func() == 42
  # Check that we get nothing from the server as a different client
  client.send(b"IZ BORED 1")
  assert client.recv() == b"PLZ WAIT"
  # Send back the result
  client.send(b"YAY " + pickle.dumps((given_id, 42)))
  assert client.recv() == b"THX"
  assert not task.running()
  assert task.done()
  assert task.result() == 42

def test_task_cancel(loop, client):
  client.send(b"HELO IAM 0")
  client.recv()
  task = loop.enqueue_task(lambda: 42)
  assert not task.running()
  assert not task.done()
  # Cancel...
  assert task.cancel()
  # And now, ask for a task
  client.send(b"IZ BORED 0")
  assert client.recv() == b"PLZ WAIT"


