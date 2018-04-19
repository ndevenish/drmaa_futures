#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests of the zeromq master loop."""

import threading
from contextlib import contextmanager

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
    while run:
      zmq.process_messages()
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

def test_hello(loop, client):
  logger.debug("t")
  client.send(b"HELO 0")
  assert client.recv() == b"HAY"
  assert loop._workers