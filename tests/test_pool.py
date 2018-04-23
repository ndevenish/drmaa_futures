# coding: utf-8

"""Test the pool launching and management capabilities"""

import pytest
from collections import namedtuple
from drmaa_futures.master import bind_to_endpoint

# If we can't import drmaa (e.g. no good environment for it), then
# skip any tests in this file
try:
  import drmaa
except RuntimeError:
  pytestmark = pytest.mark.skip()


ServerInfo = namedtuple("ServerInfo", ["socket", "endpoint"])
@pytest.fixture
def server(url=None):
  """Basic server for clients to connect to"""
  url = url or "tcp://127.0.0.1:5555"
  c = zmq.Context()
  socket = c.socket(zmq.REP)
  socket.RCVTIMEO = 500
  endpoint = bind_to_endpoint(socket)
  try:
    yield ServerInfo(socket, endpoint)
  finally:
    socket.close()
    c.term()

@pytest.fixture
def pool(server):
  pass

def test_a():
  pass