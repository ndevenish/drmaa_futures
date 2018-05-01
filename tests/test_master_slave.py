# coding: utf-8
"""Test the master talking to the slave"""

import logging
import os
import threading
import signal
import subprocess
import sys
import time

import pytest

from drmaa_futures.master import ZeroMQListener

logger = logging.getLogger(__name__)

def wait_until(condition, interval=0.1, timeout=1, *args):
  """Simple convenience function to wait for a condition."""
  start = time.time()
  while not condition(*args) and time.time() - start < timeout:
    time.sleep(interval)

@pytest.fixture
def master():
  zmq = ZeroMQListener()
  zmq._socket.LINGER = 300
  run = True

  def _do_thread():
    try:
      while run:
        zmq.process_messages()
    except Exception as e:
      logger.error("Got exception in worker thread: %s", e)
      logger.error(traceback.format_exc())

  thread = threading.Thread(target=_do_thread)
  # Allow loose test threads?
  # thread.daemon = True
  thread.start()
  yield zmq
  run = False
  logger.debug("Ended test loop")
  thread.join()
  zmq.shutdown()


@pytest.fixture
def slave(master):
  # def slave(url=None, id="0", timeout=None):
  # We need to update the environment to include this file, so that we can unpickle it's functions
  new_env = dict(os.environ)
  new_env["PYTHONPATH"] = ":".join(
      new_env.get("PYTHONPATH", "").split(":") + [os.path.dirname(__file__)])

  url = [master.endpoint]
  timeout = ["--timeout=3"]
  proc = subprocess.Popen(
      [sys.executable, "-m", "drmaa_futures", "-v", "slave"] + timeout + url,
      env=new_env)
  try:
    time.sleep(0.2)
    yield proc
  finally:
    try:
      # Kill in a gentle way
      os.kill(proc.pid, signal.SIGINT)
      proc.wait()
    except OSError:
      # On python2 trying to kill something that has just died seems to error
      pass


def test_worker_registered(master, slave):
  # Since we could have lag here, explicitly wait for a bit to give time to connect
  wait_until(lambda: master.active_workers, timeout=5)
  assert master.active_workers

def test_task_enqueue(master, slave):
  task = master.enqueue_task(lambda: 42)
  assert task.result(timeout=2) == 42

  task = master.enqueue_task(lambda: 42)
  assert task.cancel()
  task2 = master.enqueue_task(lambda: 1337)
  assert task2.result(timeout=2) == 1337

