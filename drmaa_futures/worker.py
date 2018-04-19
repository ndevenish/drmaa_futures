# coding: utf-8

"""
Classes and code to manage and interact with worker processes
"""

import os
import sys
from enum import Enum
import drmaa

import logging
logger = logging.getLogger(__name__)

class WorkerState(Enum):
  """States to track the worker lifecycle"""
  UNKNOWN = 1
  STARTED = 2
  WAITING = 3
  RUNNING = 4
  ENDED = 5

# Table of possible worker state transitions. Used to detect possible
# errors where we've missed/misprogrammed a change
WorkerState.mapping = {
    WorkerState.UNKNOWN: {WorkerState.STARTED},
    WorkerState.STARTED: {WorkerState.WAITING},
    WorkerState.WAITING:
    {WorkerState.RUNNING, WorkerState.ENDED, WorkerState.WAITING},
    WorkerState.RUNNING: {WorkerState.TASKCOMPLETE},
    WorkerState.TASKCOMPLETE: {WorkerState.WAITING},
    WorkerState.ENDED: set(),
}

class Worker(object):
  """Represents a remote worker instance"""
  def __init__(self, workerid):
    """Initialise a Worker instance.

    :param str workerid: An identifier for the worker this represents
    """
    self.id = workerid
    self.tasks = set()
    self.last_seen = None
    self.state = WorkerState.UNKNOWN

  def state_change(self, new):
    """Change the worker state.

    Validates against known transitions. If the state change is invalid, then a
    warning will be printed to the log, but the state change will proceed.

    :param WorkerState new: The new state to transition to.
    """
    if new not in WorkerState.mapping[self.state]:
      logger.warn("Invalid state transition for worker {}: {} → {}".format(
          self.id, self.state, new))
    self.state = new

  @classmethod
  def launch(cls, pool, timeout=None):
    jt = pool.session.createJobTemplate()
    jt.remoteCommand = sys.executable

    # Build a copy of environ with a backed up LD_LIBRARY_PATH - SGE
    # disallows passing of this path through but we probably need it
    env = dict(os.environ)
    env["_LD_LIBRARY_PATH"] = env.get("LD_LIBRARY_PATH", "")
    jt.jobEnvironment = env

    # If we need to pass a timeout parameter
    timeoutl = [] if timeout is None else ["--timeout={}".format(timeout)]
    # Work out a unique worker_if
    worker_id = pool.get_new_worker_id()

    # jt.args = ["-mdrmaa_futures", "-v", "slave"
    #            ] + timeoutl + [host_url, _worker_id]
