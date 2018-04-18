# coding: utf-8

import zmq
import drmaa
import threading
from concurrent.futures import Future
from six.moves.queue import Queue

from enum import Enum

class WorkerState(Enum):
  "States to track the worker lifecycle"
  UNKNOWN = 1
  STARTED = 2
  WAITING = 3
  RUNNING = 4
  ENDED = 5

import logging
logger = logging.getLogger(__name__)

class WorkItem(object):
  def __init__(self, function, args, jobid):
    self.future = Future()
    self.fn = function
    self.args = args
    self.jobid = jobid

class Worker(object):
  def __init__(self, workerid):
    self.tasks = set()
    self.last_seen = None
    self.state = WorkerState.UNKNOWN

class ZeroMQListener(threading.Thread):
  def __init__(self):
    self._work_queue = Queue()
    self._work_info = {}
    self._workers = {}
    self._job_count = 0 # Counter for job ID

  def add_workitem(self, func, args):
    """Add a work item to the queue of items."""
    jobid = self._job_count
    self._job_count += 1
    self._work_info[jobid] = WorkItem(func, args, jobid)
    self._work_queue.add(jobid)

  def add_worker(self, worker_id):
    """Register a worker to the manager"""
    if worker_id in self._workers:
      logger.warn("Trying to add worker {} twice?".format(worker_id))
    else:
      self._workers[worker_id] = Worker(worker_id)

  def clean_stop(self):
    """Notify for termination, the zeroMQ loop, in preparation for joining the thread"""
    self._run = False

  def run(self):
    self._run = True
    self._context = zmq.Context()
    self._session = self._context.session(zmg.REP)
    self._session.RCVTIMEO = 200
    while self._run:
      try:
        req = self._session.recv()
        if req.startswith(b"HELO IAM"):
          worker = req[len(b"HELO IAM "):].decode("utf-8")
          logger.info("Got handshake from worker " worker)
          self._worker_handshake(worker)
        elif req.startswith(b"IZ BORED"): 
          worker = req[len(b"IZ BORED "):].decode("utf-8")
          if self._work_queue.empty():
            self._session.send(b"PLZ WAIT")
            logger.debug("Got request for item from ")
          else:
            self._session.send(b"PLZ DO" + self._get_workitem_for_run())
        elif req.startswith("YAY"):
        elif req.startswith("ONO"):
        else:
          logger.error("Got unknown message from worker: " + req.decode("latin-1"))
      except zmq.error.Again:
        # We hit a timeout. Just keep going
        pass

  def _worker_handshake(self, worker_id):
    """A Worker has said hello."""
    if not worker_id in self._workers:
      logger.warn("Handshake from unregistered worker {}".format(worker_id)P)
      self.add_worker(worker_id)
    worker.state_change(WorkerState.UNKNOWN, WorkerState

class Pool(object):
  """Manage a pool of DRMAA workers"""
  def __init__(self):
    self._session = drmaa.Session()
    self._session.initialize()
    # Start a zeromq listener in a thread

  def start(self):
    thread = threading.Thread(target=worker_routine, args=(url_worker,))
    thread.start()

