# coding: utf-8

import zmq
import drmaa
import threading
from concurrent.futures import Future
from six.moves.queue import Queue
import time

from enum import Enum

import logging
logger = logging.getLogger(__name__)

from .worker import Worker, WorkerState

class WorkItem(object):
  """Represents a task for workers to do"""
  def __init__(self, function, args, kwargs, jobid):
    """Initialise a WorkItem.

    :param Callable function: The function to run on the remote host
    :param Iterable args:     Positional arguments to pass to the function
    :param dict kwargs:       Keyword arguments to pass to the function
    :param jobid:             A job identifier for the new item
    """
    self.id = jobid
    self.future = Future()
    # Serialize this function now, to preserve anything the user might
    # have passed in and alter later
    self.data = pickle.dumps(lambda: function(*args, **kwargs))
    self.worker_id = None


class ZeroMQListener(threading.Thread):
  """Handle the zeromq/worker loop"""
  def __init__(self):
    self._work_queue = Queue()
    self._work_info = {}
    self._workers = {}
    self._job_count = 0  # Counter for job ID

  def add_workitem(self, func, args, kwargs):
    """Add a work item to the queue of items.
    :returns: A Future tied to the work item
    """
    jobid = self._job_count
    self._job_count += 1
    item = WorkItem(func, args, kwargs, jobid)
    self._work_info[jobid] = item
    # Once added to the queue, only this class may touch it
    self._work_queue.add(jobid)
    return item.future

  def add_worker(self, worker_id):
    """Register a worker to the manager."""
    if worker_id in self._workers:
      logger.warn("Trying to add worker {} twice?".format(worker_id))
    else:
      self._workers[worker_id] = Worker(worker_id)
    return self._workers[worker_id]

  def clean_stop(self):
    """Notify for termination, the zeroMQ loop, in preparation for joining the thread"""
    self._run = False

  def run(self):
    self._run = True
    self._context = zmq.Context()
    self._socket = self._context.socket(zmg.REP)
    self._socket.RCVTIMEO = 200
    while self._run:
      try:
        req = self._socket.recv()
        if req.startswith(b"HELO IAM"):
          worker = req[len(b"HELO IAM "):].decode("utf-8")
          logger.info("Got handshake from worker " + worker)
          self._worker_handshake(worker)
        elif req.startswith(b"IZ BORED"):
          worker = req[len(b"IZ BORED "):].decode("utf-8")
          logger.debug("Got request for task from {}".format(worker))
          # Get a job for this to do
          job = self._get_next_job()
          if job is None:
            self._socket.send(b"PLZ WAIT")
            self._worker_waiting(worker)
            logger.debug("... no jobs for {}, asking to wait".format(worker))
          else:
            self._socket.send(b"PLZ DO" + job.data)
            self._worker_given_job(worker, job.id)
            logger.debug("Worker {} given task {}".format(worker, job.id))
        elif req.startswith(b"YAY"):
          self._complete_task(res[4:])
        elif req.startswith(b"ONO"):
          self._fail_task(res[4:])
        elif req.startswith(b"IGIVEUP"):
          worker = req[len(b"IGIVEUP "):].decode("utf-8")
          self._socket.send(b"BYE")
          logger.debug("Worker {} ended".format(worker))
          self._worker_ended(worker)
        else:
          logger.error(
              "Got unknown message from worker: " + req.decode("latin-1"))
      except zmq.error.Again:
        # We hit a timeout. Just keep going
        pass
    # We're shutting down. Close the socket and context.
    logger.debug("Ending ZeroMQ socket and context")
    self._socket.close()
    self._context.term()

  def _worker_handshake(self, worker_id):
    """A Worker has said hello. Change it's state and make sure it's known."""
    if not worker_id in self._workers:
      logger.warn("Handshake from unregistered worker {}".format(worker_id))
      self.add_worker(worker_id)
    worker = self._workers[worker_id]
    # Register that we now have seen this worker
    worker.state_change(WorkerState.STARTED)
    worker.last_seen = time.time()

  def _worker_waiting(self, worker_id):
    """A worker is awaiting a new job"""
    if not worker_id in self._workers:
      logger.error(
          "Worker entering wait state, but unknown?! ({})".format(worker_id))
      self.add_worker(worker_id)
    worker = self._workers[worker_id]
    worker.state_change(WorkerState.WAITING)
    worker.last_seen = time.time()

  def _worker_given_job(self, worker_id, job_id):
    """A worker has been given a job"""
    if not worker_id in self._workers:
      logger.error("Worker given job, but unknown?! ({})".format(worker_id))
      self.add_worker(worker_id)
    worker = self._workers[worker_id]
    worker.state_change(WorkerState.RUNNING)
    worker.tasks.append(job_id)
    worker.last_seen = time.time()
    job = self._work_info[job_id]
    assert job.worker_id is None
    job.worker_id = worker_id

  def _worker_ended(self, worker_id):
    worker.state_change(WorkerState.ENDED)
    worker.last_seen = time.time()

  def _get_next_job(self):
    """Look at queues and cancellations to get the next job item.

    :returns: A WorkItem whose Future state has been set to running
    """
    while True:
      try:
        job_id = self._work_queue.get(block=False)
      except Queue.Empty:
        return None
      else:
        # Make sure this isn't cancelled and set as running
        job = self._work_info[job_id]
        if job.future.set_running_or_notify_cancel():
          # This job isn't cancelled, and has been set as running
          return self._work_info[job_id]

  def _complete_task(self, data):
    (task_id, result) = pickle.loads(data)
    job = self._work_info[task_id]
    worker = self._workers[job.worker_id]
    logger.debug("Worker {} succeeded in {}".format(worker.id, job.id))
    worker.state_change(WorkerState.TASKCOMPLETE)
    worker.last_seen = time.time()
    job.future.set_result(result)
    # Remove this work item from the info dictionary
    del self._work_info[task_id]

  def _fail_task(self, data):
    (task_id, exc_trace, exc_value) = pickle.loads(data)
    job = self._work_info[task_id]
    worker = self._workers[job.worker_id]
    logger.debug("Worker {} task failed in {}: {}".format(
        worker.id, job.id, exc_value))
    logger.debug("Stack trace: " + exc_trace)
    worker.state_change(WorkerState.TASKCOMPLETE)
    worker.last_seen = time.time()
    job.future.set_exception(exc_value)

    # Clean up worker/task associations
    worker.tasks.remove(job.id)
    job.worker_id = None
    # Remove this work item from the info dictionary
    del self._work_info[task_id]


class Pool(object):
  """Manage a pool of DRMAA workers"""

  def __init__(self):
    self._session = drmaa.Session()
    self._session.initialize()
    # Start a zeromq listener in a thread

  def 

  def launch_worker(self):
    pass

  # def start(self):
    # thread = threading.Thread(target=worker_routine, args=(url_worker, ))
    # thread.start()
