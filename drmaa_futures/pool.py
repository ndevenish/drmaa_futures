# coding: utf-8

from concurrent.futures import Future
import logging
import sys
import threading
import time

import dill as pickle
import drmaa
from six.moves import queue
import zmq

from .worker import Worker, WorkerState

logger = logging.getLogger(__name__)


class Task(object):
  """Represents a task for workers to do"""
  def __init__(self, function, args, kwargs, task_id):
    """Initialise a Task

    :param Callable function: The function to run on the remote host
    :param Iterable args:     Positional arguments to pass to the function
    :param dict kwargs:       Keyword arguments to pass to the function
    :param taskid:            An identifier for the new task
    """
    self.id = task_id
    self.future = Future()
    self.worker = None
    # Serialize this function now, to preserve anything the user might
    # have passed in and alter later
    self.data = pickle.dumps(lambda: function(*args, **kwargs))


class ZeroMQListener(threading.Thread):
  """Handle the zeromq/worker loop"""
  def __init__(self):
    self._work_queue = queue.Queue()
    self._tasks = {}
    self._workers = {}
    self._task_count = 0  # Counter for task ID
    self._run = True

  def enqueue_task(self, func, args=None, kwargs=None):
    """Add a task to the queue of items.

    :param Callable func: The function to call in the task
    :param Iterable args: The positional arguments to pass to the function
    :param dict kwargs:   The keyword arguments to pass to the function
    :returns: A Future tied to the work item
    """
    taskid = self._task_count
    self._task_count += 1
    # Create the task item
    task = Task(func, args or [], kwargs or {}, id=taskid)
    task.id = taskid
    self._tasks[taskid] = task
    # Once added to the queue, only the update thread may touch it
    self._work_queue.put(taskid)
    return item.future

  def add_worker(self, worker_id):
    """Register a worker to the manager."""
    if worker_id in self._workers:
      logger.warn("Trying to add worker {} twice?".format(worker_id))
    else:
      self._workers[worker_id] = Worker(worker_id)
    return self._workers[worker_id]

  def clean_stop(self):
    """Notify that we want to terminate.

    The main zeromq/drmaa loop will shortly end, after this.
    """
    self._run = False

  def run(self):
    self._run = True
    self._context = zmq.Context()
    self._socket = self._context.socket(zmq.REP)
    self._socket.RCVTIMEO = 200
    while self._run:
      try:
        req = self._socket.recv()
        if req.startswith(b"HELO IAM"):
          worker = req[len(b"HELO IAM "):].decode("utf-8")
          logger.info("Got handshake from worker %s", worker)
          self._worker_handshake(worker)
        elif req.startswith(b"IZ BORED"):
          worker = req[len(b"IZ BORED "):].decode("utf-8")
          logger.debug("Got request for task from {}".format(worker))
          # Get a task for this to do
          task = self._get_next_task()
          if task is None:
            self._socket.send(b"PLZ WAIT")
            self._worker_waiting(worker)
            logger.debug("... no tasks for {}, asking to wait".format(worker))
          else:
            self._socket.send(b"PLZ DO" + task.data)
            self._worker_given_task(self._workers[worker], task)
            logger.debug("Worker {} given task {}".format(worker, task.id))
        elif req.startswith(b"YAY"):
          self._complete_task(req[4:])
        elif req.startswith(b"ONO"):
          self._fail_task(req[4:])
        elif req.startswith(b"IGIVEUP"):
          worker = req[len(b"IGIVEUP "):].decode("utf-8")
          self._socket.send(b"BYE")
          logger.debug("Worker {} ended".format(worker))
          self._worker_quitting(worker)
        else:
          logger.error(
              "Got unknown message from worker: %s", req.decode("latin-1"))
      except zmq.error.Again:
        # We hit a timeout. Just keep going
        pass
    # We're shutting down. Close the socket and context.
    logger.debug("Ending ZeroMQ socket and context")
    self._socket.close()
    self._context.term()

  def _worker_handshake(self, worker_id):
    """A Worker has said hello. Change it's state and make sure it's known."""
    if worker_id not in self._workers:
      logger.warn("Handshake from unregistered worker {}".format(worker_id))
      self.add_worker(worker_id)
    worker = self._workers[worker_id]
    # Register that we now have seen this worker
    worker.state_change(WorkerState.STARTED)
    worker.last_seen = time.time()

  def _worker_waiting(self, worker_id):
    """A worker is awaiting a new task"""
    if worker_id not in self._workers:
      logger.error(
          "Worker entering wait state, but unknown?! ({})".format(worker_id))
      self.add_worker(worker_id)
    worker = self._workers[worker_id]
    worker.state_change(WorkerState.WAITING)
    worker.last_seen = time.time()

  def _worker_given_task(self, worker, task):
    """A worker has been given a task

    :param Worker worker: The worker that was given the task
    :param Task task:     The task the worker was given
    """
    worker.state_change(WorkerState.RUNNING)
    worker.tasks.append(task)
    worker.last_seen = time.time()
    assert task.worker is None, "Trying to give duplicate tasks to workers"
    task.worker = worker

  def _worker_quitting(self, worker_id):
    """A worker has notified us that it is quitting."""
    worker = self._workers[worker_id]
    worker.state_change(WorkerState.ENDED)
    worker.last_seen = time.time()

  def _get_next_task(self):
    """Look at queues and cancellations to get the next task item.

    :returns: A Task whose Future state has been set to running
    """
    while True:
      try:
        task_id = self._work_queue.get(block=False)
      except queue.Empty:
        return None
      else:
        # Make sure this isn't cancelled and set as running
        task = self._tasks[task_id]
        if task.future.set_running_or_notify_cancel():
          # This task isn't cancelled, and has been set as running
          return self._tasks[task_id]

  def _complete_task(self, data):
    (task_id, result) = pickle.loads(data)
    task = self._tasks[task_id]
    worker = self._workers[task.worker_id]
    logger.debug("Worker {} succeeded in {}".format(worker.id, task.id))
    worker.state_change(WorkerState.TASKCOMPLETE)
    worker.last_seen = time.time()
    task.future.set_result(result)
    # Remove this work item from the info dictionary
    del self._tasks[task_id]

  def _fail_task(self, data):
    (task_id, exc_trace, exc_value) = pickle.loads(data)
    task = self._tasks[task_id]
    worker = task.worker
    logger.debug("Worker {} task failed in {}: {}".format(
        worker.id, task.id, exc_value))
    logger.debug("Stack trace: %s", exc_trace)
    worker.state_change(WorkerState.TASKCOMPLETE)
    worker.last_seen = time.time()
    task.future.set_exception(exc_value)

    # Clean up worker/task associations
    worker.tasks.remove(task)
    task.worker = None
    # Remove this task from the info dictionary
    del self._tasks[task_id]


class Pool(object):
  """Manage a pool of DRMAA workers"""

  def __init__(self):
    self._session = drmaa.Session()
    self._session.initialize()
    # Start a zeromq listener in a thread

  def launch_worker(self):
    pass

  # def start(self):
    # thread = threading.Thread(target=worker_routine, args=(url_worker, ))
    # thread.start()
