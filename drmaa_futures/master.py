# coding: utf-8
"""
Contains the master control for ZeroMQ communication.
"""

from concurrent.futures import Future
from enum import Enum
import logging
import socket
import time

import dill as pickle
from six.moves import queue, urllib
import zmq

logger = logging.getLogger(__name__)


class Task(object):
  """Represents a task for workers to do"""

  def __init__(self, task_id, function, *args, **kwargs):
    """Initialise a Task

    :param Callable function: The function to run on the remote host
    :param Iterable args:     Positional arguments to pass to the function
    :param dict kwargs:       Keyword arguments to pass to the function
    :param taskid:            An identifier for the new task
    """
    self._id = task_id
    self.future = Future()
    self.worker = None
    # Serialize this function now, to preserve anything the user might
    # have passed in and alter later
    self.data = pickle.dumps((self._id, lambda: function(*args, **kwargs)))

  @property
  def id(self):
    """Get the task ID (read-only)"""
    return self._id


class WorkerState(Enum):
  """States to track the worker lifecycle"""
  UNKNOWN = 1
  STARTED = 2
  WAITING = 3
  RUNNING = 4
  TASKCOMPLETE = 5
  ENDED = 6


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
      logger.warning("Invalid state transition for worker {}: {} → {}".format(
          self.id, self.state, new))
    self.state = new


def bind_to_endpoint(zsocket, endpoint=None):
  """Bind a zeromq socket to an endpoint, specified or random TCP.

  :param str endpoint: The endpoint to bind to. If None, a free-port TCP
                       endpoint will be automatically generated.
  :returns:         The endpoint to send to clients. Same is input, if not None
  :rtype: str
  """
  # Do we need to decide on an endpoint ourselves?
  if endpoint is None:
    # Bind to a random tcp port, then work out the endpoint to connect to
    endpoint = "tcp://*:0"
    logger.debug("Binding socket to %s", endpoint)
    zsocket.bind("tcp://*:0")
    # socket.getfqdn()
    bound_to = urllib.parse.urlparse(zsocket.LAST_ENDPOINT)
    return "tcp://{}:{}".format(socket.getfqdn(), bound_to.port)
  # Trust that the user knows how to connect to this custom endpoint
  logger.debug("Binding socket to %s", endpoint)
  zsocket.bind(endpoint)
  return endpoint


class ZeroMQListener(object):
  """Handle the zeromq/worker loop"""

  def __init__(self, endpoint=None):
    """Initialize the zeroMQ listener.

    :param str endpoint: The endpoint to bind to. If unspecified, then
                         a tcp connection will be created on an arbitray
                         port. The address to connect to will be available
                         on the `endpoint` parameter.
    """
    self._work_queue = queue.Queue()
    self._tasks = {}
    self._workers = {}
    self._task_count = 0  # Counter for unique task ID
    # Set up zeromq
    self._context = zmq.Context()
    self._socket = self._context.socket(zmq.REP)
    self._socket.RCVTIMEO = 200
    logger.debug("Binding zeroMQ socket")
    self.endpoint = bind_to_endpoint(self._socket, endpoint)

  @property
  def active_workers(self):
    """Gives a count of the number of currently known active workers"""
    return len(
        [x for x in self._workers.values() if x.state != WorkerState.ENDED])

  def __exit__(self, exc_type, exc_value, traceback):
    """Ensure we shutdown properly when leaving as a context."""
    self.shutdown()

  def __enter__(self):
    """Enter a context"""
    return self

  def enqueue_task(self, func, *args, **kwargs):
    """Add a task to the queue of items.

    :param Callable func: The function to call in the task
    :param Iterable args: The positional arguments to pass to the function
    :param dict kwargs:   The keyword arguments to pass to the function
    :returns: A Future tied to the work item
    """
    taskid = self._task_count
    self._task_count += 1
    # Create the task item
    task = Task(taskid, func, *args, **kwargs)
    self._tasks[taskid] = task
    # Once added to the queue, only the update thread may touch it
    self._work_queue.put(taskid)
    return task.future

  def _add_worker(self, worker_id):
    """Register a worker to the manager."""
    assert worker_id not in self._workers
    self._workers[worker_id] = Worker(worker_id)
    return self._workers[worker_id]

  def process_messages(self):
    """Process any messages that might have arrived."""
    try:
      req = self._socket.recv()
      self._socket.send(self._process_request(req))
    except zmq.error.Again:
      # We hit a timeout. Just keep going
      pass

  def shutdown(self):
    """Shut down the ZeroMQ connection"""
    if self._socket or self._context:
      # We're shutting down. Close the socket and context.
      logger.debug("Ending ZeroMQ socket and context")
      self._socket.close()
      self._context.term()
      self._socket = None
      self._context = None

  def _process_request(self, request):
    """Processes a request from a worker.

    :returns:   The message to send back to the worker
    :rtype byte:
    """

    # The first time we encounter a worker it's not known
    def decode(data):
      """Decode the message data as a string"""
      return data.decode("utf-8")

    # Once past handshaking, we already have a worker
    def decode_worker(data):
      """Decode the message data as a worker ID"""
      return self._workers[data.decode("utf-8")]

    def decode_pickle(data):
      """Decode data as a pickle blob"""
      logger.debug("Recieved %d byte result", len(data))
      return pickle.loads(data)

    # Table of possible message beginnings, the functions to decode any
    # attached data, and the functions to then handle the request
    potential_messages = {
        b"HELO IAM": (decode, self._worker_handshake),
        b"IZ BORED": (decode_worker, self._worker_waiting),
        b"YAY": (decode_pickle, self._complete_task),
        b"ONO": (decode_pickle, self._fail_task),
        b"IGIVEUP": (decode_worker, self._worker_quitting)
    }
    # Find the message in the table
    for message, (processor, function) in potential_messages.items():
      if request.startswith(message):
        logger.debug("Recieved %s message", message)
        data = processor(request[len(message) + 1:])
        return function(data)
    raise RuntimeError("Could not match message {}".format(request[:10]))

  def _worker_handshake(self, worker_id):
    """A Worker has said hello. Change it's state and make sure it's known."""
    logger.info("Got handshake from worker %s", worker_id)
    assert worker_id not in self._workers
    self._add_worker(worker_id)
    worker = self._workers[worker_id]
    # Register that we now have seen this worker
    worker.state_change(WorkerState.STARTED)
    worker.last_seen = time.time()
    return b"HAY"

  def _worker_waiting(self, worker):
    """A worker is awaiting a new task"""
    logger.debug("Got request for task from %s", worker.id)
    worker.state_change(WorkerState.WAITING)
    worker.last_seen = time.time()
    # Find a task for the worker
    task = self._get_next_task()
    if task is None:
      return b"PLZ WAIT"

    worker.state_change(WorkerState.RUNNING)
    worker.tasks.add(task)
    assert task.worker is None, "Attempting to give out duplicate tasks"
    task.worker = worker
    logger.debug("Giving worker %s task %s (%d bytes)", worker.id, task.id,
                 len(task.data))
    return b"PLZ DO " + task.data

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
        else:
          logger.debug("Task %s was cancelled before running", task.id)

  def _complete_task(self, data):
    """A worker sent us a message with a successful task."""
    (task_id, result) = data
    task = self._tasks[task_id]
    worker = task.worker
    logger.debug("Worker {} succeeded in {}".format(worker.id, task.id))
    worker.state_change(WorkerState.TASKCOMPLETE)
    worker.last_seen = time.time()
    task.future.set_result(result)
    # Clean up the worker/task
    assert task.worker is worker
    worker.tasks.remove(task)
    task.worker = None
    del self._tasks[task_id]
    self._work_queue.task_done()
    return b"THX"

  def _fail_task(self, data):
    """A worker sent us a message with a failed task."""
    (task_id, exc_trace, exc_value) = data
    task = self._tasks[task_id]
    worker = task.worker
    logger.debug("Worker {} task {} failed: {}".format(worker.id, task.id,
                                                       exc_value))
    logger.debug("Stack trace: %s", exc_trace)
    worker.state_change(WorkerState.TASKCOMPLETE)
    worker.last_seen = time.time()
    task.future.set_exception(exc_value)
    # Clean up the worker/task
    assert task.worker is worker
    worker.tasks.remove(task)
    task.worker = None
    del self._tasks[task_id]
    self._work_queue.task_done()
    return b"THX"

  @staticmethod
  def _worker_quitting(worker):
    """A worker has notified us that it is quitting."""
    logger.debug("Worker %s self-quitting", worker.id)
    worker.state_change(WorkerState.ENDED)
    worker.last_seen = time.time()
    return b"BYE"

  # def start(self):
  # thread = threading.Thread(target=worker_routine, args=(url_worker, ))
  # thread.start()
  # jt = pool.session.createJobTemplate()
  # jt.remoteCommand = sys.executable

  # # Build a copy of environ with a backed up LD_LIBRARY_PATH - SGE
  # # disallows passing of this path through but we probably need it
  # env = dict(os.environ)
  # env["_LD_LIBRARY_PATH"] = env.get("LD_LIBRARY_PATH", "")
  # jt.jobEnvironment = env

  # # If we need to pass a timeout parameter
  # timeoutl = [] if timeout is None else ["--timeout={}".format(timeout)]
  # # Work out a unique worker_if
  # worker_id = pool.get_new_worker_id()

  # # jt.args = ["-mdrmaa_futures", "-v", "slave"
  # #            ] + timeoutl + [host_url, _worker_id]
