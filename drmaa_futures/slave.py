# coding: utf-8
"""
Running a slave instance.
"""

import logging
import sys
import time
import traceback

import dill as pickle
import zmq

logger = logging.getLogger(__name__)


class UnpickleableError(Exception):
  """Represent an error attempting to pickle the result of a task"""


class TaskSystemExit(Exception):
  """For when the task raised a SystemExit exception, trying to quit"""


def do_task(data):
  """Do a task, as specified in a pickle bundle.

  :arg byte data: The pickle-data to load
  :returns: Pickle data of the result, or an exception
  """
  (task_id, task_function) = pickle.loads(data)
  try:
    logger.debug("Running task with ID {}".format(task_id))
    # Run whatever task we've been given
    result = task_function()
    logger.debug("Completed task")
    # An error pickling here counts as a job failure
    return b"YAY " + pickle.dumps((task_id, result))
  except KeyboardInterrupt:
    # This is interactive so we want to let it float
    raise
  except BaseException:
    logger.debug("Exception processing task")
    # Everything else: We want to pass back across the network
    (_, exc_value, exc_trace) = sys.exc_info()
    exc_trace = traceback.format_tb(exc_trace)
    # We don't want to propagate a SystemExit to the other side
    if isinstance(exc_value, SystemExit):
      exc_value = TaskSystemExit()
    # Be careful - we might not be able to pickle the exception?? Go to lengths
    # to make sure that we pass something sensible back
    try:
      pickle.dumps(exc_value)
    except pickle.PicklingError:
      exc_value = UnpickleableError("{}: {}".format(
          str(type(exc_value)), str(exc_value)))
    return b"ONO " + pickle.dumps((task_id, exc_trace, exc_value))


def run_slave(server_url, worker_id, timeout=30):
  """Run a slave instance and connect it to a specific master URL.
  :param str server_url: The server string to use to connect
  :param str worker_if:  The worker ID to use when communicating
  :param timeout: The time (in seconds) to wait with no jobs before terminating
  """
  try:
    logger.debug("Running slave {} connect to {}".format(
        worker_id, server_url))
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.RCVTIMEO = int(1000 * timeout)
    logger.debug("Connecting")
    socket.connect(server_url)
    logger.debug("Sending hello")
    socket.send(b"HELO IAM " + worker_id.encode("utf-8"))
    logger.debug("Awaiting confirmation of hello recieved")
    assert socket.recv() == b"HAY"
    logger.debug(
        "Got hello. Going into task loop with timeout {}s".format(timeout))
  except zmq.error.Again:
    logger.debug("Timed out waiting for handshake.")
    sys.exit(1)
  else:
    # If waiting for the whole timeout, then stop waiting
    last_job = time.time()
    while time.time() - last_job < timeout:
      logger.debug("Asking for a task")
      socket.send("IZ BORED {}".format(worker_id).encode("UTF-8"))
      reply = socket.recv()
      # We get a command returned
      assert reply.startswith(b"PLZ")
      if reply == b"PLZ WAIT":
        logger.debug("No tasks available. Trying again in a few seconds.")
        time.sleep(min(timeout / 2.0, 5))
      elif reply == b"PLZ GOWAY":
        logger.debug("Got quit signal. ending main loop.")
        break
      elif reply.startswith(b"PLZ DO"):
        result = do_task(reply[len(b"PLZ DO "):])
        logger.debug("Sending result")
        socket.send(result)
        # Await the ok
        assert socket.recv() == b"THX"
        last_job = time.time()
    if time.time() - last_job >= timeout:
      socket.send(b"IGIVEUP " + worker_id.encode("utf-8"))
      socket.recv()
      logger.debug("Timed out while waiting for tasks")
  finally:
    logger.debug("Closing socket")
    socket.close()
    logger.debug("Closing context")
    context.term()
  logger.debug("Slave completed.")


# Messaging protocol:
# Sent                    Recieved      Action
# ----------------------- ------------- ----------------------------------
# HELO IAM {id}           HAY           Negotiation success
# IZ BORED {id}           PLZ GOWAY     Exit
#                         PLZ WAIT      Nothing to do; try again soon
#                         PLZ DO {task} Hand off task to runner
# YAY {result}            THX           Task succeeded with result data
# ONO {result}            THX           Task failed - with exception data
# IGIVEUP {id}            BYE           Quitting; given up with processing
