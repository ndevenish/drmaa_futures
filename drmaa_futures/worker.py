# coding: utf-8

import os
import sys

import drmaa

class Pool(object):
  session = "DRMAA Session"
  def get_new_worker_id():
    return 0
  def start_new_worker():
    pass

class Worker(object):
  """Represents a worker job instance"""
  def __init__(self, job_id, session):
    self._session = session
    self._jobid = job_id

  @classmethod
  def launch(cls, pool, timeout=None):
    jt = pool.session.createJobTemplate()
    jt.remoteCommand = sys.executable

    # Build a copy of environ with a backed up LD_LIBRARY_PATH - SGE
    # disallows passing of this path through but we probably need it
    env = dict(os.environ)
    env["_LD_LIBRARY_PATH"] = env.get("LD_LIBRARY_PATH", "")
    jt.jobEnvironment = env

    # If we need to pass a timeout parameter
    timeoutl = [] if timeout is None else ["--timeout={}".format(timeout)]
    # Work out a unique worker_if
    worker_id = pool.get_new_worker_id()
    _worker_id += 1
   
    jt.args = ["-mdrmaa_futures", "-v", "slave"] + timeoutl + [host_url, _worker_id]
