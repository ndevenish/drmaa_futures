# coding: utf-8
"""
Pool to manage drmaa workers
"""
import os
import sys

import drmaa

class Job(object):
  """Track information about cluster jobs"""
  def __init__(self, jobid):
    self.id = jobid

class Pool(object):
  """Manage a pool of DRMAA workers"""

  def __init__(self, endpoint, debug=False):
    """Initialize a pool controller"""
    self._session = drmaa.Session()
    self._session.initialize()
    # Create a persistent template for launching workers
    self._template = self._session.createJobTemplate()
    self._template.remoteCommand = sys.executable
    # Since some (all?) clusters suppress LD_LIBRARY_PATH, we need to rename
    env_copy = dict(os.environ)
    env_copy["_LD_LIBRARY_PATH"] = env_copy.get("LD_LIBRARY_PATH", "")
    self._template.jobEnvironment = env_copy
    self._template.args = ["-mdrmaa_futures", "-v", "slave", endpoint]
    # If in debug mode, save logs for all the jobs
    if debug:
      self._template.outputPath = ":pool_$drmaa_incr_ph$.out"
      self._template.joinFiles = True
    # Keep track of active jobs
    self._jobs = []
    self._max_jobs = 1

  def shutdown(self):
    """Shutdown all pool workers and clean up resources"""
    self._session.deleteJobTemplate(self._template)

  def manage(self, task_count=None):
    """Undertake all background management activities for the pool.

    :param task_count: The number of incomplete tasks. Used to calculate
                       if new workers need to be created.
    """
    # Firstly, refresh the status of all the workers

  def launch_workers(self, count=1):
    """Explicitly launch new pool workers.

    :param int count: The number of new workers to launch.
    """
    self._session.runJob()
