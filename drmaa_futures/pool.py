# coding: utf-8
"""
Pool to manage drmaa workers
"""
import os
import sys

import drmaa


class Pool(object):
  """Manage a pool of DRMAA workers"""

  def __init__(self, endpoint):
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

  def shutdown(self):
    """Shutdown all pool workers and clean up resources"""
    self._session.deleteJobTemplate(self._template)

  def launch_worker(self):
    """Explicitly launch a new pool worker"""
