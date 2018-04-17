"""Main module."""

from concurrent.futures import Executor


class DRMAAPoolExecutor(Executor):
  """Control and submit to a pool of DRMAA-jobs."""

  def __init__(self, max_jobs=None):
    """Initialize a DRMAAPoolExecutor instance.

    :param int max_jobs: The maximum number of jobs to launch.
    """
    self._max_jobs = max_jobs
