import drmaa

class Pool(object):
  """Manage a pool of DRMAA workers"""

  def __init__(self):
    self._session = drmaa.Session()
    self._session.initialize()
    # Create a persistent template for launching workers
    self._template = self._session.createJobTemplate()
    self._template.remoteCommand = sys.executable
    # Since some (all?) clusters suppress LD_LIBRARY_PATH, we need to rename
    env_copy = dict(os.environ)
    env_copy["_LD_LIBRARY_PATH"] = env_copy.get("LD_LIBRARY_PATH", "")
    self._template.jobEnvironment = env_copy
    jt.args = ["-mdrmaa_futures", "-v", "slave", host_url]

  def shutdown(self):
    self._session.deleteJobTemplate(self._template)

  def launch_worker(self):
    pass