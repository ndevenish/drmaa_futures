"""
Entry point for jobs run on the cluster

When run as an entrypoint, the master will be attempted to be contacted
and asked for a list of tasks.

Usage: python -mdrmaa_futures slave <server_and_port>
"""

from argparse import ArgumentParser
import sys
import os
import uuid

import logging
# If __main__, we are the root logger
logger = logging.getLogger()


def generate_slave_name():
  if "JOB_ID" in os.environ and "SGE_TASK_ID" in os.environ:
    # We appear to be running inside an SGE-style system
    task_id = os.environ["SGE_TASK_ID"]
    if task_id == "undefined":
      # SGE single job, no array.
      return os.environ["JOB_ID"]
    else:
      # We're evidently in an array job!
      return "{}.{}".format(os.environ["JOB_ID"], task_id)
  else:
    # Not sure what this is - use UUID4
    return uuid.uuid4().hex


def _run_main():
  # intention: drmaa_futures [-v|(-h | --help)] slave [(-h|--help)] <url> <id>
  parser = ArgumentParser(prog="python -mdrmaa_futures")
  parser.add_argument(
      '--verbose', '-v', action='count', default=0, help="Show debug messages")
  subparsers = parser.add_subparsers(help='Mode to run in', dest='command')
  parser_slave = subparsers.add_parser("slave", help="Run a slave node")
  parser_slave.add_argument("url", type=str)
  parser_slave.add_argument("id", type=str, nargs="?")
  parser_slave.add_argument(
      '--timeout',
      action='store',
      type=float,
      default=30,
      help="How long to wait for jobs")
  # parser_slave.add_argument("port", type=int)

  args = parser.parse_args()
  if args.command is None:
    parser.print_help()
    sys.exit(1)

  assert args.command == "slave"
  # Set up logging based on the verbosity we were given
  logging.basicConfig(
      level=logging.INFO if args.verbose == 0 else logging.DEBUG)

  # Generate a slave name automatically if not provided
  if args.id is None:
    logger.debug("No slave id: Generating")
    args.id = generate_slave_name()

  # Copy over the library_path
  library_path = os.environ.get("LD_LIBRARY_PATH", "").split(":")
  backup_path = os.environ.get("_LD_LIBRARY_PATH", "").split(":")
  if backup_path:
    logger.debug("Restoring backup LD_LIBRARY_PATH")
    new_path = backup_path + [x for x in library_path if x not in backup_path]
    os.environ["LD_LIBRARY_PATH"] = ":".join(new_path)

  logger.info("Starting slave node {} with master {}".format(
      args.id, args.url))
  from drmaa_futures.slave import run_slave
  run_slave(args.url, args.id, timeout=args.timeout)


if __name__ == "__main__":
  _run_main()
