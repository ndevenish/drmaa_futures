"""
Entry point for jobs run on the cluster

When run as an entrypoint, the master will be attempted to be contacted
and asked for a list of tasks.

Usage: python -mdrmaa_futures slave <server_and_port>
"""

from argparse import ArgumentParser
import sys

import logging
# If __main__, we are the root logger
logger = logging.getLogger()

if __name__ == "__main__":
  # intention: drmaa_futures [-v|(-h | --help)] slave [(-h|--help)] <url> <id>
  parser = ArgumentParser(prog="python -mdrmaa_futures")
  parser.add_argument(
      '--verbose', '-v', action='count', default=0, help="Show debug messages")
  subparsers = parser.add_subparsers(help='Mode to run in', dest='command')
  parser_slave = subparsers.add_parser("slave", help="Run a slave node")
  parser_slave.add_argument("url", type=str)
  parser_slave.add_argument("id", type=str)
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

  logger.info("Starting slave node with master {}".format(args.url))
  from drmaa_futures.slave import run_slave
  run_slave(args.url, args.id, timeout=args.timeout)
