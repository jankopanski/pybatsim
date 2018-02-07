'''
Run PyBatsim Schedulers.

Usage:
    pybatsim <scheduler> [-o <options_string>] [options]

Options:
    -h --help                               Show this help message and exit.
    -v --verbose                            Be verbose.
    -p --protect                            Protect the scheduler using a validating machine.
    -s --socket-endpoint=<endpoint>         Batsim socket endpoint to use [default: tcp://*:28000]
    -e --event-socket-endpoint=<endpoint>   Socket endpoint to use to publish scheduler events [default: tcp://*:29000]
    -o --options=<options_string>           A Json string to pass to the scheduler [default: {}]
    -O --options-file=<options_file>        A file containing the json options
    -t --timeout=<timeout>                  How long to wait for responses from Batsim [default: 2000]
'''

import sys
import json

from docopt import docopt

from batsim.tools.launcher import launch_scheduler, instanciate_scheduler


def main():
    arguments = docopt(__doc__, version='2.0')

    if arguments['--verbose']:
        verbose = 999
    else:
        verbose = 0

    timeout = int(arguments['--timeout'] or float("inf"))

    protect = bool(arguments['--protect'])

    if arguments["--options-file"]:
        with open(arguments["--options-file"]) as options_file:
            options = json.load(options_file)
    elif arguments["--options"]:
        options = json.loads(arguments['--options'])
    else:
        options = {}

    scheduler_filename = arguments['<scheduler>']
    socket_endpoint = arguments['--socket-endpoint']
    event_socket_endpoint = arguments['--event-socket-endpoint']

    scheduler = instanciate_scheduler(scheduler_filename, options=options)

    return launch_scheduler(scheduler,
                            socket_endpoint,
                            event_socket_endpoint,
                            options,
                            timeout,
                            protect,
                            verbose)


if __name__ == "__main__":
    sys.exit(main())
