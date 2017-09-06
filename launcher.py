#!/usr/bin/env python3
'''
Run PyBatsim Schedulers.

Usage:
    launcher.py <scheduler> [-o <options_string>] [options]

Options:
    -h --help                           Show this help message and exit.
    -v --verbose                        Be verbose.
    -p --protect                        Protect the scheduler using a validating machine.
    -s --socket-endpoint=<endpoint>     Batsim socket endpoint to use [default: tcp://*:28000]
    -o --options=<options_string>       A Json string to pass to the scheduler [default: {}]
    -t --timeout=<timeout>              How long to wait for responses from Batsim [default: 1000]
'''

from batsim.tools import launcher

if __name__ == "__main__":
    launcher.main()
