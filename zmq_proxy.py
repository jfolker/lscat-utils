#!/usr/bin/python3

#
# This script is a general-purpose proxy of the simplest variety. ZMQ does our
# load-balancing for us under the hood. At LS-CAT, we use this as a proxy
# between our old remote beamline interface server (lsnode.ls-cat.org) and a set
# of servers running a bespoke C program written by Keith Brister for rendering
# JPEG images from datasets produced by MarCCD and Dectris Eiger X-ray
# detectors.
#
# This script is designed to be run only on internal networks, hence we do not
# use TLS: the network is assumed secure.
#
# We don't check our status reset ourselves because this is designed
# to run as a daemon (e.g. at LS-CAT, a systemd service). When the connections
# are broken or an error occurs, systemd restarts us w/ appropriate constraints
# for repeated failure.
#
# -Jory Folker, May 2025
#

import os
import zmq

routerAddr = os.environ.get("ZMQ_PROXY_ROUTER", "ipc://@isv2proxy")
dealerAddr = os.environ.get("ZMQ_PROXY_DEALER", "tcp://10.1.253.10:60202")

print(f"NOTICE: zmq proxy {routerAddr} --> {dealerAddr} starting up")
ctx = zmq.Context()
frontend = ctx.socket(zmq.ROUTER)
frontend.bind(routerAddr)
backend = ctx.socket(zmq.DEALER)
backend.bind(dealerAddr)
zmq.proxy(frontend, backend)
print(f"NOTICE: zmq proxy {routerAddr} --> {dealerAddr} terminating")
