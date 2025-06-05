import json
import os
import sys
import time
import traceback

import asyncio
import sanic
from sanic import Sanic

#
# This MJPEG server is designed to support legacy AXIS IP cameras which are
# only browser-compatible in 2025 using MJPEG streaming mode. Because it is
# written in Python, taking advantage of multicore processing is done by
# running multiple instances, each inside a Linux container or BSD jail with
# its own IP address.
#
# In order to bind to port 443 w/o running as root, I recommend keeping it
# simple and using authbind. This is a simple application that serves video
# frames and nothing else. No authn/authz, no DB queries, no beamline cmds.
# If excessive traffic is a problem, we can always add a check to the user's
# lsnode session cookie later and run behind a reverse proxy.
#
# In order for this code to work properly, cameras must be configured to push
# images to a ramdisk via FTP with the "Use temporary file" option selected
# and write only 1 file. This option in effect saves the entire image to its
# expected location atomically, allowing this script to keep its
# implementation simple. The tempfile used by AXIS cameras effectively a form
# of double-buffering.
#
# By default, this application is designed to adhere to directory naming
# conventions established long ago within the Life Sciences Collaborative
# Access Team (LS-CAT) by Keith Brister for interoperability with lsnode,
# the beamline control/data collection web application.
#
# -Jory Folker, Jun 4 2025
#
PORT=os.getenv("LSCAT_SERVER_PORT", 443)
TLS_CERT=os.getenv("LSCAT_TLS_CERT")
TLS_KEY=os.getenv("LSCAT_TLS_KEY")
CAM_DIR=os.getenv("LSCAT_MJPEG_DIR", "/ramcams")
FRAME_RATE=float(os.getenv("LSCAT_MJPEG_FRAMERATE", "10"))
FRAME_TIME=1.0/FRAME_RATE
CSP_HEADER=os.getenv("LSCAT_CSP_HEADER", "frame-ancestors 'self' *.ls-cat.org ls-cat.org")

app = Sanic("lscat_mjpeg_server")

@app.on_response
async def add_csp_and_disable_caching(request, response):
    response.headers["cache-control"] = "no-cache, no-store, must-revalidate"
    response.headers["pragma"] = "no-cache"
    response.headers["connection"] = "keep-alive"
    response.headers["content-security-policy"] = CSP_HEADER
    
@app.get("/<feed:str>")
async def mjpeg_server(request, feed=""):
    feedSubdir = feed
    # Check if the feed is using a subdir. We are bound to maintain
    # backward compatibility with lsnode's postgres layer where Keith
    # has ~500 functions that lsnode uses to look up things like
    # locations of camera feeds and crystallographic datasets.
    d2 = os.path.join(feed, f"{feed}-1")
    if os.path.exists(os.path.join(d2, f"image00001.jpg")):
        feedSubdir = d2
    elif not os.path.exists(os.path.join(feedSubdir, f"image00001.jpg")):
        raise sanic.exceptions.NotFound(f"cannot find images for feed {feed}")
    
    async def mjpeg_streamer(response):
        fileName = os.path.join(feedSubdir, f"image00001.jpg")
        
        # In MJPEG, the response never ends. We keep sending frames until
        # the client closes the connection.
        while True:
            start = time.perf_counter_ns()
            try:
                # Send a frame with the expected preamble and postamble
                # for MJPEG.
                finfo = os.stat(fileName)
                with open(fileName, "rb") as fh:
                    await response.send(f"--lscat_mjpeg\r\ncontent-type: image/jpg\r\ncontent-length: {finfo.st_size}\r\n\r\n")
                    await response.send(fh.read())
                    await response.send(f"\r\n")
            except Exception as e:
                raise sanic.exceptions.ServerError

            # Sleep until we expect the next frame to be ready. Under load,
            # the worst thing that could theoretically happen here is that
            # we take >90ms to send a frame and we eventually skip a frame.
            # We don't sleep for less than 10ms because the time parameter
            # gets treated as a minimum. The OS scheduler could sleep for
            # longer than we wanted and cause us to drop frames if we're
            # already cutting it close.
            elapsed = float(time.perf_counter_ns() - start)/1000000000.0
            if elapsed > 0 and elapsed < (FRAME_TIME-0.01):
                await asyncio.sleep(FRAME_TIME - elapsed)

    # This call to the Sanic API sends the obligatory response line and
    # sets headers for us using our special add_csp_and_disable_caching
    # decorator. Then, we propagate the response object built by Sanic
    # into our mjpeg_streamer callback and use it to feed data back into
    # Sanic, which does the work of sending data to many clients in a
    # non-blocking manner. Everywhere you see an "await", there is an
    # opportunity for another thread or process managed by Sanic to do
    # work while an HTTP session is blocked on I/O.
    response = await request.respond(content_type="multipart/x-mixed-replace; boundary=lscat_mjpeg")
    await mjpeg_streamer(response)

@app.route("/")
async def forbidden(request):
    raise sanic.exceptions.Forbidden
    
if __name__ == '__main__':
    tlsConf = None
    if TLS_CERT_FILE:
        tlsConf = {
            "cert": TLS_CERT_FILE
            "key" : TLS_KEY_FILE
        }
    
    os.chdir(CAM_DIR)
    try:
        if tlsConf:
            app.run(host="0.0.0.0", port=LSCAT_PORT, ssl=tlsConf)
        else:
            app.run(host="0.0.0.0", port=LSCAT_PORT)
    except KeyboardInterrupt:
        print("Received SIGINT, shutting down now")
    except Exception as e:
        print(traceback.format_exc())
        print(f"LS-CAT MJPEG server has terminated due to the following exception: {repr(e)}")
