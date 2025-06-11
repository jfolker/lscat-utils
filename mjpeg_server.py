import contextlib
import mmap
import os
import time
import traceback

import asyncio
import sanic
from sanic import Sanic

#
# This MJPEG server is designed to support legacy AXIS IP cameras which are
# only browser-compatible in 2025 using MJPEG streaming mode and provide its
# video feeds within the mxcube3 and lsnode beamline control applications.
#
# Because Sanic forks child processes and does load-balancing for us, we don't
# need a reverse proxy. To grab port 443, "authbind --deep" is recommended.
#
# In order for video feeds to work properly, cameras must be configured to push
# images to a ramdisk via FTP with the "Use temporary file" option selected
# and write only 1 file ("write up to 1 images and start over").
#
# To test this service in action, embed a url to this server in an HTML <img>
# tag on another page.
#
# -Jory Folker, Jun 4 2025
#
port_str=os.getenv("LSCAT_SERVER_PORT", "8080")
if not port_str.isdigit():
    raise ValueError(f"The LSCAT_SERVER_PORT env var was set to {port_str}, which is not a valid TCP port number.")
PORT= 8080 if not port_str.isdigit() else int(port_str)
ADDR=os.getenv("LSCAT_SERVER_ADDR", "0.0.0.0")
TLS_CERT=os.getenv("LSCAT_TLS_CERT")
TLS_KEY=os.getenv("LSCAT_TLS_KEY")
CAM_DIR=os.getenv("LSCAT_MJPEG_DIR", "/ramcams")
FRAME_RATE=float(os.getenv("LSCAT_MJPEG_FRAMERATE", "10"))
FRAME_TIME=1.0/FRAME_RATE
workers_str=os.getenv("LSCAT_MJPEG_WORKERS", "16")
WORKERS= 8 if not workers_str.isdigit() else int(workers_str)
CSP_HEADER=os.getenv("LSCAT_CSP_HEADER", "frame-ancestors 'self' *.ls-cat.org ls-cat.org")

app = Sanic("lscat_mjpeg_server")

@app.on_response
async def add_csp_and_disable_caching(request, response):
    response.headers["cache-control"] = "no-cache, no-store, must-revalidate"
    response.headers["pragma"] = "no-cache"
    response.headers["connection"] = "keep-alive"
    response.headers["strict-transport-security"] = "max-age=31536000"
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
                # for MJPEG. This is a ramdisk file, so mmap() instead of
                # read() will avoid redundant copying. Note that we must
                # use memoryview instead of directly slicing the mmap
                # object to read the data without copying it.
                with open(fileName, "rb") as fh:
                    with contextlib.closing(mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ)) as mm:
                        await response.send(f"--lscat_mjpeg\r\ncontent-type: image/jpg\r\ncontent-length: {mm.size()}\r\n\r\n")
                        await response.send(memoryview(mm)[:])
                        await response.send(f"\r\n")
            except Exception as e:
                raise sanic.exceptions.ServerError

            # Sleep until we expect the next frame to be ready.
            # We don't sleep for less than 10ms because the time parameter
            # gets treated as a minimum. The OS scheduler could sleep for
            # longer than we wanted and cause us to drop frames if we're
            # already cutting it close.
            elapsed = float(time.perf_counter_ns() - start)/1000000000.0
            if elapsed > 0 and elapsed < (FRAME_TIME-0.01):
                await asyncio.sleep(FRAME_TIME - elapsed)
    
    response = await request.respond(content_type="multipart/x-mixed-replace; boundary=lscat_mjpeg")
    await mjpeg_streamer(response)

@app.route("/")
async def forbidden(request):
    raise sanic.exceptions.Forbidden
    
if __name__ == '__main__':
    tlsConf = None
    if TLS_CERT:
        tlsConf = {"cert": TLS_CERT, "key": TLS_KEY}
    
    os.chdir(CAM_DIR)
    try:
        if tlsConf:
            app.run(host=ADDR, port=PORT, ssl=tlsConf, workers=WORKERS)
        else:
            app.run(host=ADDR, port=PORT, workers=WORKERS)
    except KeyboardInterrupt:
        print("Received SIGINT, shutting down now")
    except Exception as e:
        print(traceback.format_exc())
        print(f"LS-CAT MJPEG server has terminated due to the following exception: {repr(e)}")
