#!/usr/bin/env python

"""
Get data from OpenStreetMaps
"""

from __future__ import absolute_import
from __future__ import print_function
import os
try:
    import httplib
    import urlparse
except ImportError:
    # python3
    import http.client as httplib
    import urllib.parse as urlparse

import optparse
import base64
from os import path

def readCompressed(time_out, conn, urlpath, query, filename):
    conn.request("POST", "/" + urlpath,
    "<osm-script timeout=\"" + str(time_out) + "\"" + " element-limit=\"1073741824\">" + """
    <union>
       %s
       <recurse type="node-relation" into="rels"/>
       <recurse type="node-way"/>
       <recurse type="way-relation"/>
    </union>
    <union>
       <item/>
       <recurse type="way-node"/>
    </union>
    <print mode="body"/>
    </osm-script>""" % query)
    response = conn.getresponse()
    print(response.status, response.reason)
    if response.status == 200:
        out = open(path.join(os.getcwd(), filename), "wb")
        out.write(response.read())
        out.close()
    elif response.status == 504:
        print("Consider increasing the time out using the option --time-out higher than the standard 600s")

optParser = optparse.OptionParser()
optParser.add_option("--osm-file", default="net.osm.xml", dest="osmFile", help="use input file from path.")
optParser.add_option("-b", "--bbox", help="bounding box to retrieve in geo coordinates west,south,east,north")
optParser.add_option("-t", "--tiles", type="int",
                     default=1, help="number of tiles the output gets split into")
optParser.add_option("-d", "--output-dir", help="optional output directory (must already exist)")
optParser.add_option("-a", "--area", type="int", help="area id to retrieve")
optParser.add_option("-o", "--time-out", type="int", default=600, help="time to wait for response. "
                                                          "Consider bigger numbers for bigger areas."
                                                          "Example 600 for Ustí nad Lab area")
optParser.add_option("-u", "--url", default="www.overpass-api.de/api/interpreter",
                     help="Download from the given OpenStreetMap server")
# alternatives: overpass.kumi.systems/api/interpreter, sumo.dlr.de/osm/api/interpreter


def get(args=None):
    (options, args) = optParser.parse_args(args=args)
    if not options.bbox and not options.area:
        optParser.error("At least one of 'bbox' and 'area' and 'polygon' has to be set.")
    if options.bbox:
        west, south, east, north = [float(v) for v in options.bbox.split(',')]
        if south > north or west > east:
            optParser.error("Invalid geocoordinates in bbox.")

    prefix = options.osmFile.split("osm.xml")[0][:-1]
    if options.output_dir:
        options.osmFile = path.join(options.output_dir, options.osmFile)

    if "http" in options.url:
        url = urlparse.urlparse(options.url)
    else:
        url = urlparse.urlparse("https://" + options.url)
    if os.environ.get("https_proxy") is not None:
        headers = {}
        proxy_url = urlparse.urlparse(os.environ.get("https_proxy"))
        if proxy_url.username and proxy_url.password:
            auth = '%s:%s' % (proxy_url.username, proxy_url.password)
            headers['Proxy-Authorization'] = 'Basic ' + base64.b64encode(auth)
        conn = httplib.HTTPSConnection(proxy_url.hostname, proxy_url.port)
        conn.set_tunnel(url.hostname, 443, headers)
    else:
        if url.scheme == "https":
            conn = httplib.HTTPSConnection(url.hostname, url.port)
        else:
            conn = httplib.HTTPConnection(url.hostname, url.port)

    if options.area:
        if options.area < 3600000000:
            options.area += 3600000000
        readCompressed(options.time_out, conn, url.path, '<area-query ref="%s"/>' %
                       options.area, options.osmFile)
    if options.bbox or options.polygon:
        if options.tiles == 1:
            readCompressed(options.time_out,conn, url.path, '<bbox-query n="%s" s="%s" w="%s" e="%s"/>' %
                           (north, south, west, east), options.osmFile)
        else:
            num = options.tiles
            b = west
            for i in range(num):
                e = b + (east - west) / float(num)
                readCompressed(options.time_out, conn, url.path, '<bbox-query n="%s" s="%s" w="%s" e="%s"/>' % (
                    north, south, b, e), "%s_%s_of_%s.osm.xml" % (prefix, i, num))
                b = e

    conn.close()


if __name__ == "__main__":
    get()
