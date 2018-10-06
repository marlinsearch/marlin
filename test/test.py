# TODO : SPLIT THIS INTO MULTIPLE FILES
import sys
import json
import urllib2
import datetime
import unittest
import time
import timeit
import ssl
import os
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

print dir(ssl)
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

url = "http://localhost:9002/1/"
index = "indexes/test"
index_url = url + index 
query_url = url + index + "/query"
cfg_url = url + index + "/settings"
key_url = url + "keys"


def start_marlin():
    print "starting ..."
    os.system("cd .. && ./build-debug/main/marlin &")
    time.sleep(3)

def stop_marlin():
    os.system("killall marlin")
    time.sleep(5)

class MyHTTPSHandler(urllib2.HTTPSHandler, urllib2.HTTPDefaultErrorHandler):
    def http_error_default(self, req, rsp, code, msg, hdrs):
        return rsp 

class TestBase(unittest.TestCase):
    def __init__(self, x):
        unittest.TestCase.__init__(self, x)
        urllib2.install_opener(urllib2.build_opener(MyHTTPSHandler(context=ctx)))
        requests.packages.urllib3.disable_warnings()

    # TODO: Global load and clear index for each testsuite
    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        self.startTime = time.time()
        requests.packages.urllib3.disable_warnings()

    def tearDown(self):
        t = time.time() - self.startTime
        print "%s: %.4fs" % (self.id(), t)

class TestPing(TestBase):
    def test_a_ping(self):
        ping_url = url[0:len(url)-2]
        r = requests.get(ping_url + 'ping')
        self.assertEqual(r.status_code, 200)

if __name__ == '__main__':
    print "Starting tests .."
    requests.packages.urllib3.disable_warnings()
    live = (len(sys.argv) > 1) and sys.argv[1] == 'live'
    if not live:
        stop_marlin()
        start_marlin()
 

    fail = 0
    # Ping test
    suite = unittest.TestLoader().loadTestsFromTestCase(TestPing)
    r = unittest.TextTestRunner(verbosity=5).run(suite)
    fail = fail + len(r.failures)

    if not live:
        stop_marlin()
    if fail != 0:
        print "\n************** FAILED ", fail, " TESTS *******************\n"
    sys.exit(fail)

