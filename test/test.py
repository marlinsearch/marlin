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
#from requests.packages.urllib3.exceptions import InsecureRequestWarning

#print dir(ssl)
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

url = "http://localhost:9002/1/"
index = "indexes/test"
index_url = url + index 
query_url = url + index + "/query"
cfg_url = url + index + "/settings"
key_url = url + "keys"
master_app_id = "abcdefgh"
master_api_key = "12345678901234567890123456789012"
test_app_id = "aaaaaaaa"


def start_marlin():
    print "starting ..."
    os.system("cd .. && ./build-debug/main/marlin &")
    time.sleep(5)

def stop_marlin():
    os.system("killall marlin")
    time.sleep(5)

class MyHTTPSHandler(urllib2.HTTPSHandler, urllib2.HTTPDefaultErrorHandler):
    def http_error_default(self, req, rsp, code, msg, hdrs):
        return rsp 

class TestBase(unittest.TestCase):
    def __init__(self, x):
        unittest.TestCase.__init__(self, x)
#        urllib2.install_opener(urllib2.build_opener(MyHTTPSHandler(context=ctx)))
#        requests.packages.urllib3.disable_warnings()

    # TODO: Global load and clear index for each testsuite
    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        self.startTime = time.time()
#        requests.packages.urllib3.disable_warnings()

    def tearDown(self):
        t = time.time() - self.startTime
        print "%s: %.4fs" % (self.id(), t)

    def use_app_key(self, appId, apiKey):
        self.app_id = appId
        self.api_key = apiKey

    def get_headers(self):
        headers = { 'X-Marlin-Application-Id': self.app_id, 
                    'X-Marlin-REST-API-KEY': self.api_key, 
                    'Content-Type':'application/json'}
        return headers

    def post_any_status(self, url, data):
        headers = self.get_headers()
        r = requests.post(url, data=json.dumps(data), headers=headers, verify=False)
        response = r.json()
        return response

    def post(self, url, data, statuscode=200):
        headers = self.get_headers()
        r = requests.post(url, data=json.dumps(data), headers=headers, verify=False)
        self.assertEqual(r.status_code, statuscode)
        response = r.json()
        return response

    def delete(self, url, statuscode=200):
        headers = self.get_headers()
        r = requests.delete(url, headers=headers, verify=False)
        self.assertEqual(r.status_code, statuscode)
        response = r.json()
        return response

    def put(self, purl, pd, statuscode=200):
        headers = self.get_headers()
        r = requests.put(purl, data=json.dumps(pd), headers=headers, verify=False)
        self.assertEqual(r.status_code, statuscode)
        response = r.json()
        return response

    def patch(self, purl, pd, statuscode=200):
        headers = self.get_headers()
        r = requests.patch(purl, data=json.dumps(pd), headers=headers, verify=False)
        self.assertEqual(r.status_code, statuscode)
        response = r.json()
        return response

    def get(self, purl, statuscode=200):
        headers = self.get_headers()
        r = requests.get(purl, headers=headers, verify=False)
        self.assertEqual(r.status_code, statuscode)
        response = r.json()
        return response

    def setup_test_app(self):
        self.use_app_key(master_app_id, master_api_key)
        apps_url = url + 'applications'
        app = {'name' : 'appfortests', 'appId': 'aaaaaaaa',
                'apiKey': '12345678901234567890123456789012'}
        r = self.post_any_status(apps_url, app)

    def delete_test_app(self):
        self.use_app_key(master_app_id, master_api_key)
        apps_url = url + 'applications'
        test_app_url = apps_url + '/appfortests'
        r = self.delete(test_app_url)

class TestPing(TestBase):
    def test_a_ping(self):
        ping_url = url[0:len(url)-2]
        r = requests.get(ping_url + 'ping')
        self.assertEqual(r.status_code, 200)

class TestApp(TestBase):
    def setUp(self):
        self.use_app_key(master_app_id, master_api_key)
        super(TestApp, self).setUp()

    def test_a_marlin(self):
        marlin_url = url + 'marlin'
        r = self.get(marlin_url)
        self.assertTrue('version' in r)
        # Try a request without appId, apiKey in header
        r = requests.get(marlin_url)
        self.assertEqual(r.status_code, 400)
        r = requests.get(marlin_url + 
                '?x-marlin-application-id=%s&x-marlin-rest-api-key=%s'%(self.app_id, self.api_key)) 
        self.assertEqual(r.status_code, 200)
        self.assertTrue('version' in r.json())
        r = requests.get(marlin_url + 
                '?x-marlin-rest-api-key=%s&x-marlin-application-id=%s'%(self.api_key, self.app_id)) 
        self.assertEqual(r.status_code, 200)
        self.assertTrue('version' in r.json())
        r = requests.get(marlin_url + 
                '?x-marlin-rest-api-key=%s&x-marlin-application-id=%s'%(self.app_id, self.api_key)) 
        self.assertEqual(r.status_code, 400)
 
    def test_b_invalid_app_key(self):
        self.use_app_key('lkjaslkdfj', master_api_key)
        marlin_url = url + 'marlin'
        r = self.get(marlin_url, 400)
        self.use_app_key(master_app_id, 'asdfasdflkjwlerjljwer')
        marlin_url = url + 'marlin'
        r = self.get(marlin_url, 400)

    def test_c_get_app_list(self):
        apps_url = url + 'applications'
        r = self.get(apps_url)
        self.assertTrue(isinstance(r, list))

    def test_d_app_create_delete(self):
        # create a new app called testapp
        apps_url = url + 'applications'
        app = {'name' : 'testapp'}
        r = self.post(apps_url, app)
        self.assertTrue('name' in r)
        self.assertTrue('appId' in r)
        self.assertTrue('apiKey' in r)
        # try creating with same app name
        self.post(apps_url, app, 400)
        # try creating with same app id
        app['name'] = 'test2app'
        app['appId'] = r['appId']
        self.post(apps_url, app, 400)
        # try to get app
        testapp_url = apps_url + '/testapp'
        r2 = self.get(testapp_url)
        self.assertTrue(r['name'] == r2['name'])
        self.assertTrue(r['apiKey'] == r2['apiKey'])
        self.assertTrue(r['appId'] == r2['appId'])
        # delete app
        r = self.delete(testapp_url)
        r2 = self.get(testapp_url, 403)

class TestIndex(TestBase):
    def setUp(self):
        # Create the test app and use test app key
        self.setup_test_app()
        self.use_app_key(test_app_id, master_api_key)
        super(TestIndex, self).setUp()

    def test_a_create_index(self):
        indexes_url = url + 'indexes'
        index = {'name' : 'testindex'}
        r = self.post(indexes_url, index)
        r = self.get(indexes_url)
        # make sure we get a list of indexes
        self.assertTrue(isinstance(r, list))
        # See if the index we added now is present
        present = False
        for index in r:
            if index['name'] == 'testindex':
                present = True
        self.assertTrue(present)
        self.delete_test_app()

    def test_a_create_multishard_index(self):
        indexes_url = url + 'indexes'
        index = {'name' : 'testindex', 'numShards': 5}
        r = self.post(indexes_url, index)
        r = self.get(indexes_url)
        # make sure we get a list of indexes
        self.assertTrue(isinstance(r, list))
        # See if the index we added now is present
        present = False
        for index in r:
            if index['name'] == 'testindex':
                present = True
        self.assertTrue(present)
        self.delete_test_app()

        # TODO: Add test to delete an index next

if __name__ == '__main__':
    print "Starting tests .."
    live = (len(sys.argv) > 1) and sys.argv[1] == 'live'
    if not live:
        stop_marlin()
        start_marlin()
 

    fail = 0

    # Ping test
    suite = unittest.TestLoader().loadTestsFromTestCase(TestPing)
    r = unittest.TextTestRunner(verbosity=5).run(suite)
    fail = fail + len(r.failures)

    # App tests
    suite = unittest.TestLoader().loadTestsFromTestCase(TestApp)
    r = unittest.TextTestRunner(verbosity=5).run(suite)
    fail = fail + len(r.failures)

    # Index tests
    suite = unittest.TestLoader().loadTestsFromTestCase(TestIndex)
    r = unittest.TextTestRunner(verbosity=5).run(suite)
    fail = fail + len(r.failures)


    if not live:
        stop_marlin()
    if fail != 0:
        print "\n************** FAILED ", fail, " TESTS *******************\n"
    sys.exit(fail)

