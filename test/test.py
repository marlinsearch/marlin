# TODO : REWRITE this mess. SPLIT THIS INTO MULTIPLE FILES
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
indexes_url = url + 'indexes'
test_index_name = 'testindex'
master_app_id = "abcdefgh"
master_api_key = "12345678901234567890123456789012"
test_app_id = "aaaaaaaa"
test_index_url = indexes_url + '/' + test_index_name
test_index_fields = ["str", "strlist", "facet", "facetlist", "spell", "id", "num", "numlist", "bool"]
test_facet_fields = ["facet", "facetlist", "facetonly"]
query_url = url + test_index_url + "/query"
cfg_url = url + test_index_url + "/settings"
json_data = {}


def start_marlin():
    print "starting ..."
    if os.path.isfile("../build-debug/main/marlin"):
        os.system("cd .. && ./build-debug/main/marlin &")
    else:
        os.system("cd .. && ./build/main/marlin &")
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

    def setup_test_index(self):
        index = {'name' : test_index_name, 'numShards': 5}
        r = self.post_any_status(indexes_url, index)
 
    def delete_test_app(self):
        self.use_app_key(master_app_id, master_api_key)
        apps_url = url + 'applications'
        test_app_url = apps_url + '/appfortests'
        r = self.delete(test_app_url)

    def ensure_no_jobs_on_index(self, indexname):
        info_url = indexes_url + '/' + indexname + '/info'
        while True:
            r = self.get(info_url)
            if (r['numJobs'] == 0):
                break
            time.sleep(1/10)

    def post_valid_query(self, pd):
        return self.post(query_url, pd, True)

    def post_query(self, pd, statuscode):
        return self.post_no_success_check(query_url, pd, statuscode=statuscode)

    def post_invalid_query(self, pd):
        return self.post(query_url, pd, False)

    def post_valid_settings(self, pd, scode=200):
        return self.post(cfg_url, pd, True if scode == 200 else False, statuscode=scode)

    def get_settings(self, scode=200):
        return self.get(cfg_url, statuscode=scode)

    def post_invalid_settings(self, pd):
        return self.post(cfg_url, pd, False, 400)

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
        index = {'name' : test_index_name}
        r = self.post(indexes_url, index)
        r = self.get(indexes_url)
        # make sure we get a list of indexes
        self.assertTrue(isinstance(r, list))
        # See if the index we added now is present
        present = False
        for index in r:
            if index['name'] == test_index_name:
                present = True
        self.assertTrue(present)
        self.delete_test_app()

    def test_b_create_multishard_index(self):
        index = {'name' : test_index_name, 'numShards': 5}
        r = self.post(indexes_url, index)
        r = self.get(indexes_url)
        # make sure we get a list of indexes
        self.assertTrue(isinstance(r, list))
        # See if the index we added now is present
        present = False
        for index in r:
            if index['name'] == test_index_name:
                present = True
        self.assertTrue(present)
        self.delete_test_app()

    def test_c_delete_index(self):
        # first create the app
        index = {'name' : test_index_name, 'numShards': 5}
        r = self.post(indexes_url, index)
        r = self.get(indexes_url)
        # make sure we get a list of indexes
        self.assertTrue(isinstance(r, list))
        # See if the index we added now is present
        present = False
        for index in r:
            if index['name'] == test_index_name:
                present = True
        self.assertTrue(present)
        r = self.delete(indexes_url + '/' + test_index_name)
        r = self.get(indexes_url)
        # make sure we get a list of indexes
        self.assertTrue(isinstance(r, list))
        # See if the index we added now is present
        present = False
        for index in r:
            if index['name'] == test_index_name:
                present = True
        self.assertFalse(present)
 
class TestIndexSettings(TestBase):
    def setUp(self):
        # Create the test app and use test app key
        self.setup_test_app()
        self.use_app_key(test_app_id, master_api_key)
        self.setup_test_index()
        super(TestIndexSettings, self).setUp()

    def test_a_invalid_settings(self):
        settings_url = test_index_url + '/settings'
        settings = {'indexedFields': "abc", "facetFields": test_facet_fields}
        self.post(settings_url, settings, 400)
        settings = {'indexedFields': test_index_fields, "facetFields": 234}
        self.post(settings_url, settings, 400)

    def test_b_valid_settings(self):
        settings_url = test_index_url + '/settings'
        settings = {'indexedFields': test_index_fields, "facetFields": test_facet_fields}
        self.post(settings_url, settings)
        r = self.get(settings_url)
        self.assertTrue('indexedFields' in r)
        self.assertTrue('facetFields' in r)
        self.assertTrue(r['facetFields'] == test_facet_fields)
        self.assertTrue(r['indexedFields'] == test_index_fields)
        settings = {'indexedFields': test_index_fields[0:2], "facetFields": test_facet_fields[0:2]}
        self.post(settings_url, settings)
        r = self.get(settings_url)
        self.assertTrue('indexedFields' in r)
        self.assertTrue('facetFields' in r)
        self.assertTrue(r['facetFields'] == test_facet_fields[0:2])
        self.assertTrue(r['indexedFields'] == test_index_fields[0:2])
        self.delete_test_app()
    #TODO: Add other settings as and when added

class TestIndexObjects(TestBase):
    def setUp(self):
        # Create the test app and use test app key
        self.setup_test_app()
        self.use_app_key(test_app_id, master_api_key)
        self.setup_test_index()
        super(TestIndexObjects, self).setUp()

    def test_a_loadobjects(self):
        #make sure mapping is empty
        r = self.get(test_index_url + '/mapping')
        self.assertFalse(r['readyToIndex'])
        self.assertIsNone(r['indexSchema'])
        self.assertIsNone(r['fullSchema'])
        # Add objects
        r = self.post(test_index_url, json_data['data'][0:25])
        self.ensure_no_jobs_on_index(test_index_name)
        r = self.get(test_index_url + '/mapping')
        self.assertFalse(r['readyToIndex'])
        self.assertIsNone(r['indexSchema'])
        self.assertIsNotNone(r['fullSchema'])
        self.delete_test_app()

    def test_b_cfg_loadobjects(self):
        #make sure mapping is empty
        r = self.get(test_index_url + '/mapping')
        self.assertFalse(r['readyToIndex'])
        self.assertIsNone(r['indexSchema'])
        self.assertIsNone(r['fullSchema'])
        # configure index and facet fields
        settings_url = test_index_url + '/settings'
        settings = {'indexedFields': test_index_fields, "facetFields": test_facet_fields}
        self.post(settings_url, settings)
        # Add objects
        r = self.post(test_index_url, json_data['data'][0:25])
        self.ensure_no_jobs_on_index(test_index_name)
        r = self.get(test_index_url + '/mapping')
        self.assertTrue(r['readyToIndex'])
        self.assertIsNotNone(r['indexSchema'])
        self.assertIsNotNone(r['fullSchema'])
        r = self.post(test_index_url, json_data['data'][25:])
        self.ensure_no_jobs_on_index(test_index_name)
        self.delete_test_app()


class TestSettings(TestBase):
    def setUp(self):
        # Create the test app and use test app key
        self.setup_test_app()
        self.use_app_key(test_app_id, master_api_key)
        super(TestSettings, self).setUp()

    def check_setting(self, key, value):
        y = self.get_settings()
        self.assertEqual(y[key], value);

    def test_a_cfg_loadobjects(self):
        self.setup_test_index()
        #make sure mapping is empty
        r = self.get(test_index_url + '/mapping')
        self.assertFalse(r['readyToIndex'])
        self.assertIsNone(r['indexSchema'])
        self.assertIsNone(r['fullSchema'])
        # configure index and facet fields
        settings_url = test_index_url + '/settings'
        settings = {'indexedFields': test_index_fields, "facetFields": test_facet_fields}
        self.post(settings_url, settings)
        # Add objects
        r = self.post(test_index_url, json_data['data'][0:1000])

    def test_b_hitsperpage(self):
        q = {"q": "a"}
        x = self.post_valid_query(q)
        self.assertEqual(x["numHits"], 25)
        s = {"hitsPerPage": "10"}
        self.post_invalid_settings(s)
        s = {"hitsPerPage": 10}
        self.post_valid_settings(s)
        self.check_setting("hitsPerPage", 10)
        q = {"q": "a"}
        x = self.post_valid_query(q)
        self.assertEqual(x["numHits"], 10)
        s = {"hitsPerPage": 25}
        self.post_valid_settings(s)
        self.check_setting("hitsPerPage", 25)
        x = self.post_valid_query(q)
        self.assertEqual(x["numHits"], 25)
        # Try overriding hitsPerPage
        q = {"q": "a", "hitsPerPage":11}
        x = self.post_valid_query(q)
        self.assertEqual(x["numHits"], 11)
        # Make sure override did not screw up the original value
        q = {"q": "a"}
        x = self.post_valid_query(q)
        self.assertEqual(x["numHits"], 25)

    def test_c_maxhits(self):
        q = {"q": "a"}
        x = self.post_valid_query(q)
        self.assertEqual(x["numPages"], 500/25)
        s = {"maxHits": "500"}
        self.post_invalid_settings(s)
        s = {"maxHits": 100}
        self.post_valid_settings(s)
        self.check_setting("maxHits", 100)
        q = {"q": "a"}
        x = self.post_valid_query(q)
        self.assertEqual(x["numPages"], 4)
        s = {"maxHits": 500}
        self.post_valid_settings(s)
        self.check_setting("maxHits", 500)
        x = self.post_valid_query(q)
        self.assertEqual(x["numHits"], 25)
        self.assertEqual(x["numPages"], 500/25)
        # Try overriding hitsPerPage
        q = {"q": "a", "maxHits":100}
        x = self.post_valid_query(q)
        self.assertEqual(x["numPages"], 4)
        # Make sure override did not screw up the original value
        q = {"q": "a"}
        x = self.post_valid_query(q)
        self.assertEqual(x["numPages"], 500/25)

    def test_d_maxfacets(self):
        q = {"q": "a"}
        x = self.post_valid_query(q)
        self.assertEqual(len(x["facets"]["facetlist"]), 10)
        s = {"maxFacetResults": "500"}
        self.post_invalid_settings(s)
        s = {"maxFacetResults": 20}
        self.post_valid_settings(s)
        self.check_setting("maxFacetResults", 20)
        q = {"q": "a"}
        x = self.post_valid_query(q)
        self.assertEqual(len(x["facets"]["facetlist"]), 20)
        s = {"maxFacetResults": 10}
        self.post_valid_settings(s)
        self.check_setting("maxFacetResults", 10)
        x = self.post_valid_query(q)
        self.assertEqual(len(x["facets"]["facetlist"]), 10)
        # Try overriding hitsPerPage
        q = {"q": "a", "maxFacetResults":15}
        x = self.post_valid_query(q)
        self.assertEqual(len(x["facets"]["facetlist"]), 15)
        # Make sure override did not screw up the original value
        q = {"q": "a"}
        x = self.post_valid_query(q)
        self.assertEqual(len(x["facets"]["facetlist"]), 10)
        self.delete_test_app()

if __name__ == '__main__':
    print "Starting tests .."
    live = (len(sys.argv) > 1) and sys.argv[1] == 'live'
    if not live:
        stop_marlin()
        start_marlin()
 
    with open("./test.json") as f:
        j = f.read()
        js = json.loads(j)
        json_data = js
 
    fail = 0

    # Ping test
    suite = unittest.TestLoader().loadTestsFromTestCase(TestPing)
    r = unittest.TextTestRunner(verbosity=0).run(suite)
    fail = fail + len(r.failures)

    # App tests
    suite = unittest.TestLoader().loadTestsFromTestCase(TestApp)
    r = unittest.TextTestRunner(verbosity=0).run(suite)
    fail = fail + len(r.failures)

    # Index tests
    suite = unittest.TestLoader().loadTestsFromTestCase(TestIndex)
    r = unittest.TextTestRunner(verbosity=0).run(suite)
    fail = fail + len(r.failures)

    # Index settings tests
    suite = unittest.TestLoader().loadTestsFromTestCase(TestIndexSettings)
    r = unittest.TextTestRunner(verbosity=0).run(suite)
    fail = fail + len(r.failures)

    # Settings tests
    """
    suite = unittest.TestLoader().loadTestsFromTestCase(TestSettings)
    r = unittest.TextTestRunner(verbosity=0).run(suite)
    fail = fail + len(r.failures)
    """

    # Index objects tests
    suite = unittest.TestLoader().loadTestsFromTestCase(TestIndexObjects)
    r = unittest.TextTestRunner(verbosity=0).run(suite)
    fail = fail + len(r.failures)

    os.system("cd robot && python -m robot .")


    if not live:
        stop_marlin()
    if fail != 0:
        print "\n************** FAILED ", fail, " TESTS *******************\n"
    sys.exit(fail)

