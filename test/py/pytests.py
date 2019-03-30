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
indexes_url = url + 'indexes'
index = "indexes/test"
strs = "./strings.json"
index_url = url + index
query_url = url + index + "/query"
cfg_url = url + index + "/settings"
key_url = url + "keys"
obj_url = url + index + "/1"

js = None
count = 0
words = None
fonly = None
facetcount = None
rcount = None
appid = "pytests1"
apikey = "12345678901234567890123456789012"
master_app_id = "abcdefgh"
master_api_key = "12345678901234567890123456789012"
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

def urlget(purl, statuscode=200):
    headers = { 'X-Marlin-Application-Id': appid, 'X-Marlin-REST-API-KEY': apikey}
    r = requests.get(purl, headers=headers, verify=False)
    x = r.json()
    return x

def check_nojobs():
    nojobs = False
    scount = 0
    while not nojobs and scount < 30000:
        surl = url + index + "/info"
        try:
            x = urlget(surl)
            if x["numJobs"] == 0:
                return
        except:
            pass
        time.sleep(1/100)
        scount += 1

def posth(headers, url, data):
    r = requests.post(url, data=json.dumps(data), headers=headers, verify=False)
    response = r.json()
    return response

def deleteh(headers, url):
    r = requests.delete(url, headers=headers, verify=False)
    response = r.json()
    return response

def start_testing():
    print "creating application ..."
    headers = { 'X-Marlin-Application-Id': master_app_id,
                'X-Marlin-REST-API-KEY': master_api_key,
                'Content-Type':'application/json'}
    apps_url = url + 'applications'
    app = {'name' : 'testapp', 'appId': appid, 'apiKey': apikey}
    r = posth(headers, apps_url, app)

    headers = { 'X-Marlin-Application-Id': appid,
                'X-Marlin-REST-API-KEY': apikey,
                'Content-Type':'application/json'}
    index = {'name' : 'test', 'numShards': 1}
    r = posth(headers, indexes_url, index)
    settings = {
            "indexedFields": [
                "str",
                "strlist",
                "facet",
                "facetlist",
                "spell",
                "id",
                "num",
                "numlist",
                "bool"
                ],
            "facetFields": [
                "facet",
                "facetlist",
                "facetonly"
                ],
            "hitsPerPage": 25
            }
    r = posth(headers, cfg_url, settings)


def end_testing():
    headers = { 'X-Marlin-Application-Id': master_app_id,
                'X-Marlin-REST-API-KEY': master_api_key,
                'Content-Type':'application/json'}
    apps_url = url + 'applications'
    test_app_url = apps_url + '/testapp'
    deleteh(headers, test_app_url)

class MyHTTPSHandler(urllib2.HTTPSHandler, urllib2.HTTPDefaultErrorHandler):
    def http_error_default(self, req, rsp, code, msg, hdrs):
        return rsp 

class TestBase(unittest.TestCase):
    def __init__(self, x):
        unittest.TestCase.__init__(self, x)
        urllib2.install_opener(urllib2.build_opener(MyHTTPSHandler(context=ctx)))

    # TODO: Global load and clear datastore for each testsuite
    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        self.startTime = time.time()
        self.check_nojobs()
        requests.packages.urllib3.disable_warnings()

    def tearDown(self):
        t = time.time() - self.startTime
        print "%s: %.4fs" % (self.id(), t)

    def post_no_success_check(self, purl, pd, statuscode=200):
        headers = { 'X-Marlin-Application-Id': appid, 'X-Marlin-REST-API-KEY': apikey, 'Content-Type':'application/json'}
        r = requests.post(purl, data=json.dumps(pd), headers=headers, verify=False)
        self.assertEqual(r.status_code, statuscode)
        x = r.json()
        return x

    def delete(self, purl, statuscode=200):
        headers = { 'X-Marlin-Application-Id': appid, 'X-Marlin-REST-API-KEY': apikey}
        r = requests.delete(purl, headers=headers, verify=False)
        self.assertEqual(r.status_code, statuscode)
        x = r.json()
        return x

    def post(self, purl, pd, success, statuscode=200):
        x = self.post_no_success_check(purl, pd, statuscode)
        #self.assertEqual(x["success"], success)
        return x

    def put(self, purl, pd, statuscode=200):
        headers = { 'X-Marlin-Application-Id': appid, 'X-Marlin-REST-API-KEY': apikey, 'Content-Type':'application/json'}
        r = requests.put(purl, data=json.dumps(pd), headers=headers, verify=False)
        x = r.json
        return x

    def patch(self, purl, pd, statuscode=200):
        headers = { 'X-Marlin-Application-Id': appid, 'X-Marlin-REST-API-KEY': apikey, 'Content-Type':'application/json'}
        r = requests.patch(purl, data=json.dumps(pd), headers=headers, verify=False)
        x = r.json
        return x

    def get(self, purl, statuscode=200):
        headers = { 'X-Marlin-Application-Id': appid, 'X-Marlin-REST-API-KEY': apikey}
        r = requests.get(purl, headers=headers, verify=False)
        self.assertEqual(r.status_code, statuscode)
        x = r.json()
        return x

    def post_valid_query(self, pd):
        #if "highlight" not in pd:
        #    pd["highlight"] = {"fields": ["str", "strlist", "facet", "facetlist"]}
        return self.post(query_url, pd, True)

    def post_query(self, pd, statuscode):
        return self.post_no_success_check(query_url, pd, statuscode=statuscode)

    def post_invalid_query(self, pd):
        return self.post(query_url, pd, False, 400)

    def post_valid_settings(self, pd, scode=200):
        return self.post(cfg_url, pd, True if scode == 200 else False, statuscode=scode)

    def get_settings(self, scode=200):
        return self.get(cfg_url, statuscode=scode)

    def post_invalid_settings(self, pd):
        return self.post(cfg_url, pd, False, 400)

    def post_unauthorized_key(self, pd):
        return self.post(key_url, pd, False, 403)

    def post_key(self, pd, statuscode=200):
        return self.post_no_success_check(key_url, pd, statuscode)

    def get_object(self, oid=1, statuscode=200):
        ourl = url + index + "/" + str(oid)
        return self.get(ourl, statuscode)

    def del_object(self, oid=1, statuscode=200):
        ourl = url + index + "/" + str(oid)
        return self.delete(ourl, statuscode)

    def replace_object(self, pd, oid=1, statuscode=200):
        ourl = url + index + "/" + str(oid)
        return self.put(ourl, pd, statuscode)

    def update_object(self, pd, oid=1, statuscode=200):
        ourl = url + index + "/" + str(oid)
        return self.put(ourl, pd, statuscode)

    def add_object(self, pd, oid=1, statuscode=200):
        if "_id" not in pd:
          pd["_id"] = str(oid)
        ourl = url + index
        return self.post_no_success_check(ourl, pd, statuscode)

    def bulk_objects(self, pd, statuscode=200):
        ourl = url + index + "/bulk"
        return self.post_no_success_check(ourl, pd, statuscode)

    def check_nojobs(self):
        nojobs = False
        while not nojobs:
            surl = url + index + "/info"
            x = self.get(surl)
            if x["numJobs"] == 0:
                return
            time.sleep(1/100)


class TestSettings(TestBase):
    def check_setting(self, key, value):
        y = self.get_settings()
        self.assertEqual(y[key], value);

    def test_a_hitsperpage(self):
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

    def test_b_maxhits(self):
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

    def test_c_maxfacets(self):
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
        q = {"q": "a", "maxFacetResults":10000}
        x = self.post_valid_query(q)

class TestBasicQuery(TestBase):
    # Test basic string matching
    def test_a_strings(self):
        for i in range(0, len(words)):
            w = words[i]
            for j in range(2, len(w)+1):
                splt = w[:j]
                r = {"q": splt}
                x = self.post_valid_query(r)
                self.assertEqual(x["totalHits"], rcount[i%4])

    # Test matching strings with mistakes
    def no_test_y_spellcheck(self):
        for i in range(0, len(words)):
            # Test 8 chars with 2 mistakes
            w = words[i]
            for j in range(0, len(w)+1):
                mis = w[:j] + "ff" + w[j:]
                r = {"q": mis}
                #print mis
                x = self.post_valid_query(r)
                self.assertEqual(x["totalHits"], rcount[i%4])
            # Test 9 chars with 2 mistakes in different places
            for j in range(0, len(w)+1):
                for k in range(0, len(w)+2):
                    mis = w[:j] + "f" + w[j:]
                    qmis = mis[:k] + "f" + mis[k:]
                    #print qmis
                    r = {"q": qmis}
                    x = self.post_valid_query(r)
                    self.assertEqual(x["totalHits"], rcount[i%4])
            # Test 8 chars with 2 mistakes in different places
            w = w[:len(w)-1]
            for j in range(0, len(w)+1):
                for k in range(0, len(w)+2):
                    mis = w[:j] + "f" + w[j:]
                    qmis = mis[:k] + "f" + mis[k:]
                    #print qmis
                    r = {"q": qmis}
                    x = self.post_valid_query(r)
                    self.assertEqual(x["totalHits"], rcount[i%4])
            # Test 4 chars with 1 mistake
            w = w[:3]
            for j in xrange(0, len(w)+1):
                mis = w[:j] + "f" + w[j:]
                r = {"q": mis}
                #print mis
                x = self.post_valid_query(r)
                self.assertEqual(x["totalHits"], rcount[i%4])
            # Test numerics
            for j in xrange(0, len(w)+1):
                mis = w[:j] + "1" + w[j:]
                r = {"q": mis}
                x = self.post_valid_query(r)
                self.assertEqual(x["totalHits"], rcount[i%4])
 
    # Ordering by fields, direction
    def test_c_order_direction(self):
        #id ascending
        r = {"q": "aa1aa", "rankBy": {"id":"asc"}}
        x = self.post_valid_query(r)
        hits = x["hits"]
        for i in xrange(1, 10):
            self.assertEqual(hits[i-1]["id"], i)
        #id descending
        r = {"q": "aa1aa", "rankBy": {"id":"desc"}}
        x = self.post_valid_query(r)
        hits = x["hits"]
        for i in xrange(1, 10):
            self.assertEqual(hits[i-1]["id"], count-i+1)
        #num ascending
        r = {"q": "aa1aa", "rankBy": {"num":"asc"}}
        x = self.post_valid_query(r)
        hits = x["hits"]
        for i in xrange(1, 10):
            self.assertEqual(hits[i-1]["id"], count-i+1)
        #num descending
        r = {"q": "aa1aa", "rankBy": {"num":"desc"}}
        x = self.post_valid_query(r)
        hits = x["hits"]
        for i in xrange(1, 10):
            self.assertEqual(hits[i-1]["id"], i)
 
    # Test resultant bitmap iteration by counting facets
    # Tests facet only data too
    def test_d_result_bmap(self):
        fcount = [
                    [count, count*0.75, count*0.5, count*0.25],
                    [count*0.75, count*0.75, count*0.5, count*0.25],
                    [count*0.5, count*0.5, count*0.5, count*0.25],
                    [count*0.25, count*0.25, count*0.25, count*0.25],
                 ]
        for i in range(0, len(words)):
            w = words[i]
            for j in range(2, len(w)+1):
                splt = w[:j]
                r = {"q": splt}
                x = self.post_valid_query(r)
                self.assertEqual(x["totalHits"], rcount[i%4])
                #print x["facets"]["facetonly"]
                for k in range(0, len(fonly)):
                    for f in x["facets"]["facetonly"]:
                        if f["key"] == fonly[k]:
                            self.assertEqual(f["count"], fcount[i%4][k])

    # Test facet counts
    def test_e_facet_counts(self):
        # Query for everything
        r = {"q": words[0]}
        x = self.post_valid_query(r)
        for f,fv in x["facets"].iteritems():
            for f in fv:
                self.assertEqual(facetcount[f["key"]], f["count"])

    # Test invalid filter
    def test_z_invalid_filter(self):
        r = {"q": words[0], "filter": {"nbool": {"$eq": True}}}
        x = self.post_invalid_query(r)
        r = {"q": words[0], "filter": {"bool": {"$seq": True}}}
        x = self.post_invalid_query(r)
        r = {"q": words[0], "filter": {"bool": {"$eq": "test"}}}
        x = self.post_invalid_query(r)
        r = {"q": words[0], "filter": {"bool": {"$eq": 1}}}
        x = self.post_invalid_query(r)
        r = {"q": words[0], "filter": {"facetonly": {"$eq": 1}}}
        x = self.post_invalid_query(r)
        r = {"q": words[0], "filter": {"facetonly": {"$eq": True}}}
        x = self.post_invalid_query(r)
        r = {"q": words[0], "filter": {"str": {"$eq": "aaaaaaa"}}}
        x = self.post_invalid_query(r)

    # Test boolean filter
    def test_g_boolean_filter(self):
        # Test true in both directions eq and ne
        r = {"q": words[0], "rankBy": {"id":"asc"}, "filter": {"bool": {"$eq": True}}}
        x = self.post_valid_query(r)
        hits = x["hits"]
        val = 1;
        for h in hits:
            self.assertEqual(h["id"], val*10)
            val += 1
        r = {"q": words[0], "rankBy": {"id":"desc"}, "filter": {"bool": {"$eq": True}}}
        x = self.post_valid_query(r)
        hits = x["hits"]
        val = count;
        for h in hits:
            self.assertEqual(h["id"], val)
            val -= 10
        r = {"q": words[0], "rankBy": {"id":"asc"}, "filter": {"bool": {"$ne": True}}}
        x = self.post_valid_query(r)
        hits = x["hits"]
        val = 0;
        for h in hits:
            val += 1
            if val % 10 == 0:
                val += 1
            self.assertEqual(h["id"], val)
        # Test in both directions
        r = {"q": words[0], "rankBy": {"id":"desc"}, "filter": {"bool": {"$ne": True}}}
        x = self.post_valid_query(r)
        hits = x["hits"]
        val = count+1;
        for h in hits:
            val -= 1
            if val % 10 == 0:
                val -= 1
            self.assertEqual(h["id"], val)
        # Test false in both directions eq and neq
        r = {"q": words[0], "rankBy": {"id":"asc"}, "filter": {"bool": {"$ne": False}}}
        x = self.post_valid_query(r)
        hits = x["hits"]
        val = 1;
        for h in hits:
            self.assertEqual(h["id"], val*10)
            val += 1
        r = {"q": words[0], "rankBy": {"id":"desc"}, "filter": {"bool": {"$ne": False}}}
        x = self.post_valid_query(r)
        hits = x["hits"]
        val = count;
        for h in hits:
            self.assertEqual(h["id"], val)
            val -= 10
        r = {"q": words[0], "rankBy": {"id":"asc"}, "filter": {"bool": {"$eq": False}}}
        x = self.post_valid_query(r)
        hits = x["hits"]
        val = 0;
        for h in hits:
            val += 1
            if val % 10 == 0:
                val += 1
            self.assertEqual(h["id"], val)
        # Test in both directions
        r = {"q": words[0], "rankBy": {"id":"desc"}, "filter": {"bool": {"$eq": False}}}
        x = self.post_valid_query(r)
        hits = x["hits"]
        val = count+1;
        for h in hits:
            val -= 1
            if val % 10 == 0:
                val -= 1
            self.assertEqual(h["id"], val)

    # Test numeric filters
    def test_h_numeric_filter(self):
        # Equal filter
        r = {"q": words[0], "rankBy": {"id":"asc"}, "filter": {"id": {"$eq": 100}}}
        x = self.post_valid_query(r)
        self.assertEqual(x["totalHits"], 1)
        self.assertEqual(x["hits"][0]["id"], 100)
        # No hits
        r = {"q": words[2], "rankBy": {"id":"asc"}, "filter": {"id": {"$eq": 2}}}
        x = self.post_valid_query(r)
        self.assertEqual(x["totalHits"], 0)
        # Not equal filter
        r = {"q": words[0], "rankBy": {"id":"asc"}, "filter": {"id": {"$ne": 100}}}
        x = self.post_valid_query(r)
        self.assertEqual(x["totalHits"], count-1)
        # Greater than
        r = {"q": words[0], "rankBy": {"id":"asc"}, "filter": {"id": {"$gt": 100}}}
        x = self.post_valid_query(r)
        hits = x["hits"]
        val = 100
        for h in hits:
            val += 1
            self.assertEqual(h["id"], val)
        # Greater than equals
        r = {"q": words[0], "rankBy": {"id":"asc"}, "filter": {"id": {"$gte": 100}}}
        x = self.post_valid_query(r)
        hits = x["hits"]
        val = 100
        for h in hits:
            self.assertEqual(h["id"], val)
            val += 1
        # Lesser than
        r = {"q": words[0], "rankBy": {"id":"desc"}, "filter": {"id": {"$lt": 100}}}
        x = self.post_valid_query(r)
        hits = x["hits"]
        val = 100
        for h in hits:
            val -= 1
            self.assertEqual(h["id"], val)
        # Lesser than equals
        r = {"q": words[0], "rankBy": {"id":"desc"}, "filter": {"id": {"$lte": 100}}}
        x = self.post_valid_query(r)
        hits = x["hits"]
        val = 100
        for h in hits:
            self.assertEqual(h["id"], val)
            val -= 1
        # GT LT
        r = {"q": words[0], "rankBy": {"id":"asc"}, "filter": {"id": {"$gt": 100, "$lt": 105}}}
        x = self.post_valid_query(r)
        self.assertEqual(x["totalHits"], 4)
        hits = x["hits"]
        val = 100
        for h in hits:
            val += 1
            self.assertEqual(h["id"], val)
        # GTE LTE
        r = {"q": words[0], "rankBy": {"id":"asc"}, "filter": {"id": {"$gte": 1000, "$lte": 1005}}}
        x = self.post_valid_query(r)
        self.assertEqual(x["totalHits"], 6)
        hits = x["hits"]
        val = 1000
        for h in hits:
            self.assertEqual(h["id"], val)
            val += 1

    def test_i_facet_filter(self):
        # Equal filter
        for i in range(0, len(fonly)):
            r = {"q": words[0], "rankBy": {"id":"asc"}, "filter": {"facetonly": fonly[i]}}
            x = self.post_valid_query(r)
            self.assertEqual(x["totalHits"], rcount[i])
        r = {"q": words[0], "rankBy": {"id":"asc"}, "filter": {"facetonly": fonly[0]}}
        x = self.post_valid_query(r)
        self.assertEqual(x["totalHits"], rcount[0])
        # and filter
        r = {"q": words[0], "rankBy": {"id":"asc"}, "filter": {"facetonly": {"$and": [fonly[0], fonly[1]]}}}
        x = self.post_valid_query(r)
        self.assertEqual(x["totalHits"], rcount[1])
        r = {"q": words[0], "rankBy": {"id":"asc"}, "filter": {"facetonly": {"$and": [fonly[0], fonly[1], fonly[2]]}}}
        x = self.post_valid_query(r)
        self.assertEqual(x["totalHits"], rcount[2])
        r = {"q": words[0], "rankBy": {"id":"asc"}, "filter": {"facetonly": {"$and": [fonly[0], fonly[1], fonly[2], fonly[3]]}}}
        x = self.post_valid_query(r)
        self.assertEqual(x["totalHits"], rcount[3])
        r = {"q": "", "rankBy": {"id":"asc"}, "filter": {"facetonly": {"$in": [fonly[2], fonly[3]]}}}
        x = self.post_valid_query(r)
        self.assertEqual(x["totalHits"], rcount[2])
        r = {"q": "", "rankBy": {"id":"asc"}, "filter": {"facetonly": {"$nin": [fonly[2], fonly[3]]}}}
        x = self.post_valid_query(r)
        self.assertEqual(x["totalHits"], rcount[2])
        r = {"q": "", "rankBy": {"id":"asc"}, "filter": {"facetonly": {"$nin": [fonly[0], fonly[3]]}}}
        x = self.post_valid_query(r)
        self.assertEqual(x["totalHits"], 0)


    def test_j_pagination(self):
        # Test with insanely large page number
        r = {"q": words[0], "rankBy": {"id":"asc"}, "page": 1000, "hitsPerPage": 20}
        x = self.post_invalid_query(r)
        r = {"q": words[0], "rankBy": {"id":"asc"}, "page": -1, "hitsPerPage": 20}
        x = self.post_invalid_query(r)
        # Test valid data
        for i in range(1, 25):
            r = {"q": words[0], "rankBy": {"id":"asc"}, "page": i, "hitsPerPage": 20}
            x = self.post_valid_query(r)
            hits = x["hits"]
            idi = (i-1) * 20;
            hcnt = 0
            for h in hits:
                hcnt += 1
                self.assertEqual(h["id"], idi+hcnt)
        # Test boundry page num
        r = {"q": words[0], "rankBy": {"id":"asc"}, "page": 26, "hitsPerPage": 20}
        x = self.post_invalid_query(r)
        # Test partial result page
        r = {"q": words[0], "rankBy": {"id":"asc"}, "filter":{"id": {"$lte": 45}} , "page": 5, "hitsPerPage": 10}
        x = self.post_valid_query(r)
        self.assertEqual(x["totalHits"], 45)
        hits = x["hits"]
        # Page 5 should have 5 results when hits per page is 10
        self.assertEqual(len(hits), 5)
 
    def test_k_emptyquery(self):
        r = {"rankBy": {"id":"asc"}}
        x = self.post_valid_query(r)
        self.assertEqual(x["totalHits"], count)
        r = {"filter": {"bool":True}}
        x = self.post_valid_query(r)
        self.assertEqual(x["totalHits"], count/10)
        r = {"filter": {"bool":False}}
        x = self.post_valid_query(r)
        self.assertEqual(x["totalHits"], count-(count/10))

    def test_l_rank_attr(self):
        r = {"q":"attr", "rankBy": {"id":"asc"}, "hitsPerPage": 50}
        x = self.post_valid_query(r)
        self.assertEqual(x["totalHits"], count)
        hits = x["hits"]
        hcnt = 1
        # Every 4th entry has the best attribute position for 'attr'
        for h in hits:
            self.assertEqual(h["id"], hcnt)
            hcnt += 4

    def test_m_rank_spell(self):
        r = {"q":"eeeeeee", "rankBy": {"id":"asc"}, "hitsPerPage": 50}
        x = self.post_valid_query(r)
        self.assertEqual(x["totalHits"], 50)
        hits = x["hits"]
        hcnt = 5
        # Every 4th entry has the best attribute position for 'attr'
        for h in hits[0:9]:
            self.assertEqual(h["id"], hcnt)
            hcnt += 5
        hcnt = 1
        for h in hits[10:]:
            self.assertEqual(h["id"], hcnt)
            hcnt += 1
            if hcnt % 5 == 0:
                hcnt += 1

    def test_n_rank_proximity(self):
        r = {"q":"aaaaaaa attr", "rankBy": {"id":"asc"}, "hitsPerPage": 50}
        x = self.post_valid_query(r)
        self.assertEqual(x["totalHits"], count)
        hits = x["hits"]
        hcnt = 1
        # Every 4th entry has the best attribute position for 'attr'
        for h in hits:
            self.assertEqual(h["id"], hcnt)
            hcnt += 4


#TODO: Better tests required for testing replace / update objects
class TestObjects(TestBase):
    def test_a_objects(self):
        o = {"this":"that"}
        x = self.add_object(o, count+1)
        self.check_nojobs()
        x = self.get_object(count+1)
        self.assertEqual(x["this"], "that")
        o = {"this":"what"}
        x = self.replace_object(o, count+1)
        self.check_nojobs()
        x = self.get_object(count+1)
        self.assertEqual(x["this"], "what")
        x = self.del_object(count+1)
        self.check_nojobs()
        x = self.get_object(count+1, 404)

    def test_b_moreobjects(self):
        objs = []
        for i in xrange(1, count):
            if i%1000 == 0:
                o = self.get_object(i)
                objs.append(o)
                self.del_object(i)
        self.check_nojobs()
        for o in objs:
            self.add_object(o)
        self.check_nojobs()

class TestKeys(TestBase):
    def key_getobject(self, key, oid, scode=200):
        global apikey
        temp = apikey
        apikey = key
        x = self.get_object(oid, statuscode = scode)
        apikey = temp
        return x

    # Deletes, make sure it was deleted and adds it back
    def key_delobject(self, key, oid, scode=200):
        o = self.get_object(oid)
        global apikey
        temp = apikey
        apikey = key
        x = self.del_object(oid, statuscode = scode)
        apikey = temp
        self.check_nojobs()
        if scode == 200:
            x = self.add_object(o)
            self.check_nojobs()
        return x

    def key_replaceobject(self, key, pd, oid, scode=200):
        o = self.get_object(oid)
        global apikey
        temp = apikey
        apikey = key
        x = self.replace_object(pd, oid, statuscode = scode)
        apikey = temp
        self.check_nojobs()
        x = self.replace_object(o, oid, statuscode = scode)
        self.check_nojobs()
        return x

    def key_addobject(self, key, pd, scode=200):
        global apikey
        temp = apikey
        apikey = key
        x = self.add_object(pd, statuscode = scode)
        apikey = temp
        self.check_nojobs()
        if scode == 200:
            x = self.del_object(count+1, statuscode = scode)
        self.check_nojobs()

    def key_query(self, key, scode=200):
        global apikey
        temp = apikey
        apikey = key
        pd = {"q":"a"}
        x = self.post_query(pd, statuscode = scode)
        apikey = temp
        return x

    def key_list_index(self, key, scode=200):
        global apikey
        temp = apikey
        apikey = key
        u = url + "indexes"
        x = self.get(u, statuscode = scode)
        apikey = temp
        return x

    def key_getconfig(self, key, scode=200):
        global apikey
        temp = apikey
        apikey = key
        x = self.get_settings(scode)
        apikey = temp
        return x

    def key_setconfig(self, key, scode=200):
        global apikey
        x = self.get_settings()
        temp = apikey
        apikey = key
        x = self.post_valid_settings(x, scode)
        apikey = temp
        return x

    def validate_key(self, key):
        # TODO Add more permission handling
        p = key["permissions"]
        k = key["apiKey"]
        self.key_addobject(k, {"_id": str(count+1), "this":"that"}, 200 if "addDoc" in p else 403)
        self.key_getobject(k, 1, 200 if "browseDoc" in p else 403)
        self.key_delobject(k, 1, 200 if "delDoc" in p else 403)
        self.key_replaceobject(k, {"this":"that"}, 1, 200 if "updateDoc" in p else 403)
        self.key_query(k, 200 if "queryIndex" in p else 403)
        self.key_list_index(k, 200 if "listIndex" in p else 403)
        self.key_getconfig(k, 200 if "getConfig" in p else 403)
        self.key_setconfig(k, 200 if "setConfig" in p else 403)
 
    def del_key(self, k):
        key = k["apiKey"]
        self.delete(key_url+"/"+key)

    # Makes sure keys are allowed to be created only with the master key
    def test_a_create_only_by_master(self):
        global apikey
        key = {"description":"test key"}
        apikey = '0' + apikey[1:]
        self.post_unauthorized_key(key)
        apikey = '1' + apikey[1:]
        print apikey
        k = self.post_key(key)
        key = k["apiKey"]
        self.assertEqual(len(k["apiKey"]), 32)
        # Make sure we can get the same key
        k2 = self.get(key_url+"/"+key, 200)
        self.assertEqual(k["apiKey"], k2["apiKey"])
        # Delete key and make sure it was deleted
        self.delete(key_url+"/"+key)
        self.get(key_url+"/"+key, 404)
        self.key_getobject(apikey, 1)

    def test_b_create_key_permission(self):
        key = {"description":"test key", "permissions":"test"}
        # Permissions not an array should fail
        k = self.post_key(key, 400)
        key = {"description":"test key", "permissions":["addDoc", "dummy"]}
        k = self.post_key(key, 400)
        key = {"description":"test key", "permissions":["addDoc", "delDoc"]}
        k = self.post_key(key, 200)
        #self.del_key(k)
        key = {"description":"test key", "permissions":["addDoc", "delDoc", "updateDoc", "addIndex", "delIndex", "browseDoc", "getConfig", "setConfig", "analytics", "listIndex", "queryIndex"]}
        k2 = self.post_key(key, 200)
        key = {"description":"test key", "permissions":["browseDoc", "getConfig", "setConfig", "analytics", "listIndex", "queryIndex"]}
        k3 = self.post_key(key, 200)
        self.assertNotEqual(k["apiKey"], k2["apiKey"])
        self.assertNotEqual(k2["apiKey"], k3["apiKey"])
        self.assertNotEqual(k["apiKey"], k3["apiKey"])
        self.del_key(k)
        self.del_key(k2)
        self.del_key(k3)
        self.get(key_url+"/"+k["apiKey"], 404)
        self.get(key_url+"/"+k2["apiKey"], 404)
        self.get(key_url+"/"+k3["apiKey"], 404)

    def test_c_permission(self):
        def keytest(key):
            k = self.post_key(key)
            key["apiKey"] = k["apiKey"]
            self.validate_key(key)
            self.del_key(k)
        # TODO : Check more premission handling
        key = {"description":"test key", "permissions":["addDoc"]}
        keytest(key)
        key = {"description":"test key", "permissions":["delDoc"]}
        keytest(key)
        key = {"description":"test key", "permissions":["updateDoc"]}
        keytest(key)
        key = {"description":"test key", "permissions":["queryIndex"]}
        keytest(key)
        key = {"description":"test key", "permissions":["listIndex"]}
        keytest(key)
        key = {"description":"test key", "permissions":["getConfig"]}
        keytest(key)
        key = {"description":"test key", "permissions":["setConfig"]}
        keytest(key)
        key = {"description":"test key", "permissions":["addDoc", "queryIndex"]}
        keytest(key)
        key = {"description":"test key", "permissions":["addDoc", "delDoc", "queryIndex"]}
        keytest(key)
        key = {"description":"test key", "permissions":["addDoc", "delDoc", "updateDoc"]}
        keytest(key)
        key = {"description":"test key", "permissions":["addDoc", "delDoc", "updateDoc", "queryIndex"]}
        keytest(key)


# Verify with a list of naughty strings if things are still fine
class TestStrings(TestBase):
    def test_a_query(self):
        js = []
        with open("./strings.json") as f:
            j = f.read()
            js = json.loads(j)
        for ns in js:
            r = {"q":ns}
            x = self.post_valid_query(r)

    def test_b_query(self):
        js = []
        with open("./strings2.json") as f:
            j = f.read()
            js = json.loads(j)
        for ns in js:
            r = {"q":ns}
            x = self.post_valid_query(r)

    def test_c_facet_filter(self):
        js = []
        with open("./strings.json") as f:
            j = f.read()
            js = json.loads(j)
        for ns in js:
            r = {"q":ns, "filter": {"facetonly": ns}}
            x = self.post_valid_query(r)
        with open("./strings2.json") as f:
            j = f.read()
            js = json.loads(j)
        for ns in js:
            r = {"q":ns, "filter": {"facetonly": ns}}
            x = self.post_valid_query(r)


    def test_d_facet_and_filter(self):
        js = []
        with open("./strings.json") as f:
            j = f.read()
            js = json.loads(j)
        for ns in js:
            r = {"q":ns, "filter": {"facetonly": {"$and": [ns]}}}
            x = self.post_valid_query(r)
        with open("./strings2.json") as f:
            j = f.read()
            js = json.loads(j)
        for ns in js:
            r = {"q":ns, "filter": {"facetonly": {"$and": [ns]}}}
            x = self.post_valid_query(r)

    def test_e_facet_or_filter(self):
        js = []
        with open("./strings.json") as f:
            j = f.read()
            js = json.loads(j)
        for ns in js:
            r = {"q":ns, "filter": {"facetonly": {"$or": [ns]}}}
            x = self.post_valid_query(r)
        with open("./strings2.json") as f:
            j = f.read()
            js = json.loads(j)
        for ns in js:
            r = {"q":ns, "filter": {"facetonly": {"$or": [ns]}}}
            x = self.post_valid_query(r)

    def test_f_facet_nin_filter(self):
        js = []
        with open("./strings.json") as f:
            j = f.read()
            js = json.loads(j)
        for ns in js:
            r = {"q":ns, "filter": {"facetonly": {"$nin": [ns]}}}
            x = self.post_valid_query(r)
        with open("./strings2.json") as f:
            j = f.read()
            js = json.loads(j)
        for ns in js:
            r = {"q":ns, "filter": {"facetonly": {"$nin": [ns]}}}
            x = self.post_valid_query(r)

    def test_g_facet_and_filter(self):
        js = []
        with open("./strings.json") as f:
            j = f.read()
            js = json.loads(j)
        for ns in js:
            r = {"q":"", "filter": {"facetonly": {"$and": [ns, "oneone"]}}}
            x = self.post_valid_query(r)
        with open("./strings2.json") as f:
            j = f.read()
            js = json.loads(j)
        for ns in js:
            r = {"q":"", "filter": {"facetonly": {"$and": [ns, "oneone"]}}}
            x = self.post_valid_query(r)

    def test_h_facet_nin_filter(self):
        js = []
        with open("./strings.json") as f:
            j = f.read()
            js = json.loads(j)
        for ns in js:
            r = {"q":ns, "filter": {"facetonly": {"$nin": [ns, "oneone"]}}}
            x = self.post_valid_query(r)
        with open("./strings2.json") as f:
            j = f.read()
            js = json.loads(j)
        for ns in js:
            r = {"q":ns, "filter": {"facetonly": {"$nin": [ns, "oneone"]}}}
            x = self.post_valid_query(r)


 

def bench_search():
    req = urllib2.Request(query_url)
    req.add_header('X-Marlin-Application-Id', appid)
    req.add_header('X-Marlin-REST-API-KEY', apikey)
    req.add_header('Content-Type', 'application/json')
    pd = {"q":"aaaa"}
    resp = urllib2.urlopen(req, json.dumps(pd), context=ctx)

def bench_order_asc():
    req = urllib2.Request(query_url)
    req.add_header('X-Marlin-Application-Id', appid)
    req.add_header('X-Marlin-REST-API-KEY', apikey)
    req.add_header('Content-Type', 'application/json')
    pd = {"q":"aaaa", "rankBy": {"id" : "asc"}}
    resp = urllib2.urlopen(req, json.dumps(pd), context=ctx)

def bench_order_desc():
    req = urllib2.Request(query_url)
    req.add_header('X-Marlin-Application-Id', appid)
    req.add_header('X-Marlin-REST-API-KEY', apikey)
    req.add_header('Content-Type', 'application/json')
    pd = {"q":"aaaa", "rankBy": {"id" : "desc"}}
    resp = urllib2.urlopen(req, json.dumps(pd), context=ctx)

def bench_facet_filter():
    req = urllib2.Request(query_url)
    req.add_header('X-Marlin-Application-Id', appid)
    req.add_header('X-Marlin-REST-API-KEY', apikey)
    req.add_header('Content-Type', 'application/json')
    pd = {"q":"aaaa", "filter": {"facetonly": "fourfour"}}
    resp = urllib2.urlopen(req, json.dumps(pd), context=ctx)

 
if __name__ == '__main__':
    print "Starting tests .."
    requests.packages.urllib3.disable_warnings()
    start_testing()

    t1 = datetime.datetime.now()
    with open("../test.json") as f:
        j = f.read()
        js = json.loads(j)
        req = urllib2.Request(index_url)
        req.add_header('X-Marlin-Application-Id', appid)
        req.add_header('X-Marlin-REST-API-KEY', apikey)
        req.add_header('Content-Type', 'application/json')
        resp = urllib2.urlopen(req, json.dumps(js["data"]), context=ctx)
        if resp.getcode() != 200:
            print "Failed to load data error %d" % resp.getcode()
            exit()
        else:
            r = resp.read()
            jr = json.loads(r)
            if jr["success"]:
                t2 = datetime.datetime.now()
                td = t2-t1
                print "Successfully loaded data ", td
            else:
                print "Failed to load data"
                exit()

    if (len(sys.argv) > 1) and (sys.argv[1] == "bench"):
        print "\nRUNNING BENCHMARKS\n"
        bench_order_asc()
        print "Benchmark search       : ", timeit.timeit("bench_search()", number=1000, setup="from __main__ import bench_search"), "s"
        print "Benchmark order asc    : ", timeit.timeit("bench_order_asc()", number=1000, setup="from __main__ import bench_order_asc"), "s"
        print "Benchmark order desc   : ", timeit.timeit("bench_order_desc()", number=1000, setup="from __main__ import bench_order_desc"), "s"
        print "Benchmark facet filter : ", timeit.timeit("bench_facet_filter()", number=1000, setup="from __main__ import bench_facet_filter"), "s"
        print "\n\n"
        exit()
    count = js["count"]
    rcount = [count, count*0.75, count*0.5, count*0.25]
    words = js["words"]
    fonly = js["facetonly"]
    facetcount = js["facet_count"]
    for i in xrange(0, len(fonly)):
        facetcount[fonly[i]] = rcount[i]

    print "\nRUNNING TESTS\n"
    #Run tests

    fail = 0

    # Settings tests
    suite = unittest.TestLoader().loadTestsFromTestCase(TestSettings)
    r = unittest.TextTestRunner(verbosity=5).run(suite)
    fail = fail + len(r.failures)

    # Object tests
    suite = unittest.TestLoader().loadTestsFromTestCase(TestObjects)
    r = unittest.TextTestRunner(verbosity=5).run(suite)
    fail = fail + len(r.failures)

    # Key tests
    suite = unittest.TestLoader().loadTestsFromTestCase(TestKeys)
    r = unittest.TextTestRunner(verbosity=5).run(suite)
    fail = fail + len(r.failures)

    # Query tests
    suite = unittest.TestLoader().loadTestsFromTestCase(TestBasicQuery)
    r = unittest.TextTestRunner(verbosity=5).run(suite)
    fail = fail + len(r.failures)

    # Strings tests
    '''
    suite = unittest.TestLoader().loadTestsFromTestCase(TestStrings)
    r = unittest.TextTestRunner(verbosity=5).run(suite)
    fail = fail + len(r.failures)
    '''

    end_testing()

    if fail != 0:
        print "\n************** FAILED ", fail, " TESTS *******************\n"
    sys.exit(fail)


