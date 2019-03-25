from __future__ import print_function
import threading
import requests
import time
import json
import sys
import hashlib
import datetime
from random import randint

requests.packages.urllib3.disable_warnings()
appid = "stressss"
apikey = "12345678901234567890123456789012"
url = "http://localhost:9002/1/indexes/stress"
query_url = url + "/query"
cfg_url = url + "/settings"
shutdown = False
headers = { 'X-Marlin-Application-Id': appid, 'X-Marlin-REST-API-KEY': apikey, 'Content-Type':'application/json'}
words = []
wlen = 0
facets = []
flen = 0
objects = {}
objcount = 1
primes = [5113, 6203, 7321, 7639, 7853, 1049, 1663, 673, 2657, 4019, 4339]
delcount = 0
replacecount = 0
verifycount = 0
tcount = [0,0,0,0,0,0,0,0]
requests.packages.urllib3.disable_warnings()

def do_query():
    word = words[randint(0, wlen)]
    word_len = len(word)
    word = word[:randint(1, word_len)]
    pd = {"q":word}
    try:
        r = requests.post(query_url, data=json.dumps(pd), headers=headers, verify=False)
        if r.status_code != 200:
            pass
            #print("Status code ", r.status_code)
        else:
            x = r.json()
            return x
    except UnicodeDecodeError:
        pass
        #print(word)
    return None

def req_thread(id):
    global shutdown
    while not shutdown:
        do_query()
	tcount[id] += 1
    print("Shutting down request thread! ")

def create_object_noid():
    obj = {}
    rint = randint(1, wlen)
    obj["word"] = words[rint]
    obj["word2"] = words[rint%primes[0]]
    obj["word3"] = words[rint%primes[2]]
    obj["word4"] = words[rint%primes[5]]
    rlen = rint % 128
    wlist = ""
    for i in xrange(0, rlen):
        wlist += words[randint(1, wlen)]
        wlist += " "
    obj["words"] = wlist
    rlen = rint % 32
    warr = []
    for i in xrange(0, rlen):
        warr.append(words[randint(1, wlen)])
    obj["wlist"] = warr
    obj["num"] = rint
    obj["num2"] = rint % primes[0]
    obj["num3"] = rint % primes[5]
    obj["num4"] = rint % primes[8]
    rlen = rint % 10
    narr = []
    for i in xrange(0, rlen):
        narr.append(rint % primes[i])
    obj["numlist"] = narr
    obj["facet"] = facets[rint % flen]
    warr = []
    for i in xrange(0, rlen):
        warr.append(facets[randint(1, flen)])
    obj["facetlist"] = warr
    obj["bool"] = True if rint % 2 else False
    return obj

def create_object():
    global objcount
    obj = create_object_noid()
    obj["_id"] = str(objcount)
    oid = objcount
    objcount += 1
    objects[oid] = obj
    return obj

def print_status():
    global delcount, replacecount, verifycount
    count = tcount[0] + tcount[1] + tcount[2] + tcount[3] + tcount[4] + tcount[5] + tcount[6] +tcount[7]
    print("Total :", len(objects), "Del :", delcount, "Repl :", replacecount, "Ver :", verifycount, "Req :", count, end='\r')

def post_objects(objlen):
    olist = []
    for i in xrange(0, objlen):
        olist.append(create_object())
    r = requests.post(url, data=json.dumps(olist), headers=headers, verify=False)
    x = r.json()
    print_status()

def delete_object():
    global delcount
    oid = randint(0, len(objects)-1)
    oid = objects.keys()[oid]
    if objects.has_key(oid):
        durl = url+ "/"+ str(oid)
        r = requests.delete(durl, headers=headers, verify=False)
        x = r.json()
        # Object may still not have been added.. try again
        if r.status_code == 200:
            del objects[oid]
            delcount += 1
            #print "Deleted ", oid, " pending ", len(objects)
            print_status()

def delete_objects():
    global delcount
    for i in range(25):
        if len(objects) == 0:
            return
	oid = randint(0, len(objects)-1)
	oid = objects.keys()[oid]
	if objects.has_key(oid):
	    durl = url+ "/"+ str(oid)
	    r = requests.delete(durl, headers=headers, verify=False)
	    x = r.json()
            # Object may still not have been added.. try again
	    if r.status_code == 200:
	        del objects[oid]
	        delcount += 1
                print_status()
                #print "Deleted ", oid, " pending ", len(objects)
    print_status()


def replace_object():
    global replacecount
    oid = randint(0, len(objects)-1)
    oid = objects.keys()[oid]
    if objects.has_key(oid):
        durl = url+ "/"+ str(oid)
        newobj = create_object_noid()
        newobj["_id"] = str(oid)
        r = requests.put(durl, data=json.dumps(newobj), headers=headers, verify=False)
        if r.status_code == 200:
            objects[oid] = newobj
    replacecount += 1
    print_status()

def clear_index():
    curl = url+ "/"+ "clear"
    r = requests.post(curl, headers=headers, verify=False)

def hash_obj(d):
    return hashlib.sha1(json.dumps(d, sort_keys=True)).hexdigest()

def verify_objects():
    global verifycount
    nojobs = False
    # First make sure no pending jobs are present as it can mess up with verification
    while not nojobs:
        surl = url+ "/"+ "info"
        s = requests.get(surl, headers=headers, verify=False)
        x = s.json()
        if x["numJobs"] == 0:
            nojobs = True
        time.sleep(1/100)
    x = do_query()
    if x:
        for hit in x["hits"]:
            del hit["_docid"]
            if objects.has_key(int(hit["_id"])):
                if (hash_obj(hit) != hash_obj(objects[int(hit["_id"])])):
                    print ("\nVERIFICATION FAILED")
                    print (json.dumps(hit, sort_keys=True))
                    print("\n")
                    print (json.dumps(objects[int(hit["_id"])], sort_keys = True))
            else:
                print("\nMISSING KEY ", hit["_id"])
        #print objects[x["id"]]
        print_status()
        verifycount += 1

# Adds and deletes
def stress_a():
    limit = 1000
    clear_index()
    global shutdown
    while not shutdown:
        if objcount < limit:
            post_objects(10)
        delete_object()
        if len(objects) == 0:
            shutdown = True

# Adds and replaces
def stress_b():
    limit = 1000
    clear_index()
    global shutdown
    global replacecount
    replace = True
    while not shutdown:
        if objcount < limit:
            post_objects(10)
        replace_object() if replace else delete_object()
        if replacecount >= limit:
            replace = False
        if len(objects) == 0:
            shutdown = True


# Adds / delete/ replace and verify
def stress_c(limit):
    clear_index()
    t1 = datetime.datetime.now()
    myshut = False 
    global objects, objcount, delcount, replacecount, verifycount
    objects = {}
    objcount = 1
    delcount = 0
    replacecount = 0
    verifycount = 0
    clear_index()
    print_status()
    post_objects(1000)
    global shutdown
    try:
      while not myshut:
        job = randint(0, 5)
        if job == 0:
            if objcount < limit:
                post_objects(100)
        elif job == 1:
            delete_objects()
        elif job == 2:
            replace_object()
        elif job == 3:
            delete_objects()
        elif job == 4:
            replace_object()
        elif job == 5:
            verify_objects()
        if len(objects) == 0:
            myshut = True
            shutdown = True
            print ("\nDone test with limit", limit) 
    except KeyboardInterrupt:
      shutdown = True
      sys.exit()

    t2 = datetime.datetime.now()
    td = t2-t1
    print ("Took ", td, "\n")

if __name__ == '__main__':
    with open("./facet.txt") as f:
        facets = f.read().splitlines()
        flen = len(facets) - 1
    with open("/usr/share/dict/words") as f:
        words = f.read().splitlines()
        wlen = len(words) - 1
    for i in xrange(0, 2):
        threading.Thread(target=req_thread, args=(i,)).start()
    stress_a()
    stress_b()
    stress_c(1000)
    #stress_c(10000)
    #stress_c(100000)
    #stress_c(1000000)
    #clear_index()
    #post_objects(100)

