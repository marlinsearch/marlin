#!/bin/python
import random

facets = {}
facet = []
flen = 0
state = 0
rend = 10000
seed = random.randint(1, rend)
random.seed(seed)

with open("./facet.txt") as f:
    facet = f.read().splitlines()
    flen = len(facet) - 1


def get_facets():
    def get_rnd1():
        return random.randint(6, 50)
    def get_rnd2():
        return random.randint(0, 5)
    def get_rnd():
        return random.randint(51, flen)
    ret = [facet[get_rnd2()], facet[get_rnd1()], facet[get_rnd()], facet[get_rnd()]]
    for f in ret:
        if f in facets:
            facets[f] = facets[f] + 1
        else:
            facets[f] = 1
    return ret

print "{"
print '"count":%d,' % rend
print '"seed":%d,' % seed
print '"words": ["aaaaaaa","bbbbbbb","ccccccc","ddddddd","zzzzzzz","yyyyyyy","xxxxxxx","wwwwwww"],'
print '"facetonly": ["oneone","twotwo","threethree","fourfour"],'
print '"data":['
for i in xrange(1, rend+1):
    print "{"
    flist = get_facets()
    print '"facet": "%s",' % flist[0]
    print '"facetlist": ["%s","%s","%s"],' % (flist[1], flist[2], flist[3])
    if i%10 == 0:
        print '"bool":true,'
    else:
        print '"bool":false,'
    if i < 51:
        if i%5 == 0:
            print '"spell":"eeeeeee",'
        else:
            print '"spell":"eeeeeed",'
    if state == 0:
        print '"str":"aaaaaaa attr",'
        print '"strlist":["zzzzzzz"],'
        print '"facetonly":["oneone"],'
        print '"num": %d,' % ((rend+1)-i)
        print '"numlist": [%d, %d, %d],' % (i + rend*2, i+1+rend*2, i+3+rend*2)
    elif state == 1:
        print '"str":"aaaaaaa bbbbbbb attr",'
        print '"strlist":["zzzzzzz", "yyyyyyy"],'
        print '"facetonly":["oneone", "twotwo"],'
        print '"num": %d,' % ((rend+1)-i)
        print '"numlist": [%d, %d, %d],' % (i + rend*4, i+1+rend*4, i+3+rend*4)
    elif state == 2:
        print '"str":"aaaaaaa bbbbbbb ccccccc attr",'
        print '"strlist":["zzzzzzz", "yyyyyyy", "xxxxxxx"],'
        print '"facetonly":["oneone", "twotwo", "threethree"],'
        print '"num": %d,' % ((rend+1)-i)
        print '"numlist": [%d, %d, %d],' % (i + rend*6, i+1+rend*6, i+3+rend*6)
    elif state == 3:
        print '"str":"aaaaaaa bbbbbbb ccccccc ddddddd attr",'
        print '"strlist":["zzzzzzz", "yyyyyyy", "xxxxxxx", "wwwwwww"],'
        print '"facetonly":["oneone", "twotwo", "threethree", "fourfour"],'
        print '"num": %d,' % ((rend+1)-i)
        print '"numlist": [%d, %d, %d],' % (i + rend*8, i+1+rend*8, i+3+rend*8)
    state = state + 1
    if state > 3:
        state = 0
    print '"id":%d' % i
    print "} " if i==rend else "},"
print "],"
print '"facet_count":{'
vv = 0
for k, v in facets.iteritems():
    x = '"%s":%d,' % (k, v)
    vv += 1
    if vv == len(facets):
        x = x.strip(",")
    print x
print "},"
print '"facets":['
vv = 0
for f in facet:
    x = '"%s",' % f
    vv += 1
    if vv == len(facet):
        x = x.strip(",")
    print x
print "]"
print "}"
