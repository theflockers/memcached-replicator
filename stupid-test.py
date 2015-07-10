#!/usr/bin/env python

import memcache
import random
import time,sys

mc  = memcache.Client(['localhost:11274'], debug=2)
mc1 = memcache.Client(['localhost:11275'], debug=2)
mc2 = memcache.Client(['localhost:11276'], debug=2)

while True:
  key = str(random.randint(111111111111111111111111111111111111111111111,9999999999999999999999999999999999999999999999))
  try:
    mc.set(key, '%s %s %s' % (key, key, key))
    print 'mc1:', mc1.get(key)
    print 'mc2:', mc2.get(key)
    print 'delete mc1:', mc1.delete(key)
    print 'delete mc2:', mc2.delete(key)
  except KeyboardInterrupt, e:
    print 'delete mc1:', mc1.delete(key)
    print 'delete mc2:', mc2.delete(key)
    break
  #time.sleep(0.1)
