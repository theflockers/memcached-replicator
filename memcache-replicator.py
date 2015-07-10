#!/usr/bin/env python

import re
import sys
import socket
import random
import asyncore
import memcache
import setproctitle

# hosts
memcacheds = ( ['localhost:11275'],['localhost:11276'] )

conns = []

class MemcachedReplica():
 
  global conns

  def __init__(self, memcached_hosts):
    for pair in memcached_hosts:
      if len(conns) < len(memcacheds):
        print 'connecting to:', pair
        conns.append(memcache.Client(pair))

    sys.stdout.flush()
 
  def cmd_GET(self, key):
    mc = conns[random.randint(0, len(conns)-1)]
    result = mc.get(key)
    if result:
      response = "VALUE %s 0 %d\r\n%s\r\nEND\r\n" % (key, len(result), result)
    else:
      response = 'END\r\n'
    return response

  def cmd_DELETE(self, key):
    mc = conns[random.randint(0, len(conns)-1)]
    result = mc.delete(key)
    if result:
      response = "DELETED\r\n"
    else:
      response = 'NOT_FOUND\r\n'
    return response


  def cmd_SET(self, args):
    args = eval(args)
    params = args[0].split()
    for conn in conns:
      print 'what set?:', params
      print 'args', params[0], params[3]
      result = conn.set(params[0], args[1], time=int(params[3]) )
      print result
      if result:
        response = 'STORED'
      sys.stdout.flush()
    return '%s\r\n' % (response)
    
  def send_command(self, cmd_and_args):
    f_args = None
    try:
      print type(cmd_and_args)
      if type(cmd_and_args) is str:
        f_args = eval(cmd_and_args)
        print "f_args", f_args
        cmd_and_args = f_args[0]

      cmd = cmd_and_args[0]
      if f_args:
        mc_args = ' '.join(cmd_and_args[1:len(cmd_and_args)])
        args = '("%s", "%s")' % (mc_args, f_args[1])
      else:
        args = ' '.join(cmd_and_args[1:len(cmd_and_args)])
      sys.stdout.flush()
      print "self.cmd_%s('%s')" % (cmd.upper(), args)
      res = eval("self.cmd_%s('%s')" % (cmd.upper(), args))
      return res
    except Exception, e:
      print str(e)
      sys.stdout.flush()
      return str(e)

class MemcachedHandler(asyncore.dispatcher_with_send):

  global memcacheds

  wait = None

  def handle_read(self):
    mcr = MemcachedReplica(memcacheds)
    data = self.recv(8192)
    print 'waiting?', self.wait
    if data and self.wait != None:
       print 'setting', self.wait, 'with', data
       args = "(%s, '%s')" % (str(self.wait), data.strip())
       res = mcr.send_command(args)
       self.wait = None
       self.send(res)
    elif data and not self.wait:
      cmd_and_args = data.split()
      print 'cmd_and_args', cmd_and_args
      if cmd_and_args[0].upper() == 'SET':
         if re.search('\r\n', data):
           send_data = data.split('\r\n')
           print 'send_data', send_data
           print 'setting', send_data[0], 'with', send_data[1]
           args = "(%s, '%s')" % (send_data[0].split(), send_data[1])
           res = mcr.send_command(args)
           self.send(res)
         else:
           self.wait = cmd_and_args


      elif cmd_and_args[0].upper() == 'QUIT':
        self.close()
      else:
        self.send(mcr.send_command(cmd_and_args))
           

class MemcachedServer(asyncore.dispatcher):
  def __init__(self, host, port):
    asyncore.dispatcher.__init__(self)
    self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
    self.set_reuse_addr()
    self.bind( (host,  port) )
    self.listen(10)

  def handle_accept(self):
    pair = self.accept()
    if pair is not None:
      sock, addr = pair
      handler = MemcachedHandler(sock)

if __name__ == '__main__':
  setproctitle.setproctitle('memcached-replicator')
  server = MemcachedServer('localhost', 11274)
  asyncore.loop()
