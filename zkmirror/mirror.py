from threading import Lock
import zookeeper
import json

from .node import Node
from .js import JsNode
from .zk import ZooKeeperException
from .zk import fix_path
from .zk import describe_state
from .zk import EXPIRED_SESSION_STATE
from .zk import CONNECTED_STATE
from .zk import CHANGED_EVENT
from .zk import CHILD_EVENT
from .zk import CREATED_EVENT
from .zk import DELETED_EVENT
from .zk import SESSION_EVENT
from .zk import NONODE
from .zk import ALL_ACL
from .zk import OK
from .zk import silence

class Mirror(object):
  def __init__(self, *servers):
    silence()
    if not servers:
      servers = ('localhost',)
    servers = list(servers)

    for idx, val in enumerate(servers):
      if isinstance(val, basestring):
        servers[idx] = (val, 2181)

    self.__initstr = ','.join('%s:%d' % pair for pair in servers)
    self.__zk      = -1 
    self.__state   = 0
    self.__nodes   = {}
    self.__nodelck = Lock()

    # List of actions that failed while we were not connected
    self.__pending = []

    self._reconnect()

  def fileno(self):
    return self.__zk

  @fix_path
  def get(self, path):
    try:
      return self.__nodes[path]
    except KeyError:
      with self.__nodelck:
        try:
          node = self.__nodes[path]
        except KeyError:
          node = Node(path, self)
          self._setup(node)
          self.__nodes[path] = node
        return node

  def get_json(self, path):
    return JsNode(self.get(path))

  @fix_path
  def create(self, path, value=None, flags=0):
    if not flags:
      node = self.get(path)
      node.create(value)
      return node
    path = zookeeper.create(self.__zk, path, value, ALL_ACL, flags)
    return self.get(path)

  def create_json(self, path, value, flags=0):
    return JsNode(self.create(path, json.dumps(value), flags))

  def _get_cb(self, path):
    def cb(_zk, status, value, meta):
      self._update_node(path, status, lambda node:
          node._val(value, meta))
    return cb

  def _ls_cb(self, path):
    def cb(_zk, status, children):
      self._update_node(path, status, lambda node: node._children(children))
    return cb

  def _update_node(self, path, status, fn):
    try:
      node = self.__nodes[path]
    except KeyError:
      return
    if status == OK:
      # This is the result of zookeeper returning good data, so _events has a
      # good watch established looking for changes to path
      fn(node)
    elif status == NONODE:
      # Tried to do a get on the path, but it's gone, so _event's watch
      # isn't any good. we need to set one for once it exists
      node._delete()
      self._try_zoo(lambda z: zookeeper.aexists(z, path, self._events))
    else:
      # Something (I assume connection-related) made the request fail. We'll
      # try again once we reconnect
      self.__pending.append(lambda z:
          zookeeper.aget(z, path, self._events, self._get_cb(path)))

  def _events(self, zk, event, state, path):
    if zk != self.__zk:
      return

    if event == CHANGED_EVENT:
      zookeeper.aget(self.__zk, path, self._events, self._get_cb(path))
    elif event == CHILD_EVENT:
      zookeeper.aget_children(self.__zk, path, self._events, self._ls_cb(path))
    elif event == CREATED_EVENT:
      zookeeper.aget(self.__zk, path, self._events, self._get_cb(path))
      zookeeper.aget_children(self.__zk, path, self._events, self._ls_cb(path))
    elif event == DELETED_EVENT:
      try:
        node = self.__nodes[path]
        node._delete()
        zookeeper.aexists(self.__zk, path, self._events)
      except KeyError:
        pass
    elif event == SESSION_EVENT:
      if state == EXPIRED_SESSION_STATE:
        self._reconnect()
      elif state == CONNECTED_STATE:
        if self.__state == EXPIRED_SESSION_STATE:
          # We just reconnected from a totally dead connection, so we need to
          # setup everything again
          for node in self.__nodes.values():
            self._setup(node)
        else:
          # Happy reconnection; just do the pending stuff
          while self.__pending:
            self.__pending.pop()(self.__zk)

      self.__state = state
      print 'My state is now', describe_state(self.__state)

  def _try_zoo(self, action):
    try:
      action(self.__zk)
    except (SystemError, ZooKeeperException):
      # self.__zk must really suck; we'll throw this in pending until we get a
      # new connection
      self.__pending.append(action)

  def _reconnect(self):
    oldzk        = self.__zk
    self.__zk    = zookeeper.init(self.__initstr, self._events)
    if oldzk >= 0:
      zookeeper.close(oldzk)

  def _setup(self, node):
    path = node.path
    print 'setting up node for', path
    self._try_zoo(lambda z:
        zookeeper.aget(z, path, self._events, self._get_cb(path)))
    self._try_zoo(lambda z:
        zookeeper.aget_children(z, path, self._events, self._ls_cb(path)))

