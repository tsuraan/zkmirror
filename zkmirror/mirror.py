from threading import Thread, Lock
from Queue import Queue
import traceback
import zookeeper
import json
import time
import sys

from .chroot import ChrootMirror
from .node import Node
from .js import JsNode
from .zk import ZooKeeperException
from .zk import NodeExistsException
from .zk import NoNodeException
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

DEBUG=False
def debug(*args):
  global DEBUG
  if not DEBUG:
    return
  sys.stderr.write(' '.join(map(str, args)) + "\n")

class Mirror(object):
  def __init__(self):
    silence()
    self.__q       = Queue()
    self.__async   = Thread(target=run_tasks, args=(self.__q,))
    self.__async.daemon = True
    self.__async.start()

    self.__zk      = -1 
    self.__state   = 0

    self.__nodes   = {}
    self.__nodelck = Lock()
    self.__socklck = Lock()

    self.__missing = set()
    self.__misslck = Lock()

    self.__disconnected = time.time()

    self.__state_cbs = {}
    # List of actions that failed while we were not connected
    self.__pending = []

  def connstr(self):
    try:
      return self.__initstr
    except AttributeError:
      return None

  def connect(self, *servers):
    if not servers:
      servers = ('localhost',)
    servers = list(servers)

    for idx, val in enumerate(servers):
      if isinstance(val, basestring):
        servers[idx] = (val, 2181)

    self.__initstr = ','.join('%s:%d' % pair for pair in servers)
    self._reconnect()
    return self

  def time_disconnected(self):
    """Return how long we've been disconnected. Returns None if we are
    currently connected.
    """
    try:
      return time.time() - self.__disconnected
    except TypeError:
      # self.__disconnected is None
      return None

  def is_connected(self):
    """Returns True if we are currently connected to ZooKeeper, False if not.
    """
    return not self.__disconnected

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
          self.__nodes[path] = node
          self._setup(node)
        return node

  def get_json(self, path):
    return JsNode(self.get(path))

  @fix_path
  def create(self, path, value='', flags=0):
    if not flags:
      node = self.get(path)
      node.create(value)
      return node
    path = self._use_socket(lambda z:
        zookeeper.create(z, path, value, ALL_ACL, flags))
    return self.get(path)

  @fix_path
  def create_r(self, path, value=''):
    """Create the entire path up to this node, and then create this node"""
    pre = path.rsplit('/',1)[0]
    if pre:
      self.ensure_exists(pre)
    self.create(path, value)

  def create_json(self, path, value, flags=0):
    return JsNode(self.create(path, json.dumps(value), flags))

  def create_r_json(self, path, value):
    return JsNode(self.create_r(path, json.dumps(value)))

  @fix_path
  def ensure_exists(self, path, value=''):
    """Make sure every node, up to the given path, exists in zookeeper.
    """
    node = self.get(path)
    try:
      node.value(timeout=0.1)
    except NoNodeException:
      try:
        node.create(value)
      except NodeExistsException:
        # No problem; it exists
        pass
      except NoNodeException:
        # the parent doesn't exist
        self.ensure_exists(path.rsplit('/',1)[0])
        self.ensure_exists(path)
    return node

  def addStateWatcher(self, key, fn):
    """Add a function that will be called when our connection state changes.
    This function will be called with a zookeeper state variable (an int with
    one of the values of
    zookeeper.{AUTH_FAILED,EXPIRED_SESSION,CONNECTING,ASSOCIATING,CONNECTED}_STATE
    of the value 0 (shouldn't happen, but it does)
    """
    def catcher(val):
      try:
        fn(val)
      except:
        print 'state watcher callback threw this:'
        traceback.print_exc()
    self.__state_cbs[key] = catcher

  def delStateWatcher(self, key):
    """Remove the state watcher that was assigned at the given key.
    """
    try:             del self.__state_cbs[key]
    except KeyError: pass

  @fix_path
  def chroot(self, path):
    """Get a version of this mirror whose root has been changed to the given
    path. All create and get requests will have the given path prepended to
    them, and returned nodes will similarly have their path attributes
    changed.
    """
    return ChrootMirror(path, self)

  def _run_async(self, fn):
    """Functions that wait on results from zookeeper cannot be usefully called
    from within zookeeper callbacks, as the zookeeper receive socket is
    blocked until the callback returns. Any callback that waits on zookeeper
    data (calls to zookeeper.create, delete, exists, get, get_children wait,
    but the equivalent zookeeper.acreate, etc calls do not wait) should
    instead hand a thunk to this, so that the work can be done outside of the
    zookeeper callback.
    """
    self.__q.put(fn)

  def _events(self, zk, event, state, path):
    if event == CHANGED_EVENT:
      debug('_events: adding CHANGE watcher for', path)
      self._aget(path)
    elif event == CHILD_EVENT:
      debug('_events: adding CHILDREN watcher for', path)
      self._aget_children(path)
    elif event == CREATED_EVENT:
      debug('_events: adding CHANGE and CHILDREN watchers for', path)
      del_missing(self.__misslck, self.__missing, path)
      self._aget(path)
      self._aget_children(path)
    elif event == DELETED_EVENT:
      try:
        node = self.__nodes[path]
        node._delete()
        debug('_events: adding EXISTS watcher for', path)
        self._aexists(path)
      except KeyError:
        pass
    elif event == SESSION_EVENT:
      if zk != self.__zk:
        return

      for fn in self.__state_cbs.values():
        self._run_async(lambda: fn(state))

      if state == CONNECTED_STATE:
        self.__disconnected = None
      elif self.__disconnected is None:
        self.__disconnected = time.time()

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
            self.__pending.pop()()

      self.__state = state
      debug('_events: My state is now', describe_state(self.__state))

  def _reconnect(self):
    oldzk        = self.__zk
    self.__zk    = zookeeper.init(self.__initstr, self._events)
    if oldzk >= 0:
      zookeeper.close(oldzk)

  def _setup(self, node):
    path = node.path
    debug('_setup: adding CHANGE and CHILDREN watchers for', path)
    self._aget(path)
    self._aget_children(path)

  def _aget(self, path):
    self._try_zoo(
        lambda: self._use_socket(
          lambda z: zookeeper.aget(z, path, self._events, self._get_cb(path))))

  def _aget_children(self, path):
    self._try_zoo(
        lambda: self._use_socket(
          lambda z: zookeeper.aget_children(z, path, self._events,
            self._ls_cb(path))))

  def _aexists(self, path):
    if add_missing(self.__misslck, self.__missing, path):
      debug('_aexists is hooking in a callback on existence')
      watcher = self._events
    else:
      debug('_aexists is NOT hooking in a callback on existence')
      watcher = None

    self._try_zoo(
        lambda: self._use_socket(
          lambda z: zookeeper.aexists(z, path, watcher, self._exist_cb(path))))

  def _try_zoo(self, action):
    try:
      action()
    except (SystemError, ZooKeeperException):
      # self.__zk must be really broken; we'll throw this in pending until we
      # get a new connection
      self.__pending.append(action)

  def _get_cb(self, path):
    def cb(_zk, status, value, meta):
      self._update_node(
          path,
          status,
          lambda node: node._val(value, meta),
          lambda: self._aget(path))
    return cb

  def _ls_cb(self, path):
    def cb(_zk, status, children):
      self._update_node(
          path,
          status,
          lambda node: node._children(children),
          lambda: self._aget_children(path))
    return cb

  def _exist_cb(self, path):
    def cb(_zk, status, meta):
      if status == OK:
        # It started existing while our message was in transit; set up the
        # node's data and allow watch callbacks to occur on future aexist
        # calls
        del_missing(self.__misslck, self.__missing, path)
        self._aget(path)
        self._aget_children(path)
      elif status == NONODE:
        # This is what we expect; our watcher is set up, so we're happy
        pass
      else:
        # Something went wrong communication-wise (disconnect, timeout,
        # whatever). try again once re-connected. We need to remove the path
        # from __missing so that a future aexists call can put the watcher
        # back on
        del_missing(self.__misslck, self.__missing, path)
        self.__pending.append(lambda: self._aexists(path))

  def _update_node(self, path, status, node_action, on_servfail):
    try:
      node = self.__nodes[path]
    except KeyError:
      return
    if status == OK:
      # This is the result of zookeeper returning good data, so _events has a
      # good watch established looking for changes to path
      node_action(node)
    elif status == NONODE:
      # Tried to do a get on the path, but it's gone, so _event's watch
      # isn't any good. we need to set one for once it exists
      node._delete()
      debug('_update_node: adding EXISTS watcher for', path)
      self._aexists(path)
    else:
      # Something (I assume connection-related) made the request fail. We'll
      # try again once we reconnect
      self.__pending.append(on_servfail)

  def _use_socket(self, action):
    with self.__socklck:
      return action(self.__zk)

  def close(self):
    def quit_async():
      sys.exit(0)
    self._run_async(quit_async)
    print 'killed async thread'
    self.__async.join()
    print 'async thread done'
    if self.__zk >= 0:
      zookeeper.close(self.__zk)
      self.__zk = -1
      try:
        del self.__initstr
      except AttributeError:
        pass
    print 'zookeeper closed'

  def __del__(self):
    self.close()

def run_tasks(queue):
  try:
    while True:
      function = queue.get()
      try:
        function()
      except Exception:
        print 'zkmirror asynchronous task failed like this:'
        traceback.print_exc()
  finally:
    print 'run_tasks thread shutting down'

def add_missing(lock, missing, path):
  with lock:
    if path in missing:
      return False
    missing.add(path)
    return True

def del_missing(lock, missing, path):
  with lock:
    try:
      missing.remove(path)
    except KeyError:
      pass

