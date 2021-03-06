from .zk import NodeExistsException
from .zk import NoNodeException
from .zk import fix_path
from .zk import ALL_ACL
import traceback
import zookeeper
import time

TINY_SLEEP=0.01

class Meta(object):
  def __init__(self, dct):
    self.__ctime       = dct['ctime']
    self.__aversion    = dct['aversion']
    self.__numChildren = dct['numChildren']
    self.__version     = dct['version']
    self.__dataLength  = dct['dataLength']
    self.__mtime       = dct['mtime']
    self.__cversion    = dct['cversion']

  def __repr__(self):
    return '\n'.join([
      'Zoo Meta:',
      '  ctime:    %s' % self.ctime,
      '  mtime:    %s' % self.mtime,
      '  version:  %s' % self.version,
      '  dataLen:  %s' % self.dataLength,
      '  numChild: %s' % self.numChildren,
      ])

  @property
  def ctime(self):
    return self.__ctime

  @property
  def aversion(self):
    return self.__aversion

  @property
  def numChildren(self):
    return self.__numChildren

  @property
  def version(self):
    return self.__version

  @property
  def dataLength(self):
    return self.__dataLength

  @property
  def mtime(self):
    return self.__mtime

  @property
  def cversion(self):
    return self.__cversion

class Value(object):
  """Values from zookeeper have three states: node is good and has data
  (either content or children, depending on what this Value represents),
  node is deleted, and zookeeper hasn't told us yet. This uses None as the
  "node is deleted" state, anything else as the "we know the value" state, and
  if zookeeper hasn't told us yet, __val isn't set at all.
  """
  def get(self, timeout=5):
    """Read the value that zookeeper has stored for us. If the associated node
    is deleted, this will raise NoNodeException. If zookeeper doesn't tell us
    before <timeout> seconds have elapsed, OperationTimeoutException is
    raised.
    """
    value = self._wait(timeout)
    if value is None:
      time.sleep(0.1)
      value = self._wait(0)
      if value is None:
        raise zookeeper.NoNodeException
    return value

  def _wait(self, timeout=5):
    try:
      return self.__val
    except AttributeError:
      pass

    start = time.time()
    end   = start + timeout
    # Busy loop for a second
    while time.time() < min(start+1, end):
      try:
        return self.__val
      except AttributeError:
        time.sleep(TINY_SLEEP)

    while time.time() < end:
      try:
        return self.__val
      except AttributeError:
        time.sleep(0.1)

    raise zookeeper.OperationTimeoutException

  def _set(self, value):
    self.__val = value

class Node(object):
  @fix_path
  def __init__(self, path, zk):
    self.__path     = path
    self.__zk       = zk
    self.__value    = Value()
    self.__children = Value()
    self.__val_cbs  = {}
    self.__ch_cbs   = {}
    print path, "Node created"

  @property
  def path(self):
    return self.__path

  def value(self, timeout=5):
    """Get the value and metadata for this node. This will raise
    NoNodeException if the node doesn't exist.
    """
    timeout /= 2.0
    try:
      return self.__value.get(timeout)
    except zookeeper.OperationTimeoutException:
      if self.__zk.is_connected():
        # We are connected to zookeeper, and we have no value at all. Let's
        # try getting it again...
        self.__zk._aget(self.path)
      return self.__value.get(timeout)

  def children(self, timeout=5):
    """Get the children of this node. This raises NoNodeException if the node
    doesn't exist.
    """
    timeout /= 2.0
    try:
      return self.__children.get(timeout)
    except zookeeper.OperationTimeoutException:
      if self.__zk.is_connected():
        # We are connected to zookeeper, and we have no children at all. Let's
        # try getting them again...
        self.__zk._aget_children(self.path)
      return self.__children.get(timeout)

  def create(self, value='', await_update=1):
    """Create a node at this path; this will fail if this node already has
    data, or in all sorts of connection failure events.
    """
    try:
      self.value()
      raise NodeExistsException
    except NoNodeException:
      self.__zk._use_socket(lambda z:
          zookeeper.create(z, self.path, value, ALL_ACL, 0))
      self._wait_version(await_update, 0)

  def set(self, value, version, await_update=1):
    """Set the value to store at this node. If this node doesn't exist, this
    will raise NoNodeException; if the given version isn't the most recently
    stored in zookeeper, this will raise BadVersionException. Other server
    errors will raise other exceptions.

    To stomp over the value, regardless of what is stored in zookeeper, set
    version to -1.
    """
    self.__zk._use_socket(lambda z:
        zookeeper.set(z, self.path, value, version))
    self._wait_version(await_update, version+1)

  def delete(self, version, await_update=1):
    """Delete the node at this path. This can fail for all sorts of reasons:
    not empty, bad version, doesn't exist, various server problems. If the
    node should be deleted regardless of its current version, version can be
    given as -1.
    """
    self.__zk._use_socket(lambda z: zookeeper.delete(z, self.path, version))
    self._wait_version(await_update, -1)

  def addValueWatcher(self, key, fn):
    """Add a function to be called when the value in this node changes. This
    function will be called with (data, meta) when the node exists, and it
    will be called with None if the node's been deleted. Exceptions thrown by
    fn will be swallowed. The key parameter is used to remove the watcher.
    Keys must be unique; adding different functions with the same key will
    result in previous watchers being replaced.
    """
    self._add_cb("value", self.__val_cbs, key, fn)

  def addChildWatcher(self, key, fn):
    """Add a function to be called when the children of this node changes.
    This function will be called with [children] when the node exists, and it
    will be called with None if the node's been deleted. Exceptions thrown by
    fn will be swallowed. The key parameter is used to remove the watcher.
    Keys must be unique; adding different functions with the same key will
    result in previous watchers being replaced.
    """
    self._add_cb("child", self.__ch_cbs, key, fn)

  def delValueWatcher(self, key):
    """Remove the watcher that was added with the given key.
    """
    try:             del self.__val_cbs[key]
    except KeyError: pass

  def delChildWatcher(self, key):
    """Remove the watcher that was added with the given key.
    """
    try:             del self.__ch_cbs[key]
    except KeyError: pass

  def _add_cb(self, desc, dct, key, fn):
    def catcher(val):
      try:
        fn(val)
      except:
        print desc, "watcher callback threw this:"
        traceback.print_exc()
    dct[key]=catcher

  def _delete(self):
    """Only to be called by zk, update that this node is deleted.
    """
    try:
      already_deleted = (self.__value._wait(0) is None)
    except zookeeper.OperationTimeoutException:
      already_deleted = False

    if not already_deleted:
      # Only call the callbacks if we didn't already know that we were
      # deleted.
      for fn in self.__val_cbs.values():
        self.__zk._run_async(lambda: fn(None))
      for fn in self.__ch_cbs.values():
        self.__zk._run_async(lambda: fn(None))

    self.__value._set(None)
    self.__children._set(None)
    print self.path, "marked as deleted"

  def _val(self, value, meta):
    """Only to be called by zk, update this node's stored value.
    """
    meta = Meta(meta)
    stored = self._immed_raw_value()
    if (stored is None) or (stored[1].version != meta.version):
      for fn in self.__val_cbs.values():
        self.__zk._run_async(lambda: fn( (value, meta) ))
    self.__value._set( (value, meta) )
    print self.path, "value set"

  def _children(self, children):
    """Only to be called by zk, update this node's children.
    """
    existing = self._immed_raw_children()
    if (existing is None) or (existing != children):
      for fn in self.__ch_cbs.values():
        self.__zk._run_async(lambda: fn( children ))
    self.__children._set(children)
    print self.path, "children set"

  def _immed_raw_value(self):
    try:
      return self.__value._wait(0)
    except zookeeper.OperationTimeoutException:
      return None

  def _immed_raw_children(self):
    try:
      return self.__children._wait(0)
    except zookeeper.OperationTimeoutException:
      return None

  def _wait_version(self, timeout, version):
    """Wait for this node's version to be at least the given version. If the
    version is -1, this waits for the node to be deleted.
    """
    giveup = time.time() + timeout
    while time.time() < giveup:
      try:
        cur_version = self.value(0)[1].version
      except NoNodeException:
        cur_version = -1

      if version == cur_version:
        return
      if 0 <= version <= cur_version:
        return
      time.sleep(TINY_SLEEP)
