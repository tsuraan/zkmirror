from .zk import NodeExistsException
from .zk import NoNodeException
from .zk import fix_path
from .zk import ALL_ACL
import zookeeper
import time

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
      raise zookeeper.NoNodeException
    return value

  def _wait(self, timeout=5):
    start = time.time()
    end   = start + timeout
    # Busy loop for a second
    while time.time() < min(start+1, end):
      try:
        return self.__val
      except AttributeError:
        pass

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

  @property
  def path(self):
    return self.__path

  def get_value(self):
    """Get the value and metadata for this node. This will raise
    NoNodeException if the node doesn't exist.
    """
    return self.__value.get()

  def get_children(self):
    """Get the children of this node. This raises NoNodeException if the node
    doesn't exist.
    """
    return self.__children.get()

  def create(self, value=None):
    """Create a node at this path; this will fail if this node already has
    data, or in all sorts of connection failure events.
    """
    try:
      self.get_value()
      raise NodeExistsException
    except NoNodeException:
      zookeeper.create(self.__zk.fileno(), self.path, value, ALL_ACL, 0)

  def _delete(self):
    """Only to be called by zk, update that this node is deleted.
    """
    self.__value._set(None)
    self.__children._set(None)

  def _val(self, value, meta):
    """Only to be called by zk, update this node's stored value.
    """
    self.__value._set( (value, Meta(meta)) )

  def _children(self, children):
    """Only to be called by zk, update this node's children.
    """
    self.__children._set(children)

