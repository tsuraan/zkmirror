from .zk import BadVersionException
from .zk import NodeExistsException
from .zk import NoNodeException
import json

class JsNode(object):
  def __init__(self, node):
    self.__node = node

  def value(self, timeout=5):
    """Get the decode JSON value of whatever is stored at this node, and its
    metadata.
    """
    val, meta = self.__node.value(timeout)
    return json.loads(val), meta

  def create(self, value):
    """Create data at this path with the JSON encoding of the given value.
    """
    self.__node.create(json.dumps(value))

  def set(self, value, version):
    """Set the value stored in zookeeper to the JSON encoding of the given
    value.
    """
    self.__node.set(json.dumps(value), version)

  def update(self, updater):
    """the given updater function will be called on whatever is currently
    stored in zookeeper, and its result will be written to zookeeper. If the
    node doesn't exist, updater will be called with None as its argument.
    """
    while True:
      try:
        stored, meta = self.value()
      except NoNodeException:
        stored = None

      replacement = updater(stored)
      if stored is None:
        try:
          self.create(replacement)
          return
        except NodeExistsException:
          continue
      else:
        try:
          self.set(replacement, meta.version)
          return
        except BadVersionException:
          continue

  def addValueWatcher(self, key, fn):
    def decoder(value):
      if value is not None:
        value = (json.loads(value[0]), value[1])
      fn(value)
    self.__node.addValueWatcher(key, decoder)

  def __getattr__(self, attr):
    """Ghetto Inheritance FTW!"""
    return getattr(self.__node, attr)

