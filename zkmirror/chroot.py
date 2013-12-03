from .zk import fix_path

class ChrootMirror(object):
  """A Mirror-like class that prepends a fixed path onto any path requests,
  and that returns Node-like objects that similarly modify their path
  attribute.
  """
  @fix_path
  def __init__(self, path, mirror):
    self.__chroot          = path
    self.__mirror          = mirror
    self.time_disconnected = mirror.time_disconnected
    self.is_connected      = mirror.is_connected

  @fix_path
  def get(self, path):
    chrooted = self.__chroot + path
    return ChrootNode(self.__chroot,
        self.__mirror.get(chrooted))

  @fix_path
  def get_json(self, path):
    chrooted = self.__chroot + path
    return ChrootNode(self.__chroot,
        self.__mirror.get_json(chrooted))

  @fix_path
  def create(self, path, value='', flags=0):
    chrooted = self.__chroot + path
    return ChrootNode(self.__chroot,
        self.__mirror.create(chrooted, value, flags))

  @fix_path
  def create_r(self, path, value=''):
    chrooted = self.__chroot + path
    return ChrootNode(self.__chroot,
        self.__mirror.create_r(chrooted, value))

  @fix_path
  def create_json(self, path, value, flags=0):
    chrooted = self.__chroot + path
    return ChrootNode(self.__chroot,
        self.__mirror.create_json(chrooted, value, flags))

  @fix_path
  def create_r_json(self, path, value):
    chrooted = self.__chroot + path
    return ChrootNode(self.__chroot,
        self.__mirror.create_r_json(chrooted, value))

  @fix_path
  def ensure_exists(self, path, value=''):
    chrooted = self.__chroot + path
    return ChrootNode(self.__chroot,
        self.__mirror.ensure_exists(chrooted, value))

class ChrootNode(object):
  """A Node-like class that wraps Nodes and fakes their "path" attribute to
  not include a path base.
  """
  @fix_path
  def __init__(self, path, node):
    self.__chroot = path
    self.__node   = node

  @property
  def path(self):
    p = self.__node.path
    if p == self.__chroot or p.startswith(self.__chroot+'/'):
      p = p[len(self.__chroot):]
    if not p:
      p = '/'
    return p

  def __getattr__(self, attr):
    """Pass everything else through to wrapped node
    """
    return getattr(self.__node, attr)

