from __future__ import print_function
from .mirror import Mirror
from .zk import ZooServerProblem
import zookeeper
import time
import uuid
import sys

args = sys.argv[1:]
if '--debug' in args:
  import zkmirror.mirror as M
  M.DEBUG=True
  args.remove('--debug')

if args == ['states']:
  pairs = [(getattr(zookeeper, attr), attr) for attr in dir(zookeeper)
      if attr.endswith('_STATE')]
  for pair in sorted(pairs):
    print("%4d %s" % pair)
elif args == ['events']:
  pairs = [(getattr(zookeeper, attr), attr) for attr in dir(zookeeper)
      if attr.endswith('_EVENT')]
  for pair in sorted(pairs):
    print("%4d %s" % pair)
elif args == ['consts']:
  pairs = [(getattr(zookeeper, attr), attr) for attr in dir(zookeeper)
      if attr.upper() == attr]
  for pair in sorted(pairs):
    print("%4d %s" % pair)
elif args == ['exceptions']:
  def _go(exc, indent):
    print('%s%s' % ('  '*indent, exc.__name__))
    for child in sorted(exc.__subclasses__(), key=str):
      _go(child, indent+1)
  _go(zookeeper.ZooKeeperException, 0)
  _go(ZooServerProblem, 0)
elif args == ['functions']:
  things = [attr for attr in dir(zookeeper)
      if attr == attr.lower() and not attr.startswith('_')]
  for attr in things:
    print(attr)
else:
  m=Mirror().connect()
  root=m.get('/')
  root.addChildWatcher(uuid.uuid4(),
      lambda ch: print("root children:", ch))
  root.addValueWatcher(uuid.uuid4(),
      lambda val: print("root value:", val))
  other=m.get('/foo')
  other.addChildWatcher(uuid.uuid4(),
      lambda ch: print("other children:", ch))
  other.addValueWatcher(uuid.uuid4(),
      lambda val: print("other value:", val))

  missing=m.get("/missing")
  missing.create("value")
  (value, meta) = missing.value()
  print(value, meta)
  missing.set("new value", meta.version)
  (new_value, new_meta) = missing.value()
  print(new_value, new_meta)
  missing.delete(new_meta.version)

  while True:
    time.sleep(5)

