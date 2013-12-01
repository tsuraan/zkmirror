from .mirror import Mirror
from .zk import ZooServerProblem
import zookeeper
import time
import sys

args = sys.argv[1:]
if sys.argv[1:] == ['states']:
  for attr in dir(zookeeper):
    if attr.endswith('_STATE'):
      print attr
elif sys.argv[1:] == ['events']:
  for attr in dir(zookeeper):
    if attr.endswith('_EVENT'):
      print attr
elif sys.argv[1:] == ['exceptions']:
  def _go(exc, indent):
    print '%s%s' % ('  '*indent, exc.__name__)
    for child in sorted(exc.__subclasses__(), key=str):
      _go(child, indent+1)
  _go(zookeeper.ZooKeeperException, 0)
  _go(ZooServerProblem, 0)
elif sys.argv[1:] == ['functions']:
  things = [attr for attr in dir(zookeeper)
      if attr == attr.lower() and not attr.startswith('_')]
  for attr in things:
    print attr
else:
  from .mirror import Mirror
  m=Mirror()
  n=m.get('/')
  while True:
    print n.get_value()
    print n.get_children()
    time.sleep(5)

