from zookeeper import ApiErrorException
from zookeeper import AuthFailedException
from zookeeper import BadArgumentsException
from zookeeper import BadVersionException
from zookeeper import ClosingException
from zookeeper import ConnectionLossException
from zookeeper import DataInconsistencyException
from zookeeper import InvalidACLException
from zookeeper import InvalidCallbackException
from zookeeper import InvalidStateException
from zookeeper import MarshallingErrorException
from zookeeper import NoAuthException
from zookeeper import NoChildrenForEphemeralsException
from zookeeper import NoNodeException
from zookeeper import NodeExistsException
from zookeeper import NotEmptyException
from zookeeper import NothingException
from zookeeper import OperationTimeoutException
from zookeeper import RuntimeInconsistencyException
from zookeeper import SessionExpiredException
from zookeeper import SessionMovedException
from zookeeper import SystemErrorException
from zookeeper import UnimplementedException
from zookeeper import ZooKeeperException

from zookeeper import ASSOCIATING_STATE
from zookeeper import AUTH_FAILED_STATE
from zookeeper import CONNECTED_STATE
from zookeeper import CONNECTING_STATE
from zookeeper import EXPIRED_SESSION_STATE

from zookeeper import APIERROR
from zookeeper import MARSHALLINGERROR
from zookeeper import SYSTEMERROR

from zookeeper import CHANGED_EVENT
from zookeeper import CHILD_EVENT
from zookeeper import CREATED_EVENT
from zookeeper import DELETED_EVENT
from zookeeper import NOTWATCHING_EVENT
from zookeeper import SESSION_EVENT

from zookeeper import AUTHFAILED
from zookeeper import BADARGUMENTS
from zookeeper import BADVERSION
from zookeeper import CLOSING
from zookeeper import CONNECTIONLOSS
from zookeeper import DATAINCONSISTENCY
from zookeeper import EPHEMERAL
from zookeeper import INVALIDACL
from zookeeper import INVALIDCALLBACK
from zookeeper import INVALIDSTATE
from zookeeper import LOG_LEVEL_DEBUG
from zookeeper import LOG_LEVEL_ERROR
from zookeeper import LOG_LEVEL_INFO
from zookeeper import LOG_LEVEL_WARN
from zookeeper import NOAUTH
from zookeeper import NOCHILDRENFOREPHEMERALS
from zookeeper import NODEEXISTS
from zookeeper import NONODE
from zookeeper import NOTEMPTY
from zookeeper import NOTHING
from zookeeper import OK
from zookeeper import OPERATIONTIMEOUT
from zookeeper import PERM_ADMIN
from zookeeper import PERM_ALL
from zookeeper import PERM_CREATE
from zookeeper import PERM_DELETE
from zookeeper import PERM_READ
from zookeeper import PERM_WRITE
from zookeeper import RUNTIMEINCONSISTENCY
from zookeeper import SEQUENCE
from zookeeper import SESSIONEXPIRED
from zookeeper import SESSIONMOVED
from zookeeper import UNIMPLEMENTED
import zookeeper

import functools

class ZooServerProblem(Exception):
  """All the zookeeper exceptions that indicate a problem with the actual
  ZooKeeper server (or our connection to it) are (coerced into becoming)
  subclasses of this exception.
  """

ApiErrorException.__bases__          += (ZooServerProblem,)
BadVersionException.__bases__        += (ZooServerProblem,)
ClosingException.__bases__           += (ZooServerProblem,)
ConnectionLossException.__bases__    += (ZooServerProblem,)
DataInconsistencyException.__bases__ += (ZooServerProblem,)
InvalidStateException.__bases__      += (ZooServerProblem,)
OperationTimeoutException.__bases__  += (ZooServerProblem,)
SessionExpiredException.__bases__    += (ZooServerProblem,)
SessionMovedException.__bases__      += (ZooServerProblem,)
SystemErrorException.__bases__       += (ZooServerProblem,)

def fix_path(fn):
  """Don't want to describe this. makes paths pretty.
  """
  def wrapper(self, path, *args):
    return fn(self, '/' + '/'.join(filter(None, path.split('/'))), *args)
  functools.update_wrapper(wrapper, fn)
  return wrapper

def silence(__once=[]):
  if __once:
    return
  __once.append(0)

  zookeeper.set_debug_level(zookeeper.LOG_LEVEL_ERROR)
  zookeeper.set_log_stream(file("/dev/null", "w"))

def describe_state(number, __cached={}):
  if not __cached:
    _populate_names(__cached, '_STATE')
  return __cached.get(number, 'CONFUZZLED')

def describe_event(number, __cached={}):
  if not __cached:
    _populate_names(__cached, '_EVENT')
  return __cached.get(number, 'ALARM_CLOCK')

def _populate_names(dct, ending):
  for state in dir(zookeeper):
    if not state.endswith(ending):
      continue
    dct[getattr(zookeeper, state)] = state[:-6]

ALL_ACL = [{"perms":0x1f, "scheme":"world", "id" :"anyone"}]

__all__ = [
    ApiErrorException,
    AuthFailedException,
    BadArgumentsException,
    BadVersionException,
    ClosingException,
    ConnectionLossException,
    DataInconsistencyException,
    InvalidACLException,
    InvalidCallbackException,
    InvalidStateException,
    MarshallingErrorException,
    NoAuthException,
    NoChildrenForEphemeralsException,
    NoNodeException,
    NodeExistsException,
    NotEmptyException,
    NothingException,
    OperationTimeoutException,
    RuntimeInconsistencyException,
    SessionExpiredException,
    SessionMovedException,
    SystemErrorException,
    UnimplementedException,
    ZooKeeperException,

    ASSOCIATING_STATE,
    AUTH_FAILED_STATE,
    CONNECTED_STATE,
    CONNECTING_STATE,
    EXPIRED_SESSION_STATE,

    APIERROR,
    MARSHALLINGERROR,
    SYSTEMERROR,

    CHANGED_EVENT,
    CHILD_EVENT,
    CREATED_EVENT,
    DELETED_EVENT,
    NOTWATCHING_EVENT,
    SESSION_EVENT,

    AUTHFAILED,
    BADARGUMENTS,
    BADVERSION,
    CLOSING,
    CONNECTIONLOSS,
    DATAINCONSISTENCY,
    EPHEMERAL,
    INVALIDACL,
    INVALIDCALLBACK,
    INVALIDSTATE,
    LOG_LEVEL_DEBUG,
    LOG_LEVEL_ERROR,
    LOG_LEVEL_INFO,
    LOG_LEVEL_WARN,
    NOAUTH,
    NOCHILDRENFOREPHEMERALS,
    NODEEXISTS,
    NONODE,
    NOTEMPTY,
    NOTHING,
    OK,
    OPERATIONTIMEOUT,
    PERM_ADMIN,
    PERM_ALL,
    PERM_CREATE,
    PERM_DELETE,
    PERM_READ,
    PERM_WRITE,
    RUNTIMEINCONSISTENCY,
    SEQUENCE,
    SESSIONEXPIRED,
    SESSIONMOVED,
    UNIMPLEMENTED,
    
    ZooServerProblem,
    fix_path,
    silence,
    describe_state,
    describe_event,
    ]

# Zookeeper behaviour notes:
# 
# zk = zookeeper.init(...):
#  with zookeeper running, state is CONNECTING
#  with zookeeper unresponsive, state is 0 or ASSOCIATING
#  with zookeeper dead, state is 0 or ASSOCIATING
#
# state == CONNECTING:
#  when zookeeper starts responding, state = CONNECTED, evt = CONNECTED
#
# state == CONNECTED:
#  when zookeeper disconnects, state = CONNECTING or 0, evt = CONNECTING
#  when zookeeper stops, state = 0 or ASSOCIATING, evt=CONNECTING
#
# state == 0 or ASSOCIATING:
#  if zookeeper comes up quickly, state = CONNECTED, evt = CONNECTED
#  if zookeeper takes too long, state = EXPIRED_SESSION, evt = EXPIRED_SESSION

