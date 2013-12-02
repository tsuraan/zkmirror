# zkmirror Overview

ZkMirror is a client for Apache ZooKeeper. Its primary goal is to wrap
ZooKeeper nodes in objects that are always consistent with the data stored
within zookeeper. It does this using zookeeper watches, and it also makes an
attempt to handle server disconnects, connection timeouts, and other
edge-cases.

# Usage

The connection to ZooKeeper is managed through the zkmirror.Mirror class. Its
constructor takes a list of server addresses, or (address, port) pairs. If
listed servers cannot be resolved to IP addresses, the constructor will raise;
otherwise, it will always succeed:

```python
import zkmirror
mirror = zkmirror.Mirror()
```

Once a client is created, node instances can be created with the ```get```
method:

```python
root = mirror.get("/")
```

```get``` will always return a node, whether the named path exists in
ZooKepeer or not. A node's value can be read with the ```value()``` method,
and its children can be listed with its ```children()``` method. If the node
doesn't exist in zookeeper, these will fail by raising ```NoNodeException```s.
If zookeeper has not been reached since the node was created, the calls will
fail with ```OperationTimeoutException``` after a certain timeout has elapsed.
As the node's value and children change on the server, ZooKeeper informs the
client, and the nodes' contents are updated.

```python
root_val  = root.value()
root_cont = root.children()

missing = mirror.get("/missing")
missing.value()    # throws NoNodeException
missing.children() # throws NoNodeException
```

A non-existent node can be created with a node's ```create(...)``` method.

```python
missing.create("value")
missing.value()    # ("value", meta)
missing.children() # []
```

Finally, nodes can have their values changed and be deleted with the
```set(...)``` and ```delete(...)``` methods. These both take a version
argument, which can be retreived from the meta returned by ```get(...)```.

```python
(value, meta) = missing.value()
missing.set("new_value", meta.version)
(new_value, new_meta) = missing.value()
missing.delete(new_meta.version)
```

The ```addValueWatcher(..)``` and ```addChildWatcher(...)``` methods can be
used to call functions as ZooKeeper pushes value updates. Value watchers get
called with (value, meta) pairs, or None if the node doesn't exist. Child
watchers get called with [children] lists, or None if the node doesn't exist.

```python
import uuid
x=[]
root.addValueWatcher(uuid.uuid4(), lambda (val,meta): x.append(val))
root.set('stuff', -1)
print x # ['stuff']
```

The final purpose of zkmirror is that it does a decent job of handling
connection failures and timeouts between the client and ZooKeeper. This is
probably hard to demonstrate in a text file, so I won't try, but on
disconnections that require the creation of a new ZooKeeper connection, all
status watches are re-established, and updated nodes have their watchers
called.

