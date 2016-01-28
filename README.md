Gizmo
=====
> This is still very alpha. Some of this documentation is incorrect/incomplete. It works, but I'd give it a second. I am in the process of making this already incomplete library a Tinkerpop3-only implementataion

Gizmo is a lightweight Python >=2.7 Object Graph Mapper (O.G.M.) for [Tinkerpop Blueprints' Rexster 3.x](http://www.tinkerpop.com) servers. 


### About

Gizmo starts and ends with Rexster. It is made up of model, mapper, query, request, response, and other objects whose job is to convert pure Python to a Rexster string to be executed on a server.

Gizmo is a loose implementation of a [Data Mapper](). You have entites, mappers, and adapters (in the form of request objects). This means that the entity objects know nothing of storage, it cannot persist itself nor can it directly get more data than what was given to it. 

### Gremlin

Gremlin is the basis of all interactions between Gizmo and the Rexster server. Gizmo employs [Gremlinpy](https://github.com/emehrkay/gremlinpy) under the hood to aid with translating objects to scripts. 

If you're new to Gremlin there are a few good resources to check out that will help you get the concept and allow you to hit the ground running with this library.

* [Tinkerpop Gremlin](http://tinkerpop.incubator.apache.org/docs/3.0.0-incubating/#traversal) -- Tinkerpop's Gremlin documentation.
* [SQL2Gremlin](http://sql2gremlin.com) -- A site dedicated to explaing how you can convert some simple SQL into Gremlin/Groovy scripts.
* [GremlinDocs](http://gremlindocs.com) -- A site that goes over the core functions that you will use in your scripts.
* [Tinkerpop mailing list](https://groups.google.com/forum/#!forum/gremlin-users) -- These guys/gals are cool. 

After getting a grasp of the Gremlin/Groovy language, you can now begin to write scripts with [Gremlinpy](https://github.com/emehrkay/gremlinpy) (or without) and take full advantage of what Gizmo can do.

### Dependencies

* [gremlinpy](https://github.com/emehrkay/gremlinpy)
* [gremlinclient](https://github.com/davebshow/gremlinclient)
* [tornado](https://github.com/tornadoweb/tornado)

### Installation

    python setup.py install
   
> pip/easy_install coming soon

### Quickstart

~~~python
from tornado.ioloop import IOLoop
from tornado import gen

from gizmo.entity import Vertex, GenericEdge
from gizmo.mapper import Mapper
from gizmo.request import Request

from gremlinpy import Gremlin


# grab an instance of the ioloop
loop = IOLoop.current()

# build the base mapper
r = Request('localhost', 8984, 'gizmo_test')
g = Gremlin('gizmo_testing')
m = Mapper(r, g)


# all of your work is done inside of the coroutine
@gen.coroutine
def run():
    script = '1 + 1' # run a simple script directly
    resp = yield m.query(script=script)

    print(resp, resp.first()['response']) # <gizmo.mapper.Collection object at #ID> 2

# run the code once on the ioloop
loop.run_sync(run)



# make a blocking-style request
from gizmo.utils import blocking

# callback returning a future
def cb(a, b):
    script = 'sleep(3000); {}+{}'.format(a, b)
    r =  m.query(script=script)

    # you can return a Future object
    return r

b = blocking(cb, 1, 3)
print(b.first()['response']) # in 3 seconds it will print out 4


# a callback that doesnt return a future
def cb_no_future(a, b):
    import time

    # return a Future object
    time.sleep(3)
    return a + b

b = blocking(cb_no_future, 1, 30)
print(b) # it will print out 31 in 3 seconds




# utilitize the model and mapper system
class User(Vertex):
    _allowed_undefined = True

# create a couple of users and connect them
u = User({'name': 'mark', 'sex': 'male'})
g = User({'name': 'sadé', 'sex': 'female'})
d = {'out_v': u, 'in_v': g, 'since': 'last year'}
e = GenericEdge(d, 'girlfriend')

m.save(e) #this will CRUD all entites

# run the script in a non-blocking fashion 
@gen.coroutine
def run():
    result = yield m.send() #builds query and sends to the server

    # the entities have been updated with the response from the server
    print(u['_id'], e.data) # 1 <some_id>, <OrderedDict>

loop.run_sync(run)

# or run it as a blocking call
result = blocking(m.send)
print(u['_id'], e.data) # 1 <some_id>, <OrderedDict>

# you can call Mapper.query with blocking
script = '1+5'
result = blocking(m.query, script=script)
print(result.first()['response']) # 6
~~~


### Testing

The test suite can be run by calling:

~~~python
python setup.py test
~~~

This will run all of the tests defined in the test package. However, you do need to update some settings in your local Gremlin server instance in order to run the integration tests.

* Add a new file 'tinkergraph-gizmo-testing.properties' to the `conf/` directory of your gremlin server outlining the settings for the testing graph 

~~~
gremlin.graph=org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph
gremlin.tinkergraph.vertexIdManager=LONG
gremlin.tinkergraph.edgeIdManager=LONG
~~~

* Open up the configuration yaml that you are using with your Gremlin server (gremlin-server.yaml by default) and find the `graphs` section. Add the gizmo\_test\_graph to it (take note of the `scripts` section and see which script is being called during startup of the server).

~~~
graphs: {
  graph: conf/tinkergraph-empty.properties,
  ...
  gizmo_test_graph: conf/tinkergraph-gizmo-testing.properties,
  }
~~~

* Gizmo operates on the graph's traversal instance. Add to the startup script (or create a new one) that exposes the gizmo_testing graph traversal to the global remote server instance.

~~~
gizmo_testing = gizmo_test_graph.traversal()
~~~

### Entities

A [graph](http://en.wikipedia.org/wiki/Graph_(mathematics\)) is defined as a representation of a set of objects where some pairs of objects are connected by links. The objects are commonly referred to as nodes or vertices and links as edges. Vertices are your objects and edges are the connections between your objects. 

Gizmo's entity module contians definitions for `Vertex` and `Edge` objects. You will extend these to create custom model definitions or you can use the `GenericVertex` for vertices and `GenericEdge` for edges.

#### Models

Gizmo allows you to interact with the graph server by either sending a string to the server, sending a Gremlinpy object, or by invoking and using models. Using the entity `Vertex` and `Edge` objects for your models will give you more power, flexibility, and control when writing your applications.

Gizmo uses the `_label` property to identify which entity should be loaded when the data is returned from the server, if it is undefined, or not found, Gizmo will attempt to load a `GenericVertex` or `GenericEdge` object. By default Gizmo uses the class name to fill in the `_label` property. This can be manually overwritten by defining a `_node_label` member on the entity. This is useful if you find yourself repeating entity names.

~~~python
class Article(Vertex):
    _node_label = 'some_article'
    title = String()
    content = String()

#in another package
class Article(Vertex):
    _node_label = 'some_other_article'


...
~~~
    
##### Fields

Gizmo entities comes with a few predefined fields that will help you structure and query your data once it is saved in your database. By default the fields member defines how your model's data is structured. 

If you want your model to have unstructured data, set the instance member `_allowed_undefined` to `True`. When this member is set to true and an undefined field is set, Gizmo will do its best to figure out what field type to use. 

> `GenericVertex` and `GenericEdge` have allowed_undefined set to True by default

**Field Types**

Gizmo ships with a few self-explanatory types for fields. The field object's main job is to convert the data from a Python type to a Groovy type (if necessary). 

> You can always add more by extending `field.Field` and defining `to_python` and `to_graph` methods.

* String
* Integer
* Float
* Boolean
* \* Map -- converts a Python dict to Grooy map. {'key': 'val'} -> ['key': 'val']
* \* List -- converts a Python tuple or list to Groovy list. (1, 2, '3') -> [1, 2, '3']
* DateTime
* Enum -- this simply takes a pre-defined list and only allows its members to be used.

> \* both Map and List do not handle the conversion to the Groovy type. This is done in the Query instance so that the values can be bound with the request. 

These are fields created and populated at class instantiation:

* GIZMO_MODEL _:String_ -- the model that is used for the entity
* GIZMO_CREATED _:DateTime_ -- the original date created. This cannot be overwritten
* GIZMO_MODIFIED _:DateTime_ -- this is updated with every save
* GIZMO_ID _:String_ -- the _id from the graph. It is a string because different graphs store ids differently. OrientDB's ids have a : in them
* GIZMO_LABEL _:String_ -- as of Tinkerpop3 all entities have a _label member. This defines how the vertices are connected

**Hooks**

<<<TALK ABOUT THE HOOKS>>>


##### Edges

### Mappers

Mapper objects are the real workhorses in Gizmo, it is the entry and exit points for all interactions between entities and the graph. 

A Mapper instance exposes a few key methods which allow you to interact with the graph:

* **save**(model<Entity>, data<Dict>, bind_return<Boolean>, mapper<_GenericMapper>, callback<callable>) -- this method is used to save the changes made againt the model to the graph
* **delete**(model<Entity>, mapper<_GenericMapper>, callback<callable>) -- will detele the entity from the graph
* **connect**(out_v<Entity>, in_v<Entity>, label<String>, data<Dict>, edge_model<Entity class>, data_type<String>) -- utility method used to create a connection between two entities. 
* **create_model (data<Dict>. model_class<Entity class>, data_type<String>) -- this method is used throughout the library to create actual instances of Entity objects. It uses some of the metadata that defined in the data argument to determine what type of Entity should be created

#### Queries and Statements

You have the ability to send strings or Gremlin objects to the sever and entity `Vertex` or `Edge` objects are returned. 

~~~python
get_all_v = 'g.V'
collection = mapper.send(script=get_all_v)

for entity in collection:
    print entity.data
    
get_specific = 'g.v(ID)'
params = {'ID': 12}

collection = mapper.send(script=get_specific, params=params)
...
~~~

You can augment the Gremlin object directly ([more details](https://github.com/emehrkay/gremlinpy)) and pass that as an argument instead.

~~~python
gremlin = Gremlin()

g.V(12).out('knows')

collection = mapper.send(gremlin=gremlin)

...
~~~

Statements are useful when you create complex queries often and want to reference that logic in multiple places.

~~~python
from gremlinpy.statement import Statement

#silly illustrative example
class HasOutVal(Statement):
    def __init__(self, out_val):
        self.out_val = out_val
     
    def build(self):
        self.gremlin.out(self.out_val)

# this will augment the gremlin instance on the mapper
mapper.apply_statement(HasOutVal('knows'))
mapper.send() #some query with your HasOutVal statement added
~~~

#### Custom Mappers

The `Mapper` object acts as proxy for any `_GenericMapper` instances. When you write a custom mapper and subclass `_GenericMapper` you have to bind that mapper to an entity. 

~~~python
class MyCustomVertex(Vertex):
    my_name = String()


class MyCustomVertexMapper(_GenericMapper):
    model = MyCustomVertex
~~~

Anytime an instance of `MyCustomVertex` is acted against via the main `Mapper`, all actions are routed through to the `MyCustomVertexMapper` object.

**Custom Properties**

Custom mappers allow you to define certain properties that will affect how Gizmo saves and retrieves entity data.

* **unique_fields** <List<String>> -- this is only applicable for Vertex objects. You can list the names of the fields that should be unique to the graph and the '_label' . Gizmo will run a query against the graph and determine if any vertieces are found with these properties, if there are results, an error is raised if **error_on_non_unique** is set to True.
* **unique** <Boolean> -- this is only applicable for Edge objects. When this is set to true, it will first check to see if an edge exists with the given label between the two vertices passed into the method. If an edge is found, Gizmo will use that edge instead of creating a new one.

**Callbacks**

Gizmo triggers callbacks at certain events in the mapper lifecycle (each callback recieves the entity as an argument). There are two levels of callbacks for a custom mapper/mapper: mapper-wide and one-time. Mapper-wide callbacks are defined on the custom mapper itself and will be triggered whenever a certain event happens on that mapper. The one-time callbacks as passed into the save or delete methods and are only executed once.

Mapper-wide callbacks:

* **on_create** -- this is called after the entity is crated
* **on_update** -- this is called after the entity is updated
* **on_delete** -- this is called after the entity is deleted

An example use-case for mapper-wide callbacks would be sending an email after a user is created:

```python
class UserMapper(_GenericMapper):
    model = User
    
    def on_create(self, user):
        # everytime a user is successfully crated, an email will be sent out
        send_email(user)
```

If you wanted to only send an email in certain situations, you would define the callback and pass it when you call save against the mapper

~~~python
user = mapper.create_model({})
def email_once(entity):
    send_email(entity)

mapper.save(user, callback=send_once).send() #sends email

user['email'] = 'somenew@email.address'
user.save(user).send() #does not send email
~~~

#### Shortcutting

The main purpose of Gizmo's custom mappers is to allow entity-specific functionality to be handled in one location. When entities are saved or deleted, Gizmo will first figure out if that entity has a customer mapper and use its respective save or delete methods. 

Utilitizing custom mappers could be a bit cumbersome; you have to get the entity, retrieve the custom mapper, and then call the method that you're looking for (again passing in the entity):

~~~python
class UserMapper(_GenericMapper):
    model = User

    def get_emails(self, user):
        g = self.mapper.start(user)
        g.outE('user_email')
        return self.mapper.query(gremlin=g)

user = User()
user_mapper = mapper.get_mapper(user)
emails = user_mapper.get_emails(user)
~~~

Making use of Python's magic methods, we can shortcut this process by calling the `get_emails` method against our main mapper instance. It will figure out which custom mapper to use and pass in any `*args` and `**kwargs`.

~~~python
user = User()
email = mapper.get_emails(user)
~~~

> Note: Gizmo assumes that methods defined on custom mappers all take the entity as the first argument. So to utilize this shortcut, the code must be written in this fashion

#### Traversal Object

The `mapper.Traversal` object allows you to build a query with a given `Vertex` or `Edge` as the starting point. Its main purpose is to bind the `Gremlin` instance with the `Mapper` and the given Entity.

~~~python
user = User() # assume its _id is 1
g = mapper.start(user)
print(g) # g.V(BOUND_VAR_FOR_ID) 

g.outE()
print(g) # g.V(BOUND_VAR_FOR_ID).outE()
~~~

**Future: ** I plan on adding depth-frst vs bredth-first traversals via filtering functions to this class.
