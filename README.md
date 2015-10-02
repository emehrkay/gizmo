Gizmo
=====
> This is still very alpha. Some of this documentation is incorrect/incomplete. It works, but I'd give it a second. I am in the process of making this already incomplete library a Tinkerpop3-only implementataion

Gizmo is a lightweight Python Object Graph Mapper (O.G.M.) for [Tinkerpop Blueprints' Rexster](http://www.tinkerpop.com) servers. 

##Table of Contents:


### About

Gizmo starts and ends with Rexster. It is made up of model, mapper, query, request, response, and other objects whose job is to convert pure Python to a Rexster string to be executed on a server.

Gizmo is a loose implementation of a [Data Mapper](). You have entites, mappers, and adapters (in the form of request objects). This means that the entity objects know nothing of storage, it cannot persist itself nor can it directly get more data than what was given to it. 

### Gremlin

Gremlin is the basis of all interactions between Gizmo and the Rexster server. Gizmo employs [Gremlinpy](https://github.com/emehrkay/gremlinpy) under the hood to aid with translating objects to scripts. 

If you're new to Gremlin there are a few good resources to check out that will help you get the concept and allow you to hit the ground running with this library.

* [Tinkerpop Gremlin](https://github.com/tinkerpop/gremlin/wiki) -- Tinkerpop's Gremlin documentation.
* [SQL2Gremlin](http://sql2gremlin.com) -- A site dedicated to explaing how you can convert some simple SQL into Gremlin/Groovy scripts.
* [GremlinDocs](http://gremlindocs.com) -- A site that goes over the core functions that you will use in your scripts.
* [Tinkerpop mailing list](https://groups.google.com/forum/#!forum/gremlin-users) -- These guys/gals are cool. 

After getting a grasp of the Gremlin/Groovy language, you can now begin to write scripts with [Gremlinpy](https://github.com/emehrkay/gremlinpy) (or without) and take full advantage of what Gizmo can do.

### Dependencies

* [Gremlinpy](https://github.com/emehrkay/gremlinpy) >= 0.2
* [Requests](http://docs.python-requests.org/en/latest/) -- If you're connecting via HTTP.
* [Rexpro](https://pypi.python.org/pypi/rexpro/) -- If you're connecting via the binary interface.

### Installation

    python setup.py install

### Quickstart

    from gizmo.entity import Vertex
    from gizmo.mapper import Mapper, GenericEdge
    from gizmo.request import Binary
    
    r = BinaryRequest('localhost', 8984, 'gizmo_test')
    g = Gremlin()
    m = Mapper(r, g)
    
    class User(Vertex):
        _allowed_undefined = True
        	
    u = User({'name': 'mark', 'sex': 'male'})
    g = User({'name': 'sadé', 'sex': 'female'})
    d = {'out_v': u, 'in_v': g, 'since': 'last year'}
    e = GenericEdge(d, 'girl')
    
    m.save(e) #this will CRUD all entites
    m.send() #builds query and sends to the server
    
    #the entities have been updated with the response from the server
    print u['_id'], e.data

### Entities

A [graph](http://en.wikipedia.org/wiki/Graph_(mathematics)) is defined as a representation of a set of objects where some pairs of objects are connected by links. The objects are commonly referred to as nodes or vertices and links as edges. Vertices are your objects and edges are the connections between your objects. 

Gizmo's entity module contians definitions for `Vertex` and `Edge` objects. You will extend these to create custom model definitions or you can use the `GenericVertex` for vertices and `GenericEdge` for edges.

#### Models

Gizmo allows you to interact with the graph server by either sending a string to the server, sending a Gremlinpy object, or by invoking and using models. Using the entity `Vertex` and `Edge` objects for your models will give you more power, flexibility, and control when writing your applications.

Gizmo uses the `_label` property to identify which entity should be loaded when the data is returned from the server, if it is undefined, or not found, Gizmo will attempt to load a `GenericVertex` or `GenericEdge` object. By default Gizmo uses the class name to fill in the `_label` property. This can be manually overwritten by defining a `_node_label` member on the entity. This is useful if you find yourself repeating entity names.

    class Article(Vertex):
        _node_label = 'some_article'
        title = String()
        content = String()
    
    #in another package
    class Article(Vertex):
        _node_label = 'some_other_article'
    
    
    ...

    
##### Fields

Gizmo entities comes with a few predefined fields that will help you structure and query your data once it is saved in your database. By default the fields member defines how your model's data is structured. 

If you want your model to have unstructured data, set the instance member `_allowed_undefined` to `True`. When this member is set to true and an undefined field is set, Gizmo will do its best to figure out what field type to use. 

> `GenericVertex` and `GenericEdge` have allowed_undefined set to True by default

**Field Types**

Gizmo ships with a few self-explanatory types for fields. The field object main job is to convert the data from a Python type to a Groovy type (if necessary). 

> You can always add more by extending `field.Field` and defining `to_python` and `to_graph` methods.

* String
* Integer
* Float
* Boolean
* Map -- converts a Python dict to Grooy map. {'key': 'val'} -> ['key': 'val']
* List -- converts a Python tuple or list to Groovy list. (1, 2, '3') -> [1, 2, '3']
* DateTime
* Enum -- this simply takes a pre-defined list and only allows its members to be used.

These are fields created and populated at class instantiation:

* GIZMO_MODEL _:String_ -- the model that is used for the entity
* GIZMO_CREATED _:DateTime_ -- the original date created. This cannot be overwritten
* GIZMO_MODIFIED _:DateTime_ -- this is updated with every save
* GIZMO_ID _:String_ -- the _id from the graph. It is a string because different graphs store ids differently. OrientDB's ids have a : in them
* GIZMO_LABEL _:String_ -- all edges have a _label member. This defines how the vertices are connected


##### Edges

### Mappers

Mapper objects are the real workhorses in Gizmo, it is the entry and exit points for all interactions between entities and the graph. 

#### Queries and Statements

You have the ability to send strings or Gremlin objects to the sever and entity `Vertex` or `Edge` objects are returned. 

    get_all_v = 'g.V'
    collection = mapper.send(script=get_all_v)
    
    for entity in collection:
        print entity.data
        
    get_specific = 'g.v(ID)'
    params = {'ID': 12}
    
    collection = mapper.send(script=get_specific, params=params)
    ...
  
You can augment the Gremlin object directly ([more details](https://github.com/emehrkay/gremlinpy)) and pass that as an argument instead.

    gremlin = Gremlin()
    
    g.V(12).out('knows')
    
    collection = mapper.send(gremlin=gremlin)
    
    ...
    
Statements are useful when you create complex queries often and want to reference that logic in multiple places.

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

#### Custom Mappers

The `Mapper` object acts as proxy for any `_GenericMapper` instances. When you write a custom mapper and subclass `_GenericMapper` you have to bind that mapper to an entity. 


    class MyCustomVertex(Vertex):
        my_name = String()


    class MyCustomVertexMapper(_GenericMapper):
        model = MyCustomVertex

Anytime an instance of `MyCustomVertex` is acted against via the main `Mapper`, all actions are routed through to the `MyCustomVertexMapper` object.

#### Traversal Object

The `mapper.Traversal` object allows you to build a query with a given `Vertex` or `Edge` as the starting point. Its main purpose is to bind the `Gremlin` instance with the `Mapper` and the given Entity.

### Future releases

I wasn't able to fit all of the features that I see will make this library useful in this initial relase.
