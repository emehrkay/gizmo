Gizmo
=====

Gizmo is a lightweight Python Object Graph Mapper (O.G.M.) for [Tinkerpop Blueprints' Rexster](http://www.tinkerpop.com) servers. 

##Table of Contents:


### About

Gizmo starts and ends with Rexster. It is made up of model, mapper, query, request, response, and other object whose jobe is to convert pure Python to a Rexster string to be executed on a server.

### Gremlin

Gremlin is the basis of all interactions between Gizmo and the Rexster server. Gizmo employs [Gremlinpy]() under the hood to aid with translating objects to scripts. 

If you're new to Gremlin there are a few good resources to check out that will help you get the concept and allow you to hit the ground running with this library.

* [SQL2Gremlin](http://sql2gremlin.com) -- A site dedicated to explaing how you can convert some simple SQL into Gremlin/Groovy scripts.
* [GremlinDocs](http://gremlindocs.com) -- A site that goes over the core functions that you will use in your scripts.
* [Tinkerpop mailing list](https://groups.google.com/forum/#!forum/gremlin-users) -- These guys/gals are cool. 

### Dependencies

Rexster servers can be acc

### Quickstart

### Entities

#### Models

#### Edges

### Mappers

#### Custom Mappers

#### Queries and Statements

#### Traversal Object

The `mapper.Traversal` object allows you to build a query with a given `Vertex` or `Edge` as the starting point. Its main purpose is to bind the `Gremlin` instance with the `Mapper` and the given Entity.

### Future releases

I wasn't able to fit all of the features that I see will make this library useful in this initial relase.
