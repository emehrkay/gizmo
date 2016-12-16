Gizmo
=====

> Alpha build of this library. The API may change while working toward a stable release

 Gizmo is a lightweight asynchronous Python 3.5+ Object Graph Mapper (O.G.M.) for the [Tinkerpop Rexster](http://tinkerpop.apache.org) graphs.


## About

Gizmo starts and ends with Gremlin/Groovy. It is made up of entity, mapper, query, request, response, and other objects whose job is to convert pure Python to a Gremlin/Groovy string to be executed on a server.


### QuickStart Example

```python
    import asyncio

    from gizmo import Request, Vertex, Edge, Mapper, String

    from gremlinpy import Gremlin


    # setup the connection
    req = Request('localhost', 8182)
    gremlin = Gremlin('g') # this should be whatever your graph name is
    mapper = Mapper(request=req, gremlin=gremlin)


    # define a few entity classes
    class User(Vertex):
        name = String()


    class Knows(Edge):
        pass


    # create a few entities
    mark = User({'name': 'mark'})
    steve = User({'name': 'steve'})
    knows = mapper.connect(mark, steve, Knows)


    # Save your entities
    async def example():
        mapper.save(knows) # saving the edge will save users if they needed

        result = await mapper.send()

        # entities have been updated with response data from the graph
        print('mark: {}'.format(mark['id']))
        print('steve: {}'.format(steve['id']))
        print('knows: {}'.format(knows['id']))


    asyncio.get_event_loop().run_until_complete(example())


    # or write a custom query

    async def custom():
        g = Gremlin()
        g.V()

        result = await mapper.query(gremlin=g)

        print(result) # gizmo.mapper.Collection object
        print(result[0]) # user object (because of the queries in prev example)

    asyncio.get_event_loop().run_until_complete(custom())


    # do whatever Gremlin/Groovy allows you to do

    async def random_java():
        script = 'def x = new Date(); x'

        r = await mapper.query(script=script)

        print(r[0]['response'].value) # whatever x evaluates to

    asyncio.get_event_loop().run_until_complete(random_java())
```

## Running tests

Test can be run via the setup file or directly with `python -m`.

```
python setup.py test
```

or

```
python -m unittest gizmo.test.entity
python -m unittest gizmo.test.mapper
python -m unittest gizmo.test.integration.tinkerpop
...
```

If you're going to run the integration tests, right now, both the name of the graph and the port are hard-coded into the suite. Make sure your Gremlin and Titan server use these settings:

__tinkerpop__
* graph: gizmo_testing
* port: 8182

__titan__
* graph: gizmo_testing
* port: 8182

