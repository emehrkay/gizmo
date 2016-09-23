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


# create a few entites
mark = User({'name': 'mark'})
steve = User({'name': 'steve'})
knows = mapper.connect(mark, steve, Knows)


# Save your entities
async def example():
    mapper.save(knows)

    result = await mapper.send()

    # entities have been updated with data from graph ids
    print('mark: {}'.format(mark['id']))
    print('steve: {}'.format(steve['id']))
    print('knows: {}'.format(knows['id']))


asyncio.get_event_loop().run_until_complete(example())
```
