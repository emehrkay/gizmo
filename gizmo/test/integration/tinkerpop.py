import asyncio
import unittest
import random

from gremlinpy import Gremlin

from . import ConnectionTestCases, EntityTestCases, MapperTestCases
from gizmo import Mapper, Request, Collection, Vertex, Edge
from gizmo.mapper import EntityMapper


class BaseTests(unittest.TestCase):

    def setUp(self):
        self.request = Request('localhost', port=8182)
        self.gremlin = Gremlin('gizmo_testing')
        self.mapper = Mapper(self.request, self.gremlin)
        self.ioloop = asyncio.get_event_loop()
        super(BaseTests, self).setUp()


    def tearDown(self):
        super(BaseTests, self).tearDown()

    async def purge(self):
        script = "%s.V().map{it.get().remove()}" % self.gremlin.gv
        res = await self.mapper.query(script=script)

        return res


class ConnectionTests(BaseTests, ConnectionTestCases):
    pass


class EntityTests(EntityTestCases, BaseTests):
    pass


class MapperTests(MapperTestCases, BaseTests):
    pass


class CollectionTests(BaseTests):
    pass

class TraversalTests(BaseTests):
    pass


if __name__ == '__main__':
    unittest.main()
