import unittest
import random

from tornado.testing import AsyncTestCase, gen_test
from gremlinpy import Gremlin

from . import ConnectionTestCases, EntityTestCases, MapperTestCases
from gizmo import Mapper, Request, Collection, Vertex, Edge
from gizmo.mapper import _GenericMapper


class BaseTests(AsyncTestCase):

    def setUp(self):
        self.request = Request('localhost', 'gizmo_testing', port=8182)
        self.gremlin = Gremlin('gizmo_testing')
        self.mapper = Mapper(self.request, self.gremlin, logger=None)
        super(BaseTests, self).setUp()


    def tearDown(self):
        super(BaseTests, self).tearDown()

    def purge(self):
        script = "%s.V().map{it.get().remove()}" % self.gremlin.gv
        return self.mapper.query(script=script)


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
