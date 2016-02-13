import unittest
import random

from tornado.testing import AsyncTestCase, gen_test
from gremlinpy import Gremlin
from gizmo import Mapper, Request, Collection, Vertex, Edge
from gizmo.mapper import _GenericMapper
from .tinkerpop import (ConnectionTestCases, EntityTestCases, MapperTestCases)


class TitanBaseTests(AsyncTestCase):

    def setUp(self):
        super(TitanBaseTests, self).setUp()
        self.request = Request('localhost', 'gizmo_testing', port=9192)
        self.gremlin = Gremlin('gizmo_testing_titan')
        self.mapper = Mapper(self.request, self.gremlin, logger=False,
                             auto_commit=False,
                             graph_instance_name='gremlin_titan_graph')


    def tearDown(self):
        super(TitanBaseTests, self).tearDown()

    def purge(self):
        script = "%s.V().map{it.get().remove()}" % self.gremlin.gv
        return self.mapper.query(script=script)


class TitanConnectionTests(ConnectionTestCases, TitanBaseTests):
    pass


class TitanEntityTests(EntityTestCases, TitanBaseTests):
    pass


class TitanMapperTests(MapperTestCases, TitanBaseTests):
    pass

