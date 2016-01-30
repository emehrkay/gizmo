import unittest
import random

from tornado.testing import AsyncTestCase, gen_test
from gremlinpy import Gremlin
from gizmo import Mapper, Request, Collection, Vertex, Edge
from gizmo.mapper import _GenericMapper


class BaseTests(AsyncTestCase):

    def setUp(self):
        self.request = Request('localhost', 'gizmo_testing', port=8182)
        self.gremlin = Gremlin('gizmo_testing')
        self.mapper = Mapper(self.request, self.gremlin, logger=False)
        super(BaseTests, self).setUp()


    def tearDown(self):
        super(BaseTests, self).tearDown()

    def purge(self):
        script = "%s.V().map{it.get().remove()}" % self.gremlin.gv
        return self.mapper.query(script=script)


class ConnectionTests(BaseTests):

    @gen_test
    def test_can_establish_mapper(self):
        yield self.purge()
        c = '%s.V()' % self.gremlin.gv
        r = yield self.mapper.query(script=c)

        self.assertEqual(0, len(r))

    @gen_test
    def test_can_send_request_and_retrieve_collection_objec(self):
        script = 'a = 1'
        r = yield self.mapper.query(script=script)

        self.assertIsInstance(r, Collection)
        self.assertIsInstance(r[0], Vertex)

    @gen_test
    def test_can_get_database_time(self):
        script = 'def x = new Date(); x'
        r = yield self.mapper.query(script=script)

        self.assertTrue(r[0]['response'] != '')
        self.assertIsInstance(r[0]['response'], int)

    @gen_test
    def test_can_send_math_equation_to_server_and_retrieve_genderic_vertex_with_respnose_to_result(self):
        script = 'b = 1 + 1;'
        r = yield self.mapper.query(script=script)
        r1 = r[0]

        self.assertIsInstance(r1, Vertex)
        self.assertIn('response', r1.data)
        self.assertEqual(2, r1['response'])


class EntityTests(BaseTests):

    def entity_save_assertions(self, entity):
        from gizmo import GIZMO_MODEL, GIZMO_CREATED, GIZMO_MODIFIED, \
            GIZMO_ID, GIZMO_LABEL

        empty = ['', 0, None]
        non_zero = ['', None]
        fields = [GIZMO_MODEL, GIZMO_CREATED, GIZMO_MODIFIED, \
            GIZMO_ID, GIZMO_LABEL]

        for f in fields:
            if f is GIZMO_ID:
                check = non_zero
            else:
                check = empty

            self.assertIsNotNone(entity[f])
            self.assertNotIn(entity[f], check, msg='{} is `{}`'.format(f, entity[f]))

    @gen_test(timeout=10)
    def test_can_save_generic_vertex_and_update_its_id(self):
        data = {'name': 'mark', 'sex': 'male'}
        v = self.mapper.create_model(data=data)

        self.mapper.save(v)

        res = yield self.mapper.send()
        self.entity_save_assertions(v)

    @gen_test
    def test_can_save_generic_vertex_and_get_response_entity_with_id(self):
        data = {'name': 'mark', 'sex': 'male'}
        v = self.mapper.create_model(data=data)
        self.mapper.save(v)
        r = yield self.mapper.send()
        v1 = r.first()

        self.entity_save_assertions(v1)

    @gen_test
    def test_can_save_defined_vertex_and_update_its_id(self):
        class TestVertex(Vertex):
            _allowed_undefined = True

        data = {'name': 'mark', 'sex': 'male'}
        v = self.mapper.create_model(data=data, model_class=TestVertex)

        yield self.mapper.save(v).send()
        self.entity_save_assertions(v)

    @gen_test
    def test_can_save_defined_vertex_and_get_response_entity_with_id(self):
        class TestVertex(Vertex):
            _allowed_undefined = True

        data = {'name': 'mark', 'sex': 'male'}
        v = self.mapper.create_model(data=data, model_class=TestVertex)
        self.mapper.save(v)
        r = yield self.mapper.send()
        v1 = r.first()

        self.entity_save_assertions(v1)

    @gen_test
    def test_can_save_generic_edge_with_two_generic_vertices_all_at_once_and_update_all_ids(self):
        label = 'some_label'
        v1 = self.mapper.create_model()
        v2 = self.mapper.create_model()
        e = self.mapper.connect(v1, v2, label)

        yield self.mapper.save(e).send()
        self.entity_save_assertions(v1)
        self.entity_save_assertions(v2)
        self.entity_save_assertions(e)

    @gen_test
    def test_can_save_generic_edge_with_one_generic_vertex_all_at_once_and_update_all_ids(self):
        class TestVertex(Vertex):
            _allowed_undefined = True

        label = 'some_label'
        v1 = self.mapper.create_model(model_class=TestVertex)
        v2 = self.mapper.create_model()
        e = self.mapper.connect(v1, v2, label)

        yield self.mapper.save(e).send()
        self.entity_save_assertions(v1)
        self.entity_save_assertions(v2)
        self.entity_save_assertions(e)

    @gen_test
    def test_can_save_generic_edge_with_two_defined_vertices_all_at_once_and_update_all_ids(self):
        class TestVertex(Vertex):
            _allowed_undefined = True

        class TestVertex2(Vertex):
            _allowed_undefined = True

        label = 'some_label'
        v1 = self.mapper.create_model(model_class=TestVertex)
        v2 = self.mapper.create_model(model_class=TestVertex2)
        e = self.mapper.connect(v1, v2, label)

        yield self.mapper.save(e).send()
        self.entity_save_assertions(v1)
        self.entity_save_assertions(v2)
        self.entity_save_assertions(e)

    @gen_test
    def test_can_save_defined_edge_with_two_defined_vertices_all_at_once_and_update_all_ids(self):
        class TestVertex(Vertex):
            _allowed_undefined = True

        class TestVertex2(Vertex):
            _allowed_undefined = True

        class TestEdge(Edge):
            pass

        label = 'some_label'
        v1 = self.mapper.create_model(model_class=TestVertex)
        v2 = self.mapper.create_model(model_class=TestVertex2)
        e = self.mapper.connect(v1, v2, label, edge_model=TestEdge)

        yield self.mapper.save(e).send()
        self.entity_save_assertions(v1)
        self.entity_save_assertions(v2)
        self.entity_save_assertions(e)


class MapperTests(BaseTests):

    @gen_test
    def test_can_utilitze_custom_mapper(self):
        variable = str(random.random())

        class MapperTestVertexCutsom(Vertex):
            _allowed_undefined = True


        class MapperTestMapperCustom(_GenericMapper):
            model = MapperTestVertexCutsom

            def create_model(self, *args, **kwargs):
                entity = super(MapperTestMapperCustom, self).create_model(*args, **kwargs)
                entity['variable'] = variable
                return entity

        v = self.mapper.create_model(model_class=MapperTestVertexCutsom)
        d = v.data

        self.assertIsInstance(v, MapperTestVertexCutsom)
        self.assertIn('variable', d)
        self.assertEqual(d['variable'], variable)

    @gen_test(timeout=900)
    def test_can_restrict_model_creation_based_on_duplicate_field_values(self):
        yield self.purge()

        class MapperTestVertexDuplicate(Vertex):
            _allowed_undefined = True


        class MapperTestMapper(_GenericMapper):
            model = MapperTestVertexDuplicate
            unique_fields = ['first_name',]


        d = {'first_name': 'mark' + str(random.random())}
        v1 = self.mapper.create_model(data=d, model_class=MapperTestVertexDuplicate)
        v2 = self.mapper.create_model(data=d, model_class=MapperTestVertexDuplicate)

        r = yield self.mapper.save(v1).send()
        r2 = yield self.mapper.save(v2).send()

        gremlin = self.mapper.gremlin.V()
        res = yield self.mapper.query(gremlin=gremlin)

        self.assertEqual(1, len(res))

    @gen_test
    def test_can_restrict_multiple_model_connections(self):
        yield self.purge()

        class MapperTestVertexRestrict(Vertex):
            _allowed_undefined = True

        class MapperTestEdgeRestrict(Edge):
            _allowed_undefined = True

        class MapperTestEdgeMapperRestrict(_GenericMapper):
            model = MapperTestEdgeRestrict
            unique = 'both'

        d = {'first_name': 'mark' + str(random.random())}
        v1 = self.mapper.create_model(data=d, model_class=MapperTestVertexRestrict)
        v2 = self.mapper.create_model(data=d, model_class=MapperTestVertexRestrict)
        e = self.mapper.connect(v1, v2, edge_model=MapperTestEdgeRestrict)
        e2 = self.mapper.connect(v1, v2, edge_model=MapperTestEdgeRestrict)
        res = yield self.mapper.save(e).send()
        res2 = yield self.mapper.save(e2).send()
        gremlin = self.mapper.gremlin.E()
        result = yield self.mapper.query(gremlin=gremlin)

        self.assertEqual(1, len(result))

    @gen_test
    def test_can_save_edge_on_vertices_that_were_used_in_previous_connection_when_unique_is_true(self):
        yield self.purge()

        class MapperTestVertexRestrictAgain(Vertex):
            _allowed_undefined = True

        class MapperTestEdgeRestrictAgain(Edge):
            _allowed_undefined = True

        class MapperTestEdgeMapperRestrictAgain(_GenericMapper):
            model = MapperTestEdgeRestrictAgain
            unique = 'both'

        d = {'first_name': 'mark' + str(random.random())}
        v1 = self.mapper.create_model(data=d, model_class=MapperTestVertexRestrictAgain)
        v2 = self.mapper.create_model(data=d, model_class=MapperTestVertexRestrictAgain)
        v3 = self.mapper.create_model(data=d, model_class=MapperTestVertexRestrictAgain)
        e = self.mapper.connect(v1, v2, edge_model=MapperTestEdgeRestrictAgain)
        e2 = self.mapper.connect(v1, v3, edge_model=MapperTestEdgeRestrictAgain)
        e3 = self.mapper.connect(v1, v3, edge_model=MapperTestEdgeRestrictAgain)
        res = yield self.mapper.save(e).send()
        res2 = yield self.mapper.save(e2).send()
        res2 = yield self.mapper.save(e3).send()
        gremlin = self.mapper.gremlin.E()
        result = yield self.mapper.query(gremlin=gremlin)

        self.assertEqual(2, len(result))


class CollectionTests(BaseTests):
    pass

class TraversalTests(BaseTests):
    pass


if __name__ == '__main__':
    unittest.main()
