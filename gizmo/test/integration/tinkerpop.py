import unittest

from gremlinpy import Gremlin
from gizmo import Mapper, Async, Collection, Vertex, Edge


class BaseTests(unittest.TestCase):

    def setUp(self):
        self.request = Async('localhost', 'gizmo_testing', port=8182)
        self.gremlin = Gremlin('gizmo_testing')
        self.mapper = Mapper(self.request, self.gremlin, logger=False)
        super(BaseTests, self).setUp()

    def tearDown(self):
        script = "%s.V().map{it.get().remove()}" % self.gremlin.gv
        self.mapper.query(script=script)
        super(BaseTests, self).tearDown()


class ConnectionTests(BaseTests):

    def test_can_establish_mapper(self):
        # TODO: fix this test. It assumes that it is run in isolation and there
        # will be no other interactions with the graph beforehand
        c = '%s.V()' % self.gremlin.gv
        r = self.mapper.query(script=c)

        self.assertEqual(0, len(r))

    def test_can_send_request_and_retrieve_collection_objec(self):
        script = 'a = 1'
        r = self.mapper.query(script=script)

        self.assertIsInstance(r, Collection)
        self.assertIsInstance(r[0], Vertex)

    def test_can_get_database_time(self):
        self.assertTrue(False)

    def test_can_send_math_equation_to_server_and_retrieve_genderic_vertex_with_respnose_to_result(self):
        script = 'b = 1 + 1;'
        r = self.mapper.query(script=script)
        r1 = r[0]

        self.assertIsInstance(r1, Vertex)
        self.assertIn('response', r1.data)
        self.assertEqual(2, r1['response'])


class EntityTests(BaseTests):

    def entity_save_assertions(self, entity):
        from gizmo import GIZMO_MODEL, GIZMO_CREATED, GIZMO_MODIFIED, \
            GIZMO_ID, GIZMO_LABEL

        empty = ['', 0, None]
        fields = [GIZMO_MODEL, GIZMO_CREATED, GIZMO_MODIFIED, \
            GIZMO_ID, GIZMO_LABEL]

        for f in fields:
            self.assertIsNotNone(entity[f])
            self.assertNotIn(entity[f], empty)


    def test_can_save_generic_vertex_and_update_its_id(self):
        data = {'name': 'mark', 'sex': 'male'}
        v = self.mapper.create_model(data=data)

        self.mapper.save(v).send()
        self.entity_save_assertions(v)

    def test_can_save_generic_vertex_and_get_response_entity_with_id(self):
        data = {'name': 'mark', 'sex': 'male'}
        v = self.mapper.create_model(data=data)
        r = self.mapper.save(v).send()
        v1 = r.first()

        self.entity_save_assertions(v1)

    def test_can_save_defined_vertex_and_update_its_id(self):
        class TestVertex(Vertex):
            _allowed_undefined = True

        data = {'name': 'mark', 'sex': 'male'}
        v = self.mapper.create_model(data=data, model_class=TestVertex)

        self.mapper.save(v).send()
        self.entity_save_assertions(v)

    def test_can_save_defined_vertex_and_get_response_entity_with_id(self):
        class TestVertex(Vertex):
            _allowed_undefined = True

        data = {'name': 'mark', 'sex': 'male'}
        v = self.mapper.create_model(data=data, model_class=TestVertex)
        r = self.mapper.save(v).send()
        v1 = r.first()

        self.entity_save_assertions(v1)

    def test_can_save_generic_edge_with_two_generic_vertices_all_at_once_and_update_all_ids(self):
        label = 'some_label'
        v1 = self.mapper.create_model()
        v2 = self.mapper.create_model()
        e = self.mapper.connect(v1, v2, label)

        self.mapper.save(e).send()
        self.entity_save_assertions(v1)
        self.entity_save_assertions(v2)
        self.entity_save_assertions(e)

    def test_can_save_generic_edge_with_one_generic_vertex_all_at_once_and_update_all_ids(self):
        class TestVertex(Vertex):
            _allowed_undefined = True

        label = 'some_label'
        v1 = self.mapper.create_model(model_class=TestVertex)
        v2 = self.mapper.create_model()
        e = self.mapper.connect(v1, v2, label)

        self.mapper.save(e).send()
        self.entity_save_assertions(v1)
        self.entity_save_assertions(v2)
        self.entity_save_assertions(e)

    def test_can_save_generic_edge_with_two_defined_vertices_all_at_once_and_update_all_ids(self):
        class TestVertex(Vertex):
            _allowed_undefined = True

        class TestVertex2(Vertex):
            _allowed_undefined = True

        label = 'some_label'
        v1 = self.mapper.create_model(model_class=TestVertex)
        v2 = self.mapper.create_model(model_class=TestVertex2)
        e = self.mapper.connect(v1, v2, label)

        self.mapper.save(e).send()
        self.entity_save_assertions(v1)
        self.entity_save_assertions(v2)
        self.entity_save_assertions(e)

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

        self.mapper.save(e).send()
        self.entity_save_assertions(v1)
        self.entity_save_assertions(v2)
        self.entity_save_assertions(e)


class MapperTests(object):

    def test_can_utilitze_custom_mapper(self):
        pass

if __name__ == '__main__':
    unittest.main()
