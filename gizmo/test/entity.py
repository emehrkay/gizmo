import unittest
from random import randrange
from gizmo.entity import Vertex, Edge
from gizmo.field import String
from gremlinpy.gremlin import Gremlin

TEST_EDGE = 'test_edge'
TEST_UNIQUE_EDGE = 'test_unique_edge'
TEST_VERTEX = 'test_vertex'
TEST_UNDEFINED_VERTEX = 'test_undefied_vertex'
TEST_UNDEFINED_EDGE = 'test_undefied_edge'

class TestVertex(Vertex):
    some_field = String()
    _node_type = TEST_VERTEX


class TestEdge(Edge):
    some_field = String()
    _node_type = TEST_EDGE


class TestUniqueEdge(Edge):
    _node_type = TEST_UNIQUE_EDGE

class TestUndefinedVertex(Vertex):
    _node_type = TEST_UNDEFINED_VERTEX
    _allowed_undefined = True


class TestUndefinedEdge(Vertex):
    _node_type = TEST_UNDEFINED_EDGE
    _allowed_undefined = True


class EntityTests(unittest.TestCase):
    def set_up(self):
        pass

    def test_can_create_vertex(self):
        v = TestVertex()

        self.assertTrue(isinstance(v, Vertex))
        self.assertEqual(v._type, 'vertex')

    def test_can_create_vertex_with_data(self):
        d = {'some_field': 1}
        v = TestVertex(d)
        data = v.data

        for k, v in d.iteritems():
            self.assertIn(k, data)
            self.assertEqual(v, data[k])

    def test_can_create_edge(self):
        e = TestEdge()

        self.assertTrue(isinstance(e, Edge))
        self.assertEqual(e._type, 'edge')

    def test_can_create_edge_with_data(self):
        d = {'some_field': 1}
        e = TestEdge(d)
        data = e.data

        for k, v in d.iteritems():
            self.assertIn(k, data)
            self.assertEqual(v, data[k])

    def test_can_add_undefined_field_to_undefied_vertex(self):
        d = {'one': 1, 'two': 2, 'three': 3}
        v = TestUndefinedVertex(d)
        data = v.data

        for k, v in d.iteritems():
            self.assertIn(k, data)
            self.assertEqual(v, data[k])

    def test_can_add_undefined_field_to_undefied_edge(self):
        d = {'one': 1, 'two': 2, 'three': 3}
        v = TestUndefinedEdge(d)
        data = v.data

        for k, v in d.iteritems():
            self.assertIn(k, data)
            self.assertEqual(v, data[k])


if __name__ == '__main__':
    unittest.main()
