import unittest
from random import randrange
from gizmo.entity import Vertex, Edge
from gizmo.field import String
from gizmo.utils import camel_to_underscore
from gremlinpy.gremlin import Gremlin


class TestVertex(Vertex):
    some_field = String()


class TestEdge(Edge):
    some_field = String()


class TestUniqueEdge(Edge):
    pass


class TestUndefinedVertex(Vertex):
    _allowed_undefined = True


class TestUndefinedEdge(Vertex):
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

        for k, v in d.items():
            self.assertIn(k, data)
            self.assertEqual(v, data[k])

    def test_can_create_edge(self):
        e = TestEdge()

        self.assertTrue(isinstance(e, Edge))
        self.assertEqual(e.label, camel_to_underscore(e.__class__.__name__))

    def test_can_create_edge_with_data(self):
        d = {'some_field': 1}
        e = TestEdge(d)
        data = e.data

        for k, v in d.items():
            self.assertIn(k, data)
            self.assertEqual(v, data[k])

    def test_can_add_undefined_field_to_undefied_vertex(self):
        d = {'one': 1, 'two': 2, 'three': 3}
        v = TestUndefinedVertex(d)
        data = v.data

        for k, v in d.items():
            self.assertIn(k, data)
            self.assertEqual(v, data[k])

    def test_can_add_undefined_field_to_undefied_edge(self):
        d = {'one': 1, 'two': 2, 'three': 3}
        v = TestUndefinedEdge(d)
        data = v.data

        for k, v in d.items():
            self.assertIn(k, data)
            self.assertEqual(v, data[k])


if __name__ == '__main__':
    unittest.main()
