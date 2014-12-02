import unittest
from random import randrange
from gizmo.element import Vertex, Edge
from gremlinpy.gremlin import Gremlin

TEST_EDGE = 'test_edge'
TEST_VERTEX = 'test_vertex'


class TestVertex(Vertex):
    @property
    def _node_type(self):
        return TEST_VERTEX


class TestEdge(Edge):
    @property
    def _node_type(self):
        return TEST_EDGE


class ElementTests(unittest.TestCase):
    def set_up(self):
        pass
    
    def test_can_create_vertex(self):
        v = TestVertex()

        self.assertTrue(isinstance(v, Vertex))
        self.assertEqual(v['_type'], 'vertex')
        
    def test_can_create_vertex_with_data(self):
        d = {'one': 1, 'two': 2, 'three': 3}
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
        d = {'one': 1, 'two': 2, 'three': 3}
        e = TestEdge(d)
        data = e.data

        for k, v in d.iteritems():
            self.assertIn(k, data)
            self.assertEqual(v, data[k])

if __name__ == '__main__':
    unittest.main()
