import unittest
from random import randrange, random
from collections import OrderedDict
from gizmo.mapper import Mapper, Vertex, Edge
from gizmo.request import _Request
from gizmo.utils import GIZMO_MODEL, GIZMO_CREATED, GIZMO_MODIFIED, GIZMO_NODE_TYPE, GIZMO_TYPE, GIZMO_ID, GIZMO_LABEL
from gremlinpy.gremlin import Gremlin
from element import TestVertex, TestEdge

def get_dict_key(dict, value):
    for k, v in dict.iteritems():
        if v == value:
            return k
    
    return None
    

def get_element_entry(element_queue, element):
    for i, s in element_queue.iteritems():
        if s == element:
            return {i: element_queue[i]}
            
    return None


class MapperTests(unittest.TestCase):
    def setUp(self):
        self.gremlin = Gremlin()
        self.request = _Request('localhost', 'x', 'x')
        self.mapper = Mapper(self.request, self.gremlin)
    
    def get_model_data(self, data, id=None, edge=False):
        seed = random()
        d = {
            GIZMO_MODEL     : 'some_model_%s' % seed,
            GIZMO_CREATED   : 'some_created_%s' % seed,
            GIZMO_MODIFIED  : 'some_mod_%s' % seed,
            GIZMO_NODE_TYPE : 'some_node_type_%s' % seed,
            GIZMO_TYPE      : 'edge' if edge is not False else 'vertex'
        }
        
        if edge:
            d[GIZMO_LABEL] = 'some_label_%s' % seed
        
        if id is not None:
            d[GIZMO_ID] = id
            
        return OrderedDict(sorted(d.items()))

    def test_mapper_instance(self):
        m = Mapper(self.gremlin, self.request)

        self.assertTrue(type(m) == Mapper)

    def test_can_create_vertex(self):
        v = self.mapper.create_model(model_class=TestVertex)

        self.assertTrue(isinstance(v, Vertex))
        self.assertTrue(v.data['_type'] == 'vertex')
        
    def test_can_create_vertex_with_data(self):
        d  = {'name': 'mark', 'sex': 'male'}
        v  = self.mapper.create_model(d, TestVertex)
        vd = v.data

        """
        the element's data will have a _type member
        """
        self.assertTrue(isinstance(v, Vertex))

        for k, v in d.iteritems():
            self.assertIn(k, vd)
            self.assertEqual(v, vd[k])
        
    def test_can_update_existing_vertex(self):
        vid = 1111
        d = self.get_model_data({
            'name': 'mark', 
            'sex': 'male'
        }, vid)
        v = self.mapper.create_model(d, TestVertex)
        
        self.mapper.save(v)._build_queries()

        params   = self.mapper.params
        immutable = v._immutable
        query_ps = []
        entry_v1 = get_element_entry(self.mapper.models, v)
        print '*'*90
        print v.data

        print '*'*90
        print params
        for k, v in d.iteritems():
            if k not in immutable:
                prop = "it.setProperty('%s', %s)" % (k, get_dict_key(params, v))
                query_ps.append(prop)

        print '*'*90
        print query_ps
        propv    = get_dict_key(params, vid)
        close    = '._().sideEffect{%s}.next()' % '; '.join(query_ps)
        params   = (entry_v1.keys()[0], propv, close)
        expected = "%s = g.v(%s)%s" % params
        
        self.assertEqual(expected, self.mapper.queries[0])
        self.assertEqual(len(query_ps), len(self.mapper.params))

    def test_can_queue_save_vertex_with_two_params_query(self):
        d = self.get_model_data({
            'name': 'mark', 
            'sex': 'male'
        })
        v = self.mapper.create_model(d, TestVertex)

        self.mapper.save(v)._build_queries()

        params = self.mapper.params
        immutable = v._immutable
        props = []
        entry_v1 = get_element_entry(self.mapper.models, v)
        
        for k,v in d.iteritems():
            if k not in immutable:
                prop = "'%s': %s" % (k, get_dict_key(params, v))
                props.append(prop)
                
        expected = "%s = g.addVertex([%s])" % (entry_v1.keys()[0] ,', '.join(props))
        
        self.assertEqual(expected, self.mapper.queries[0])
        self.assertEqual(len(query_ps), len(self.mapper.params))

    def test_can_create_edge_with_existing_vertices(self):
        v1 = self.get_model_data({}, id=15)
        v2 = self.get_model_data({}, id=10)
        out_v = self.mapper.create_model(v1, TestVertex)
        in_v = self.mapper.create_model(v2, TestVertex)
        ed = {'out_v': out_v, 'in_v': in_v}
        edge = self.mapper.create_model(ed, TestEdge)
        
        self.assertTrue(isinstance(edge, Edge))
        self.assertTrue(isinstance(edge.out_v, TestVertex))
        self.assertTrue(isinstance(edge.in_v, TestVertex))
        
if __name__ == '__main__':
    unittest.main()
