import unittest
import json
from random import randrange, random
from pprint import pprint

from gizmo.entity import Vertex, Edge, GenericVertex
from gizmo.field import *
from gizmo.util import camel_to_underscore, GIZMO_LABEL

from gremlinpy.gremlin import Gremlin


class TestVertex(Vertex):
    some_field = String()


class TestEdge(Edge):
    some_field = String()


class TestUniqueEdge(Edge):
    pass


class TestUndefinedVertex(GenericVertex):
    allow_undefined = True


class TestUndefinedEdge(Vertex):
    allow_undefined = True


class EntityTests(unittest.TestCase):

    def set_up(self):
        pass

    def test_can_create_vertex(self):
        v = TestVertex()

        self.assertTrue(isinstance(v, Vertex))
        self.assertEqual(v['type'], 'vertex')

    def test_can_access_fields_as_items_or_attributes(self):
        v = TestVertex({'id': 7})
        i_id = v.id
        a_id = v['id']

        self.assertEqual(i_id, a_id)
        self.assertEqual(v.some_field, v['some_field'])

    def test_can_create_vertex_with_data(self):
        d = {'some_field': '1'}
        v = TestVertex(d)
        data = v.data

        for k, v in d.items():
            self.assertIn(k, data)
            self.assertEqual(1, len(data[k]))
            self.assertEqual(v, data[k][0]['value'])

    def test_can_create_edge(self):
        e = TestEdge()

        self.assertTrue(isinstance(e, Edge))
        self.assertEqual(e['label'], camel_to_underscore(e.__class__.__name__))

    def test_can_create_edge_with_data(self):
        d = {'some_field': '1'}
        e = TestEdge(d)
        data = e.data

        for k, v in d.items():
            self.assertIn(k, data)
            self.assertEqual(1, len(data[k]))
            self.assertEqual(v, data[k][0]['value'])

    def test_can_add_undefined_field_to_undefied_vertex(self):
        d = {'one': 1, 'two': 2, 'three': 3}
        v = TestUndefinedVertex(d)
        data = v.data

        for k, v in d.items():
            self.assertIn(k, data)
            self.assertEqual(v, data[k][0]['value'])

    def test_can_add_undefined_field_to_undefied_edge(self):
        d = {'one': 1, 'two': 2, 'three': 3}
        v = TestUndefinedEdge(d)
        data = v.data

        for k, v in d.items():
            self.assertIn(k, data)
            self.assertEqual(v, data[k][0]['value'])

    def test_can_create_with_and_without_label_override(self):
        custom = 'custom_name' + str(random())

        class CustomV(Vertex):
            label = custom

        class V(Vertex):
            pass

        custom_v = CustomV()
        v = V()
        name = camel_to_underscore(v.__class__.__name__)

        self.assertEqual(custom, custom_v[GIZMO_LABEL[0]])
        self.assertEqual(name, v[GIZMO_LABEL[0]])

    def test_can_get_rep_of_entity(self):
        d = {'id': '1'}
        v = TestVertex(d)
        entity, id = v.get_rep()

        self.assertEqual('V', entity)
        self.assertEqual(d['id'], id)

    def test_can_change_datatype_for_entity(self):
        v = TestVertex()
        t = 'graph'

        v.data_type = t

        self.assertEqual(t, v.data_type)

    def test_can_add_undefined_fields(self):
        v = TestUndefinedVertex()
        fields = v.fields
        dic = {'name': str(random())}
        flo = random()
        inte = int(random())
        li = ['1', '2', '3']
        s = str(random())
        d = {
            'dic': dic,
            'flo': flo,
            'inte': inte,
            'li': li,
            's': s,
        }
        v.hydrate(d)

        self.assertIsInstance(fields['dic'], Map)
        self.assertIsInstance(fields['flo'], Float)
        self.assertIsInstance(fields['inte'], Integer)
        self.assertIsInstance(fields['li'], List)
        self.assertIsInstance(fields['s'], String)

    def test_can_get_undefiend_field(self):
        v = TestUndefinedVertex()
        n = 'name'
        val = v[n]

        self.assertIsInstance(v.fields[n], String)

    def test_can_create_entities_that_can_subclass_entities(self):
        class Base(Vertex):
            base_field = String()
            name = String()

        class Sub(Base):
            sub_field = String()
            name = Integer()

        init = {
            'sub_field': str(random()),
            'base_field': str(random()),
            'name': str(random())
        }
        ins = Sub(init)
        data = ins.data

        self.assertIn('base_field', data)
        self.assertIn('sub_field', data)
        self.assertIn('name', data)
        self.assertIsInstance(ins.fields['name'], Integer)

        class SubTwo(Sub):
            sub_two_field = String()
            name = Boolean()

        init['sub_two_field'] = str(random())
        ins = SubTwo(init)
        data = ins.data
        self.assertIn('base_field', data)
        self.assertIn('sub_field', data)
        self.assertIn('name', data)
        self.assertIn('sub_two_field', data)
        self.assertIsInstance(ins.fields['name'], Boolean)

        class SubThree(Sub):
            sub_three_field = String()
            name = Float()

        class Diamond(SubTwo, SubThree):
            diamon_field = String()

        init['diamon_field'] = str(random())
        init['sub_three_field'] = str(random())
        ins = Diamond(init)
        data = ins.data

        self.assertIn('base_field', data)
        self.assertIn('sub_field', data)
        self.assertIn('name', data)
        self.assertIn('sub_two_field', data)
        self.assertIn('sub_three_field', data)
        self.assertIn('diamon_field', data)
        self.assertIsInstance(ins.fields['name'], String)

    def test_can_create_fields_from_json_gremlin_response(self):
        j = '{"requestId":"cce2b0ff-10ff-472f-847e-35c5efdd813a","status":{"message":"","code":200,"attributes":{}},"result":{"data":[{"id":4,"label":"vertex","type":"vertex","properties":{"__GIZMO_ENTITY__":[{"id":31,"value":"gizmo.test.mapper.TestVertex"}],"name":[{"id":34,"value":"mark","properties":{"age":35}}],"id":[{"id":32,"value":"0.28441421794883837"}],"type":[{"id":33,"value":"vertex"}]}}],"meta":{}}}'
        j = json.loads(j)
        data = j['result']['data'][0]['properties']
        v = TestUndefinedVertex(data=data)
        v_data = v.data

        # plus one for the label
        self.assertEqual(len(data) + 1, len(v_data))

        for k, v in data.items():
            self.assertIn(k, v_data)
            if k != 'name':
                self.assertEqual(v[0]['value'], v_data[k])
            else:
                self.assertEqual(v[0]['value'], v_data[k][0]['value'])
                self.assertEqual(v[0]['properties'], v_data[k][0]['properties'])


if __name__ == '__main__':
    unittest.main()
