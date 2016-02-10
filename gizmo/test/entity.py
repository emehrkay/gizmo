import unittest
from random import randrange, random

from gizmo.entity import Vertex, Edge
from gizmo.field import *
from gizmo.utils import camel_to_underscore, GIZMO_LABEL
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

    def test_can_create_entity_with_and_without_label_override(self):
        custom = 'custom_name' + str(random())

        class CustomV(Vertex):
            _node_label = custom

        class V(Vertex):
            pass

        custom_v = CustomV()
        v = V()
        name = camel_to_underscore(v.__class__.__name__)

        self.assertEqual(custom, custom_v[GIZMO_LABEL])
        self.assertEqual(name, v[GIZMO_LABEL])

    def test_can_get_the_fields_that_were_changed(self):

        class V(Vertex):
            name = String(track_changes=True)

        i = {'name': 'initial' + str(random())}
        v = V()
        f = {'name': str(random())}

        v.hydrate(f)
        changed = v.changed
        unchanged = v.unchanged
        self.assertEqual(len(changed), len(f))
        self.assertEqual(len(unchanged), 0)

    def test_can_get_the_fields_that_were_removed(self):

        class V(Vertex):
            name = String(track_changes=True)
            age = Integer(track_changes=True)

        i = {'name': 'initial' + str(random()), 'age': int(random()) + 3}
        v = V()
        f = {'name': str(random()), 'age': None}

        v.hydrate(f)
        removed = v.removed
        self.assertTrue(len(removed), 1)

    def test_can_get_rep_of_entity(self):
        d = {'_id': 1}
        v = TestVertex(d)
        entity, id = v.get_rep()

        self.assertEqual('V', entity)
        self.assertEqual(d['_id'], id)

    def test_can_change_datatype_for_entity(self):
        v = TestVertex()
        t = 'graph'
        v.field_type = t

        self.assertEqual(t, v.field_type)

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

        self.assertIsNone(val)

    def test_can_create_entities_that_can_subclass_entities(self):
        class Base(Vertex):
            base_field = String()
            name = String()

        class Sub(Base):
            sub_field = String()
            name = Integer()

        ins = Sub()
        data = ins.data
        self.assertIn('base_field', data)
        self.assertIn('sub_field', data)
        self.assertIn('name', data)
        self.assertIsInstance(ins.fields['name'], Integer)

        class SubTwo(Sub):
            sub_two_field = String()
            name = Boolean()

        ins = SubTwo()
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

        ins = Diamond()
        data = ins.data

        self.assertIn('base_field', data)
        self.assertIn('sub_field', data)
        self.assertIn('name', data)
        self.assertIn('sub_two_field', data)
        self.assertIn('sub_three_field', data)
        self.assertIn('diamon_field', data)
        self.assertIsInstance(ins.fields['name'], Float)


if __name__ == '__main__':
    unittest.main()
