import unittest
import json
from random import randrange, random, choice, randint
from pprint import pprint

from gizmo.field import *


class FieldTests(unittest.TestCase):

    def test_can_create_field_without_value(self):
        f = Field()

        self.assertEqual(type(f), Field)
        self.assertEqual(0, len(f.values))

    def test_can_create_field_with_one_value(self):
        v = str(random())
        f = Field(values=v)

        self.assertEqual(type(f), Field)
        self.assertEqual(1, len(f.values))
        self.assertIn(v, f.values)

    def test_can_create_field_with_two_value_in_list(self):
        v = [str(random()), str(random())]
        f = Field(values=v)

        self.assertEqual(type(f), Field)
        self.assertEqual(2, len(f.values))
        self.assertIn(v[0], f.values)
        self.assertIn(v[1], f.values)

    def test_can_add_value_to_field(self):
        f = Field()
        v = str(random())
        f + v

        self.assertEqual(1, len(f.values))
        self.assertIn(v, f.values)

    def test_can_add_multiple_values_to_field(self):
        f = Field()
        v = str(random())
        d = str(random())
        f + v + d

        self.assertEqual(2, len(f.values))
        self.assertIn(v, f.values)
        self.assertIn(d, f.values)

    def test_can_add_multiple_same_value_to_field(self):
        f = Field()
        v = str(random())
        f + v + v

        self.assertEqual(2, len(f.values))
        self.assertIn(v, f.values)

    def test_can_limit_how_many_values_can_be_added_to_field(self):
        mv = 6
        r = 15
        f = Field(max_values=mv)

        for i in range(r):
            f + i

        values = f.values

        self.assertEqual(mv, len(values))
        self.assertTrue(values[-1] < mv)

    def test_can_overwrite_the_last_value_when_limiting_number_of_values(self):
        mv = 6
        r = 15
        f = Field(max_values=mv, overwrite_last_value=True)

        for i in range(r):
            f + i

        values = f.values

        self.assertEqual(mv, len(values))
        self.assertTrue(values[-1] == r - 1)

    def test_can_overwrite_the_last_value_when_limiting_number_of_values_to_one(self):
        mv = 1
        r = 15
        f = Field(max_values=mv, overwrite_last_value=True)

        for i in range(r):
            f + i

        values = f.values

        self.assertEqual(mv, len(values))
        self.assertTrue(values[-1] == r - 1)

    def test_can_create_field_with_value_and_properties(self):
        iv = 'initail_value_' + str(random())
        pk = 'property_key' + str(random())
        pv = 'property_value' + str(random())
        initial = {
            'value': iv,
            'properties': {
                pk: pv,
            }
        }
        f = Field(values=initial)
        data = f.data

        self.assertEqual(1, len(data))

        res = data[0]

        self.assertIn('value', res)
        self.assertEqual(iv, res['value'])
        self.assertIn('properties', res)
        self.assertIn(pk, res['properties'])
        self.assertEqual(pv, res['properties'][pk])

    def test_can_create_field_with_multiple_values_and_properties(self):
        iv = 'initail_value_' + str(random())
        iv2 = '2initail_value_' + str(random())
        pk = 'property_key' + str(random())
        pv = 'property_value' + str(random())
        pk2 = 'property_key' + str(random())
        pv2 = 'property_value' + str(random())
        initial = [
            {
                'value': iv,
                'properties': {
                    pk: pv,
                }
            },
            {
                'value': iv2,
                'properties': {
                    pk2: pv2,
                }
            }
        ]
        f = Field(values=initial)
        data = f.data
        self.assertEqual(2, len(data))

        res = data[0]
        res2 = data[1]

        self.assertIn('value', res)
        self.assertEqual(iv, res['value'])
        self.assertIn('properties', res)
        self.assertIn(pk, res['properties'])
        self.assertEqual(pv, res['properties'][pk])

        self.assertIn('value', res2)
        self.assertEqual(iv2, res2['value'])
        self.assertIn('properties', res2)
        self.assertIn(pk2, res2['properties'])
        self.assertEqual(pv2, res2['properties'][pk2])

    def test_can_create_multiple_fields_with_value_and_properties(self):
        iv = 'initail_value_' + str(random())
        pk = 'property_key' + str(random())
        pv = 'property_value' + str(random())
        iv2 = '2initail_value_' + str(random())
        pk2 = '2property_key' + str(random())
        pv2 = '2property_value' + str(random())
        value1 = {
            'value': iv,
            'properties': {
                pk: pv,
            }
        }
        value2 = {
            'value': iv2,
            'properties': {
                pk2: pv2,
            }
        }
        initial = [value1, value2,]
        f = Field(values=initial)
        data = f.data

        self.assertEqual(2, len(data))

        for res in data:
            self.assertIn('value', res)
            self.assertIn('properties', res)
            self.assertIn(res, initial)

    def test_can_add_property_to_existing_value(self):
        f = Field()
        v = 'name'
        p = str(random())
        prop = 'prop'
        f + v

        f[v].properties[prop] = p

        data = f.data

        self.assertEqual(1, len(data))
        self.assertIn('properties', data[0])
        self.assertIn(prop, data[0]['properties'])
        self.assertEqual(p, data[0]['properties'][prop])

    def test_can_add_property_to_nonexisting_value(self):
        f = Field()
        v = 'name'
        p = str(random())
        prop = 'prop' + str(random())

        f[v].properties[prop] = p

        data = f.data

        self.assertEqual(1, len(data))
        self.assertIn('properties', data[0])
        self.assertIn(prop, data[0]['properties'])
        self.assertEqual(p, data[0]['properties'][prop])

    def test_can_add_multiple_values_with_unique_properties(self):
        f = Field()
        v = 'v' + str(random())
        v2 = 'v2' + str(random())
        p = str(random())
        f + v + v2

        vprops = f[v].properties
        v2props = f[v2].properties
        data = f.data

        self.assertEqual(2, len(data))
        self.assertNotEqual(vprops, v2props)

        for d in data:
            self.assertIn('properties', d)

    def test_can_add_multiple_values_and_set_properties_on_all(self):
        f = Field()
        vf = 'vf' + str(random())
        v = 'v' + str(random())
        v2 = 'v2' + str(random())
        p = str(random())
        f + v + v2

        f.properties[vf] = v
        data = f.data
        properties = f.properties.data

        self.assertEqual(2, len(f.values))
        self.assertEqual(2, len(properties))
        self.assertNotEqual(id(properties[0]), id(properties[1]))

        for prop in properties:
            self.assertIn(vf, prop)
            self.assertEqual(v, prop[vf])

    def test_can_add_multiple_values_and_set_properties_on_all_and_some_on_some(self):
        f = Field()
        vf = 'vf' + str(random())
        v = 'v' + str(random())
        v2 = 'v2' + str(random())
        only = 'only' + str(random())
        onlyprop = 'prop' + str(random())
        p = str(random())
        f + v + v2

        f.properties[vf] = v
        f[v2].properties[onlyprop] = only
        data = f.data
        all_properties = f.properties.data
        v_properties = f[v].properties.data
        v2_properties = f[v2].properties.data

        self.assertEqual(2, len(f.values))
        self.assertEqual(2, len(all_properties))
        self.assertEqual(1, len(v_properties))
        self.assertEqual(1, len(v2_properties))
        self.assertNotEqual(id(v_properties), id(v2_properties))
        self.assertNotEqual(len(v_properties[0]), len(v2_properties[0]))
        self.assertIn(vf, v_properties[0])
        self.assertEqual(v, v_properties[0][vf])
        self.assertIn(vf, v2_properties[0])
        self.assertEqual(v, v2_properties[0][vf])
        self.assertNotIn(onlyprop, v_properties[0])
        self.assertIn(onlyprop, v2_properties[0])
        self.assertEqual(only, v2_properties[0][onlyprop])

    def test_can_delete_value(self):
        f = Field()
        v = str(random())
        f + v
        data = f.data

        self.assertEqual(1, len(data))

        del f[v]

        data = f.data

        self.assertEqual(0, len(data))

    def test_can_delete_values_via_empty(self):
        f = Field()
        v = str(random())
        f + v
        data = f.data

        self.assertEqual(1, len(data))
        f.empty()

        data = f.data

        self.assertEqual(0, len(data))

    def test_can_get_changes_to_single_value(self):
        v = 'initial'
        f = Field(v)
        changed = 'changed'+ str(random())
        f[v] = changed
        changes = f.changes['values']

        self.assertIn('changes', changes)
        self.assertEqual(1, len(changes['changes']))
        self.assertIn('value', changes['changes'][0])
        self.assertIn('from', changes['changes'][0]['value'])
        self.assertIn('to', changes['changes'][0]['value'])
        self.assertEqual(v, changes['changes'][0]['value']['from'])
        self.assertEqual(changed, changes['changes'][0]['value']['to'])

    def test_can_get_changes_to_multiple_values(self):
        v = ['initial_one' + str(random()), 'initial_two' + str(random())]
        f = Field(values=v)
        changed_zero = 'changed_0_'+ str(random())
        changed_one = 'changed_1_'+ str(random())
        values = [(v[0], changed_zero), (v[1], changed_one)]
        f[v[0]] = changed_zero
        f[v[1]] = changed_one
        changes = f.changes

        self.assertIn('changes', changes['values'])
        self.assertEqual(2, len(changes['values']['changes']))

        for change in changes['values']['changes']:
            test = (change['value']['from'], change['value']['to'])
            self.assertIn(test, values)

    def test_can_get_values_added_to_field_via_changes(self):
        f = Field()
        v1 = str(random())
        v2 = str(random())
        both = [v1, v2,]
        f + v1 + v2

        changes = f.changes['values']

        self.assertIn('added', changes)
        self.assertTrue(2, len(changes['added']))

        for add in changes['added']:
            self.assertIn(add['value'], both)

    def test_can_get_changes_after_adding_value_and_changing_value(self):
        i = 'initial_' + str(random())
        f = Field(values=i)
        v = random()
        change = 'changed_' + str(random())
        f + v
        f[i] = change
        changes = f.changes['values']

        self.assertTrue(2, len(changes))
        self.assertIn('added', changes)
        self.assertIn('changes', changes)
        self.assertTrue(1, len(changes['added']))
        self.assertTrue(1, len(changes['changes']))
        self.assertEqual(v, changes['added'][0]['value'])
        self.assertEqual(i, changes['changes'][0]['value']['from'])
        self.assertEqual(change, changes['changes'][0]['value']['to'])

    def test_can_get_deleted_value_from_changes(self):
        i = 'initial_' + str(random())
        f = Field(values=i)
        del f[i]
        changes = f.changes['values']

        self.assertEqual(2, len(changes))
        self.assertIn('deleted', changes)
        self.assertEqual(1, len(changes['deleted']))
        self.assertEqual(i, changes['deleted'][0]['value'])

    def test_can_get_deleted_value_from_empty_from_changes(self):
        i = 'initial_' + str(random())
        f = Field(values=i)
        f.empty()
        changes = f.changes['values']

        self.assertEqual(2, len(changes))
        self.assertIn('deleted', changes)
        self.assertEqual(1, len(changes['deleted']))
        self.assertEqual(i, changes['deleted'][0]['value'])

    def test_can_add_value_and_delete_it_without_appearing_in_changes(self):
        f = Field()
        v = str(random())
        f + v

        del f[v]

        changes = f.changes['values']

        self.assertEqual(2, len(changes))
        self.assertIn('deleted', changes)
        self.assertEqual(1, len(changes['deleted']))
        self.assertEqual(v, changes['deleted'][0]['value'])

    def test_can_init_field_with_values_and_delete_one(self):
        v = 'strr_' + str(random())
        t = random()
        vals = [v, t,]
        f = Field(values=vals)
        data = f.data

        self.assertTrue(2, len(data))

        del f[t]

        changes = f.changes['values']
        self.assertEqual(1, len(f.data))
        self.assertEqual(2, len(changes))
        self.assertIn('deleted', changes)
        self.assertEqual(1, len(changes['deleted']))
        self.assertEqual(t, changes['deleted'][0]['value'])

    def test_can_init_field_with_values_and_delete_one_and_change_one(self):
        v = 'strr_' + str(random())
        t = random()
        vals = [v, t,]
        f = Field(values=vals)
        upped = 'updated!!'+ str(random())
        data = f.data

        self.assertTrue(2, len(data))

        del f[t]

        f[v] = upped
        changes = f.changes['values']

        self.assertEqual(1, len(f.data))
        self.assertEqual(3, len(changes))
        self.assertIn('changes', changes)
        self.assertEqual(1, len(changes['changes']))
        self.assertEqual(v, changes['changes'][0]['value']['from'])
        self.assertEqual(upped, changes['changes'][0]['value']['to'])
        self.assertIn('deleted', changes)
        self.assertEqual(1, len(changes['deleted']))
        self.assertEqual(t, changes['deleted'][0]['value'])

    def test_can_init_field_with_three_values_and_delete_one_and_change_one(self):
        v = 'strr_' + str(random())
        t = random()
        x = 'third_'+ str(random())
        vals = [v, t, x]
        f = Field(values=vals)
        upped = 'updated!!'+ str(random())
        data = f.data

        self.assertTrue(2, len(data))

        del f[t]

        f[v] = upped
        changes = f.changes['values']

        self.assertEqual(2, len(f.data))
        self.assertEqual(3, len(changes))
        self.assertIn('changes', changes)
        self.assertEqual(1, len(changes['changes']))
        self.assertEqual(v, changes['changes'][0]['value']['from'])
        self.assertEqual(upped, changes['changes'][0]['value']['to'])
        self.assertIn('deleted', changes)
        self.assertEqual(1, len(changes['deleted']))
        self.assertEqual(t, changes['deleted'][0]['value'])

    def test_can_get_added_property_on_value(self):
        i = 'initial_'+ str(random())
        f = Field(values=i)
        pk = 'key' + str(random())
        pv = 'val' + str(random())
        f[i].properties[pk] = pv

        changes = f.changes['values']

        self.assertEqual(2, len(changes))
        self.assertIn('changes', changes)
        self.assertEqual(1, len(changes['changes']))
        self.assertIn('properties', changes['changes'][0])
        self.assertIn(pk, changes['changes'][0]['properties'])
        self.assertEqual(pv, changes['changes'][0]['properties'][pk])

    def test_can_get_changed_property_on_value(self):
        iv = 'initial_value_' + str(random())
        ipk = 'property_key_' + str(random())
        ipv = 'initial_prop_val_' + str(random())
        up_pv = 'updated_prop_val_' + str(random())
        initial = {
            'value': iv,
            'properties': {
                ipk: ipv,
            }
        }
        f = Field(values=initial)
        f[iv].properties[ipk] = up_pv
        changes = f.changes['values']

        self.assertEqual(2, len(changes))
        self.assertIn('changes', changes)

        changes = changes['changes']

        self.assertEqual(1, len(changes))

        changes = changes[0]

        self.assertIn('properties', changes)

        properties = changes['properties']

        self.assertEqual(1, len(properties))
        self.assertIn(up_pv, properties.values())
        self.assertIn(ipk, properties.keys())

    def test_can_add_properties_to_multiple_values(self):
        iv = 'initail_value_' + str(random())
        pk = 'property_key' + str(random())
        pv = 'property_value' + str(random())
        iv2 = '2initail_value_' + str(random())
        pk2 = '2property_key' + str(random())
        pv2 = '2property_value' + str(random())
        value1 = {
            'value': iv,
        }
        value2 = {
            'value': iv2,
        }
        prop = {pk: pv}
        initial = [value1, value2,]
        f = Field(values=initial)
        f.properties[pk] = pv
        data = f.data

        self.assertEqual(2, len(data))

        for val in data:
            self.assertIn('properties', val)
            self.assertIn('value', val)
            self.assertEqual(val['properties'], prop)

    def test_can_add_properties_to_multiple_values_one_with_properties(self):
        iv = 'initail_value_' + str(random())
        pk = 'property_key' + str(random())
        pv = 'property_value' + str(random())
        ipk = 'initial_prop_key_' + str(random())
        ipv = 'initial_prop_val_' + str(random())
        iv2 = '2initail_value_' + str(random())
        pk2 = '2property_key' + str(random())
        pv2 = '2property_value' + str(random())
        value1 = {
            'value': iv,
            'properties': {
                ipk: ipv,
            }
        }
        value2 = {
            'value': iv2,
        }
        initial = [value1, value2,]
        f = Field(values=initial)
        f.properties[pk] = pv
        data = f.data

        self.assertEqual(2, len(data))

        for val in data:
            self.assertIn('properties', val)
            self.assertIn('value', val)

            properties = val['properties']

            if ipk in properties:
                self.assertEqual(2, len(properties))
                self.assertEqual(ipv, properties[ipk])
            else:
                self.assertEqual(1, len(properties))

            self.assertIn(pk, properties)
            self.assertEqual(pv, properties[pk])


class StringTests(unittest.TestCase):

    def test_can_create_string_without_value_and_python_type_ret_empty_string(self):
        f = String()

        self.assertEqual(0, len(f.values))

    def test_can_create_string_with_numeric_value_ret_string_for_gremlin(self):
        v = random()
        f = String(values=v, data_type='graph')
        data = f.data

        self.assertIsInstance(f.values[0], str)

    def test_can_create_string_with_numeric_value_ret_str_value(self):
        v = random()
        f = String(values=v)

        self.assertIsInstance(f.values[0], str)


class IntegerTests(unittest.TestCase):

    def test_can_create_type_with_non_numeric_value_and_get_integer_python(self):

        class X:
            pass

        v = ['43.34.', X(), 'iii', '987eee']
        f = Integer(values=choice(v))
        values = f.values

        self.assertIsInstance(values[0], int)
        self.assertEqual(values[0], 0)

    def test_can_create_type_with_non_numeric_value_and_get_integer_gremlin(self):

        class X:
            pass

        v = ['43.34.', X(), 'iii', '987eee']
        f = Integer(values=choice(v), data_type='graph')
        values = f.values

        self.assertIsInstance(values[0], int)
        self.assertEqual(values[0], 0)


class FloatTests(unittest.TestCase):

    def test_can_get_float_with_non_numeric_value_graph_data_type(self):
        v = 'dsafsd$@#4..'
        f = Float(values=v)
        f.data_type = 'graph'

        self.assertIsInstance(f.values[0], float)
        self.assertEqual(f.values[0], 0.0)

    def test_can_convert_integer_to_float(self):
        v = 12
        f = Float(values=v)

        self.assertIsInstance(f.values[0], float)
        self.assertEqual(f.values[0], 12.0)

    def test_can_convert_integer_to_float_graph_data_type(self):
        v = 12
        f = Float(values=v)
        f.data_type = 'graph'

        self.assertIsInstance(f.values[0], float)
        self.assertEqual(f.values[0], 12.0)


class IncrementTests(unittest.TestCase):

    def test_can_create_increment_without_default_value(self):
        f = Increment()

        self.assertEqual(1, len(f.values))
        self.assertEqual(0, f.values[0])

    def test_can_set_default_value(self):
        d = 9
        f = Increment(values=d)
        v = f.values[0]

        self.assertEqual(v, d)

    def test_will_only_increment_when_data_type_is_graph(self):
        f = Increment()
        f.data_type = 'graph'
        v = f.values[0]

        self.assertEqual(v, 1)
        self.assertEqual(1, len(f.values))

    def test_will_increment_when_data_type_is_graph_with_default_value(self):
        d = 9
        f = Increment(values=d)
        f.data_type = 'graph'
        v = f.values[0]

        self.assertEqual(v, d + 1)

    def test_will_increment_multiple_times(self):
        d = v = 9
        l = choice(range(4, 15))
        f = Increment(values=d)
        f.data_type = 'graph'

        for _ in range(l):
            v = f.values[0]

        self.assertEqual(v, d + l)


class BooleanTests(unittest.TestCase):

    def test_can_create_boolean_without_value_and_get_false(self):
        f = Boolean()

        self.assertIsInstance(f.values[0], bool)
        self.assertFalse(f.values[0])

    def test_can_create_boolean_without_value_and_get_false_for_graph(self):
        f = Boolean(data_type='graph')

        self.assertIsInstance(f.values[0], str)
        self.assertEqual('false', f.values[0])

    def test_can_get_boolean_from_non_bool_val(self):
        f = Boolean(values='ooo')

        self.assertIsInstance(f.values[0], bool)
        self.assertTrue(f.values[0])

    def test_can_get_boolean_from_none_graph_data_type(self):
        f = Boolean()
        f.data_type = 'graph'

        self.assertEqual(f.values[0], 'false')

    def test_can_get_graph_boolean_from_non_bool_val(self):
        f = Boolean(values='ooo')
        f.data_type = 'graph'

        self.assertEqual(f.values[0], 'true')


class MapTests(unittest.TestCase):

    def test_can_create_empty_dict_from_none(self):
        f = Map()

        self.assertIsInstance(f.values[0], dict)
        self.assertEqual(len(f.values[0]), 0)

    def test_can_create_empty_dict_from_none_for_graph_data_type(self):
        f = Map()
        f.data_type = 'graph'

        self.assertIsInstance(f.values[0], dict)
        self.assertEqual(len(f.values[0]), 0)

    def test_can_get_dict_from_valid_json_object_literal(self):
        ol = '{"name": "mark", "sex": "male", "loc": {"city": "here"}}'
        j = json.loads(ol)
        f = Map(values=j)

        self.assertIsInstance(f.values[0], dict)
        self.assertEqual(len(f.values[0]), len(j))

    def test_can_get_dict_from_valid_empty_json_object_literal(self):
        ol = '{}'
        j = json.loads(ol)
        f = Map(values=j)

        self.assertIsInstance(f.values[0], dict)
        self.assertEqual(len(f.values[0]), len(j))


class ListTests(unittest.TestCase):

    def test_can_get_empty_list_from_none(self):
        f = List()

        self.assertIsInstance(f.values[0], list)
        self.assertEqual(len(f.values[0]), 0)

    def test_can_get_empty_list_from_none_for_graph_data_type(self):
        f = List()

        self.assertIsInstance(f.values[0], list)
        self.assertEqual(len(f.values[0]), 0)

    def test_can_get_list_from_list(self):
        ol = ["one", 1, 222, "three", "four", random()]
        f = List(values=ol)

        self.assertIsInstance(f.values[0], list)
        self.assertEqual(len(f.values[0]), len(ol))
        self.assertIn(ol, f.values)

    def test_can_get_list_from_valid_json_array(self):
        ol = '["one", 1, 222, "three", "four"]'
        j = json.loads(ol)

        f = List(values=j)

        self.assertIsInstance(f.values[0], list)
        self.assertEqual(len(f.values[0]), len(j))

    def test_can_get_dict_from_empty_json_array(self):
        ol = '[]'
        j = json.loads(ol)
        f = List(values=j)

        self.assertIsInstance(f.values[0], list)
        self.assertEqual(len(f.values[0]), len(j))

    def test_can_get_list_from_valid_json_gremlin_response(self):
        ol = """[
                    {
                        "value" :["one", 1, 222, "three", "four"]
                    }
                ]"""
        j = json.loads(ol)
        f = List(values=j)

        self.assertIsInstance(f.values[0], list)
        self.assertEqual(len(f.values[0]), len(j[0]['value']))


class ImmutableFieldTests(unittest.TestCase):

    def test_can_set_a_value_and_return_only_one_value(self):
        f = GremlinID()
        v = str(random())
        f + v
        data = f.data

        self.assertFalse(isinstance(data, (list, tuple)))
        self.assertEqual(v, data)

    def test_can_set_value_and_return_changes_dict(self):
        iv = 'initial' + str(random())
        f = GremlinID(values=iv)
        v = str(random())
        f + v
        changes = f.changes

        self.assertIn('values', changes)
        self.assertIn('deleted', changes)

        values = changes['values']

        self.assertIn('added', values)

        added = values['added']

        self.assertEqual(1, len(added))
        self.assertEqual(v, added[0]['value'])


class FieldManagerTests(unittest.TestCase):

    def test_can_create_a_field_manager_without_fields(self):
        f = FieldManager()
        data = f.data

        self.assertIsInstance(f, FieldManager)
        self.assertEqual(0, len(data))

    def test_can_create_a_field_manager_with_one_field(self):
        string = String()
        fields = {'string': string}
        f = FieldManager(fields)
        data = f.data

        self.assertIsInstance(f, FieldManager)
        self.assertEqual(1, len(f.fields))
        self.assertEqual(0, len(data))

    def test_can_create_a_field_manager_with_one_field_and_add_value(self):
        string = String()
        fields = {'string': string}
        f = FieldManager(fields)
        v = str(random())
        f['string'] = v
        data = f.data

        self.assertIsInstance(f, FieldManager)
        self.assertEqual(1, len(f.fields))
        self.assertEqual(1, len(data))

    def test_can_create_a_field_manager_with_one_field_and_add_two_values(self):
        string = String()
        key = 'string' + str(random())
        fields = {key: string}
        f = FieldManager(fields)
        v = str(random())
        v2 = 'v2++++' + str(random())
        f[key] + v
        f[key] + v2
        data = f.data

        self.assertIsInstance(f, FieldManager)
        self.assertEqual(1, len(f.fields))
        self.assertEqual(1, len(data))
        self.assertEqual(2, len(data[key]))

    def test_can_create_manager_with_no_fields_and_dynamically_add_string(self):
        f = FieldManager(allow_undefined=True)
        key = 'str_key' + str(random())
        val = 'str_val' + str(random())
        f[key] = val

        self.assertEqual(1, len(f.fields))
        self.assertIn(key, f.fields)
        self.assertEqual(val, f.fields[key].values[0])
        self.assertIsInstance(f.fields[key], String)

    def test_can_create_manager_with_no_fields_and_dynamically_add_integer(self):
        f = FieldManager(allow_undefined=True)
        key = 'int_key' + str(random())
        val = randint(1, 1000)
        f[key] = val

        self.assertEqual(1, len(f.fields))
        self.assertIn(key, f.fields)
        self.assertEqual(val, f.fields[key].values[0])
        self.assertIsInstance(f.fields[key], Integer)

    def test_can_create_manager_with_no_fields_and_dynamically_add_float(self):
        f = FieldManager(allow_undefined=True)
        key = 'float_key' + str(random())
        val = random()
        f[key] = val

        self.assertEqual(1, len(f.fields))
        self.assertIn(key, f.fields)
        self.assertEqual(val, f.fields[key].values[0])
        self.assertIsInstance(f.fields[key], Float)

    def test_can_create_manager_with_no_fields_and_dynamically_add_boolean(self):
        f = FieldManager(allow_undefined=True)
        key = 'bool_key' + str(random())
        val = choice([True, False])
        f[key] = val

        self.assertEqual(1, len(f.fields))
        self.assertIn(key, f.fields)
        self.assertEqual(val, f.fields[key].values[0])
        self.assertIsInstance(f.fields[key], Boolean)

    def test_can_create_manager_with_no_fields_and_dynamically_add_list(self):
        f = FieldManager(allow_undefined=True)
        key = 'list_key' + str(random())
        val = [random(), random(), random()]
        f[key] = val

        self.assertEqual(1, len(f.fields))
        self.assertIn(key, f.fields)
        self.assertEqual(val, f.fields[key].values[0])
        self.assertIsInstance(f.fields[key], List)

    def test_can_create_manager_with_no_fields_and_dynamically_add_map(self):
        f = FieldManager(allow_undefined=True)
        key = 'list_key' + str(random())
        val = {'1': random(), '2': random(), '3' + str(random()): random()}
        f[key] = val

        self.assertEqual(1, len(f.fields))
        self.assertIn(key, f.fields)
        self.assertEqual(val, f.fields[key].values[0])
        self.assertIsInstance(f.fields[key], Map)

    def test_can_get_all_values_from_manager(self):
        f = FieldManager(allow_undefined=True)
        k = 'key' + str(random())
        k2 = 'key2' + str(random())
        v = random()
        v2 = random()
        f[k] + v
        f[k2] + v2
        values = f.values

        self.assertEqual(2, len(values))
        self.assertIn(k, values)
        self.assertIn(v, values[k])
        self.assertIn(k2, values)
        self.assertIn(v2, values[k2])

    def test_can_get_all_changes_to_initial_values(self):
        ik = 'ik' + str(random())
        ik2 = 'ik2' + str(random())
        iv = 'iv' + str(random())
        iv2 = 'iv2' + str(random())
        cv = 'changed'+ str(random())
        cv2 = 'changed_2_'+ str(random())
        initial = {
            'value': iv
        }
        initial2 = {
            'value': iv2
        }
        string = String(values=initial)
        string2 = String(values=initial2)
        fields = {ik: string, ik2: string2}
        f = FieldManager(fields=fields)
        f[ik][iv] = cv
        f[ik2][iv2] = cv2
        changes = f.changes

        self.assertEqual(2, len(changes))
        self.assertIn(ik, changes)
        self.assertEqual(2, len(changes[ik]['values']))
        self.assertEqual(iv, changes[ik]['values']['changes'][0]['value']['from'])
        self.assertEqual(cv, changes[ik]['values']['changes'][0]['value']['to'])
        self.assertEqual(2, len(changes[ik2]['values']))
        self.assertEqual(iv2, changes[ik2]['values']['changes'][0]['value']['from'])
        self.assertEqual(cv2, changes[ik2]['values']['changes'][0]['value']['to'])

    def test_can_get_the_fields_that_were_removed(self):
        fields = {
            'name': String(),
            'age': Integer(),
        }
        v = FieldManager(fields)

        del v['age']
        deleted = v.deleted

        self.assertEqual(len(deleted), 1)
        self.assertEqual(len(v.changed), 2)

    def test_can_get_data_from_fields_including_immutable(self):
        iid = str(random())
        iname = 'name_' + str(random())
        fields = {
            'id': GremlinID(values=iid),
            'name': String(values=iname),
        }
        f = FieldManager(fields=fields)
        data = f.data

        self.assertEqual(2, len(data))
        self.assertIn('id', data)
        self.assertIn('name', data)

    def test_can_get_data_from_fields_including_immutable_when_values_set_later(self):
        iid = str(random())
        iname = 'name_' + str(random())
        fields = {
            'id': GremlinID(),
            'name': String(),
        }
        f = FieldManager(fields=fields)
        f['id'] = iid
        f['name'] = iname
        data = f.data

        self.assertEqual(2, len(data))
        self.assertIn('id', data)
        self.assertIn('name', data)

    def test_can_overwrite_field_values_when_directly_set(self):
        iid = str(random())
        iname = 'name_' + str(random())
        fields = {
            'id': GremlinID(values=iid),
            'name': String(values=iname),
        }
        f = FieldManager(fields=fields)
        data = f.data

        self.assertEqual(1, len(data['name']))
        self.assertEqual(iname, data['name'][0]['value'])

        updated = 'new_name' + str(random())
        f['name'] = updated
        data = f.data

        self.assertEqual(1, len(data['name']))
        self.assertEqual(updated, data['name'][0]['value'])

    def test_can_add_value_to_field(self):
        iid = str(random())
        iname = 'name_' + str(random())
        fields = {
            'id': GremlinID(values=iid),
            'name': String(values=iname),
        }
        f = FieldManager(fields=fields)
        data = f.data

        self.assertEqual(1, len(data['name']))
        self.assertEqual(iname, data['name'][0]['value'])

        updated = 'new_name' + str(random())
        f['name'] + updated
        data = f.data
        both = [iname, updated]

        self.assertEqual(2, len(data['name']))

        for val in data['name']:
            self.assertIn(val['value'], both)


if __name__ == '__main__':
    unittest.main()
