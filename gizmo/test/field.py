import random
import json
import unittest

from gizmo.field import *


class FieldTests(unittest.TestCase):

    def test_can_create_field_without_value(self):
        f = Field()

        self.assertEqual(type(f), Field)
        self.assertEqual(f.value, None)

    def test_can_create_field_with_value(self):
        v = str(random.random())
        f = Field(value=v)

        self.assertEqual(type(f), Field)
        self.assertEqual(f.value, v)

    def test_can_create_field_without_value_and_set_max(self):
        f = Field(set_max=5)

        for i in range(10):
            f.value = i

        self.assertEqual(f.value, 4)

    def test_can_create_field_with_value_and_set_max(self):
        v = '5555'
        f = Field(value=v, set_max=5)

        for i in range(10):
            f.value = i

        self.assertEqual(f.value, 4)

    def test_can_create_field_wtih_value_that_cannot_be_changed(self):
        v = str(random.random())
        f = Field(value=v, set_max=0)

        for i in range(10):
            f.value = i

        self.assertEqual(f.value, v)

    def test_can_determine_if_field_has_changed(self):
        f = Field()

        self.assertFalse(f.changed())
        f.value = random.random()
        self.assertTrue(f.changed())


class StringTests(unittest.TestCase):

    def test_can_create_string_without_value_and_python_type_ret_none(self):
        f = String()

        self.assertIsNone(f.value)

    def test_can_create_string_without_value_and_gremlin_type_ret_string(self):
        f = String(data_type='graph')

        self.assertIsInstance(f.value, str)

    def test_can_create_string_with_numeric_value_ret_string_for_gremlin(self):
        v = random.random()
        f = String(value=v, data_type='graph')

        self.assertIsInstance(f.value, str)

    def test_can_create_string_with_numeric_value_ret_numeric_value(self):
        v = random.random()
        f = String(value=v)

        self.assertIsInstance(f.value, type(v))


class IntegerTests(unittest.TestCase):

    def test_can_create_integer_without_value_and_python_type_and_get_zero(self):
        f = Integer()

        self.assertIsInstance(f.value, int)
        self.assertEqual(f.value, 0)

    def test_can_create_integer_without_value_and_gremlin_type_and_get_zero(self):
        f = Integer(data_type='graph')

        self.assertIsInstance(f.value, int)
        self.assertEqual(f.value, 0)

    def test_can_create_type_with_non_numeric_value_and_get_integer_python(self):

        class X(object):
            pass

        v = ['43.34.', X(), 'iii', '987eee']
        f = Integer(value=random.choice(v))

        self.assertIsInstance(f.value, int)
        self.assertEqual(f.value, 0)

    def test_can_create_type_with_non_numeric_value_and_get_integer_gremlin(self):

        class X(object):
            pass

        v = ['43.34.', X(), 'iii', '987eee']
        f = Integer(value=random.choice(v), data_type='graph')

        self.assertIsInstance(f.value, int)
        self.assertEqual(f.value, 0)


class IncrementTests(unittest.TestCase):

    def test_can_create_increment_with_zero_value(self):
        f = Increment()
        v = f.value

        self.assertEqual(v, 0)

    def test_can_set_default_value(self):
        d = 9
        f = Increment(value=d)
        v = f.value

        self.assertEqual(v, d)

    def test_will_only_increment_when_data_type_is_graph(self):
        f = Increment()
        f.data_type = 'graph'
        v = f.value

        self.assertEqual(v, 1)

    def test_will_increment_when_data_type_is_graph_with_default_value(self):
        d = 9
        f = Increment(value=d)
        f.data_type = 'graph'
        v = f.value

        self.assertEqual(v, d + 1)

    def test_will_increment_multiple_times(self):
        d = v = 9
        l = random.choice(range(4, 15))
        f = Increment(value=d)
        f.data_type = 'graph'

        for _ in range(l):
            v = f.value

        self.assertEqual(v, d + l)


class FloatTests(unittest.TestCase):

    def test_can_get_float_with_none_value(self):
        f = Float()

        self.assertIsInstance(f.value, float)
        self.assertEqual(f.value, 0.0)

    def test_can_get_float_with_non_numeric_value_graph_data_type(self):
        v = 'dsafsd$@#4..'
        f = Float(value=v)
        f.data_type = 'graph'

        self.assertIsInstance(f.value, float)
        self.assertEqual(f.value, 0.0)

    def test_can_convert_integer_to_float(self):
        v = 12
        f = Float(value=v)

        self.assertIsInstance(f.value, float)
        self.assertEqual(f.value, 12.0)

    def test_can_convert_integer_to_float_graph_data_type(self):
        v = 12
        f = Float(value=v)
        f.data_type = 'graph'

        self.assertIsInstance(f.value, float)
        self.assertEqual(f.value, 12.0)


class BooleanTests(unittest.TestCase):

    def test_can_get_boolean_from_none(self):
        f = Boolean()

        self.assertIsInstance(f.value, bool)
        self.assertFalse(f.value)

    def test_can_get_boolean_from_non_bool_val(self):
        f = Boolean(value='ooo')
        print(f.value)
        self.assertIsInstance(f.value, bool)
        self.assertTrue(f.value)

    def test_can_get_boolean_from_none_graph_data_type(self):
        f = Boolean()
        f.data_type = 'graph'

        self.assertEqual(f.value, 'false')

    def test_can_get_boolean_from_non_bool_val(self):
        f = Boolean(value='ooo')
        f.data_type = 'graph'

        self.assertEqual(f.value, 'true')


class MapTests(unittest.TestCase):

    def test_can_create_empty_dict_from_none(self):
        f = Map()

        self.assertIsInstance(f.value, dict)
        self.assertEqual(len(f.value), 0)

    def test_can_create_empty_dict_from_none_for_graph_data_type(self):
        f = Map()
        f.data_type = 'graph'

        self.assertIsInstance(f.value, dict)
        self.assertEqual(len(f.value), 0)

    def test_can_get_dict_from_valid_json_object_literal(self):
        ol = '{"name": "mark", "sex": "male", "loc": {"city": "here"}}'
        j = json.loads(ol)
        f = Map(value=j)

        self.assertIsInstance(f.value, dict)
        self.assertEqual(len(f.value), len(j))

    def test_can_get_dict_from_valid_empty_json_object_literal(self):
        ol = '{}'
        j = json.loads(ol)
        f = Map(value=j)

        self.assertIsInstance(f.value, dict)
        self.assertEqual(len(f.value), len(j))


class ListTests(unittest.TestCase):

    def test_can_get_empty_list_from_none(self):
        f = List()

        self.assertIsInstance(f.value, list)
        self.assertEqual(len(f.value), 0)

    def test_can_get_empty_list_from_none_for_graph_data_type(self):
        f = List()

        self.assertIsInstance(f.value, list)
        self.assertEqual(len(f.value), 0)

    def test_can_get_list_from_valid_json_array(self):
        ol = '["one", 1, 222, "three", "four"]'
        j = json.loads(ol)
        f = Map(value=j)

        self.assertIsInstance(f.value, list)
        self.assertEqual(len(f.value), len(j))

    def test_can_get_dict_from_empty_json_array(self):
        ol = '[]'
        j = json.loads(ol)
        f = Map(value=j)

        self.assertIsInstance(f.value, dict)
        self.assertEqual(len(f.value), len(j))


class EnumTests(unittest.TestCase):

    def setUp(self):
        self.allowed = [1, 2, 3, 'one', 'two', 'three']
        super(EnumTests, self).setUp()

    def test_can_get_default_value_if_none_is_passed(self):
        f = Enum(allowed=self.allowed)

        self.assertEqual(f.value, self.allowed[0])

    def test_can_get_set_allowed_value(self):
        v = random.choice(self.allowed)
        f = Enum(allowed=self.allowed, value=v)

        self.assertEqual(f.value, v)

    def test_cannot_init_value_that_isnt_allowed(self):
        v = 'value' + str(random.random())
        f = Enum(value=v, allowed=self.allowed)

        self.assertNotEqual(f.value, v)

    def test_cannot_set_value_that_isnt_allowed(self):
        f = Enum(allowed=self.allowed)
        v = 'value' + str(random.random())
        f.value = v

        self.assertNotEqual(f.value, v)

if __name__ == '__main__':
    unittest.main()
