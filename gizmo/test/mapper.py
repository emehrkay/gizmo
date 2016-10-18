import asyncio
import unittest
import json
import copy

from random import randrange, random, choice, randint
from pprint import pprint

from gremlinpy.gremlin import Gremlin

from gizmo.connection import Response
from gizmo.mapper import *
from gizmo.exception import *
from gizmo.entity import GenericVertex, GenericEdge
from gizmo.util import camel_to_underscore, entity_name, _query_debug


DEFAULT_PROPRTIES = sorted([GIZMO_LABEL[0], GIZMO_TYPE, GIZMO_ENTITY])


class TestVertex(GenericVertex):
    pass


class TestEdge(GenericEdge):
    label = 'test_edge_label'


class TestRequest:

    async def send(*args, **kwargs):
        pass


def get_dict_key(params, value, unset=False):
    for k, v in params.items():
        if v == value:
            if unset:
                del(params[k])
            return k, params

    return None, dict


def build_prop(key, value, params=None, value_properties=None):
    prop = ['property({}'.format(key)]
    properties = []

    if value_properties:
        for k, v in value_properties.items():
            kv, _ = get_dict_key(params, k)
            vv, _ = get_dict_key(params, v)
            properties += ['{}, {}'.format(kv, vv)]

    prop += [', {}'.format(value)]

    if properties:
        properties = ', '.join(properties)
        prop += [', {}'.format(properties)]

    prop += [')']

    return ''.join(prop)


def build_params(entity, values, mapper, params=None, value_properties=None,
    entities=None, deleted=None):
    params = copy.deepcopy(params or {})
    expected = []
    value_properties = value_properties or {}
    deleted = deleted or []
    entity_name = str(entity)
    entity_alias = '{}_alias'.format(entity_name)

    def get_key(key):
        if key.startswith('T.'):
            return key
        else:
            k, _ = get_dict_key(params, key)
            return k

    def delete_key(key):
        nonlocal expected
        k, _ = get_dict_key(params, key)
        a, _ = get_dict_key(params, entity_alias)
        expected += ['as({})'.format(a)]
        expected += ['properties({})'.format(k)]
        expected += ['sideEffect{it.get().remove()}']
        expected += ['select({})'.format(a)]
        pass

    for key, val in entity.data.items():
        if key in deleted:
            delete_key(key)
            continue

        if isinstance(val, (list, tuple)) and len(val)\
            and isinstance(val[0], dict)\
            and 'value' in val[0]:
            if isinstance(val[0], (list, tuple)):
                for v in val[0]['value']:
                    if v not in values:
                        continue

                    var, _ = get_dict_key(params, v)
                    prop = build_prop(get_key(key), var, params,
                        value_properties.get(v, None))

                    expected.append(prop)
            else:
                if val[0]['value'] not in values:
                    continue

                v = val[0]['value']
                var, _ = get_dict_key(params, v)
                prop = build_prop(get_key(key), var, params,
                    value_properties.get(v, None))

                expected.append(prop)
        else:
            if val not in values:
                continue

            var, _ = get_dict_key(params, val)
            prop = build_prop(get_key(key), var, params,
                value_properties.get(val, None))

            expected.append(prop)

    return expected


def build_vertex_create_query(entity, values, mapper, params=None,
    value_properties=None, entities=None, deleted=None, return_var=None):
    expected = []
    add = '{}.addV()'.format(mapper.gremlin.gv)

    if return_var:
        expected += ['{} = {}'.format(return_var, add)]
    else:
        expected += [add]

    expected += build_params(entity=entity, values=values, params=params,
        value_properties=value_properties, entities=entities, mapper=mapper,
        deleted=deleted)

    expected.append('next()')

    return '.'.join(expected)


def build_update_query(entity, values, mapper, params=None,
    value_properties=None, entities=None, deleted=None, return_var=None):
    entity_type, _id = entity.get_rep()
    _id, _ = get_dict_key(params, _id)
    expected = []
    update = '{}.{}({})'.format(mapper.gremlin.gv, entity_type.upper(), _id)

    if return_var:
        expected += ['{} = {}'.format(return_var, update)]
    else:
        expected += [update]

    expected += build_params(entity=entity, values=values, params=params,
        value_properties=value_properties, entities=entities, mapper=mapper,
        deleted=deleted)

    return '.'.join(expected)


def build_delete_query(entity, mapper, params=None):
    params = params or {}
    e_type, _id = entity.get_rep()
    _id, _ = get_dict_key(params, _id)

    return '{}.{}({}).next().remove()'.format(mapper.gremlin.gv,
        e_type.upper(), _id)


def build_edge_create_query(entity, out_v, in_v, values, mapper, params=None,
    value_properties=None, entities=None, deleted=None):

    def g_v(_id):
        if isinstance(_id, str) and _id.startswith('var:'):
            _id = _id.split(':')[1]
        else:
            _id, _ = get_dict_key(params, _id)

        return '{}.V({}).next()'.format(mapper.gremlin.gv, _id)

    v_in = g_v(in_v)
    v_out = g_v(out_v)
    label, _ = get_dict_key(params, entity[GIZMO_LABEL[0]])
    edge_params = []
    ignore = [GIZMO_LABEL[0], GIZMO_LABEL[1], GIZMO_TYPE]

    for f, changes in entity.changes.items():
        if f in ignore:
            continue

        try:
            if changes['immutable']:
                val = changes['values']['values'][-1]
            else:
                val = changes['values'][-1]
        except:
            continue

        f_arg, _ = get_dict_key(params, f)
        v_arg, _ = get_dict_key(params, val)
        edge_params += [f_arg, v_arg]


    edge_params = ', '.join(edge_params)
    expected = [v_out, ]

    if edge_params:
        expected += ['addEdge({}, {}, {})'.format(label, v_in, edge_params)]
    else:
        expected += ['addEdge({}, {})'.format(label, v_in)]

    return '.'.join(expected)


class QueryTests(unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.request = TestRequest()
        self.mapper = Mapper(request=self.request)
        self.query = Query(mapper=self.mapper)

    def test_can_save_vertex_with_no_field_values(self):
        v = TestVertex()

        self.query.save(v)

        queries = self.query.queries
        values = ['vertex', entity_name(v), str(v), GIZMO_ENTITY, GIZMO_TYPE]

        self.assertEqual(1, len(queries))

        entry = queries[0]
        params = entry['params']
        expected = build_vertex_create_query(entity=v, values=values,
            params=params, mapper=self.mapper)

        self.assertEqual(expected, entry['script'])
        self.assertEqual(len(values), len(params))

    def test_can_save_vertex_with_one_field_value(self):
        v = TestVertex()
        ik = '__some_field' + str(random())
        iv = 'some_value' + str(random())
        v[ik] = iv

        self.query.save(v)

        values = [ik, iv, 'vertex', entity_name(v), str(v), GIZMO_ENTITY,
            GIZMO_TYPE]
        queries = self.query.queries

        self.assertEqual(1, len(queries))

        entry = queries[0]
        params = entry['params']
        expected = build_vertex_create_query(entity=v, values=values,
            params=params, mapper=self.mapper)
        self.assertEqual(expected, entry['script'])
        self.assertEqual(len(values), len(params))

    def test_can_save_vertex_with_one_field_value_one_property(self):
        v = TestVertex()
        ik = 'some_field' + str(random())
        iv = 'some_value' + str(random())
        pk = 'prop_key' + str(random())
        pv = 'prop_value' + str(random())
        v[ik] = iv
        v[ik][iv].properties[pk] = pv

        self.query.save(v)

        values = [ik, iv, 'vertex', entity_name(v), str(v), GIZMO_ENTITY,
            GIZMO_TYPE]
        value_properties = {
            iv: {
                pk: pv,
            }
        }
        queries = self.query.queries

        self.assertEqual(1, len(queries))

        entry = queries[0]
        params = entry['params']
        expected = build_vertex_create_query(entity=v, values=values, params=params,
            value_properties=value_properties, mapper=self.mapper)
        self.assertEqual(expected, entry['script'])

        # 2 is the number of params added because of the one property defined
        self.assertEqual(len(values) + 2, len(params))

    def test_can_save_vertex_with_one_field_value_two_properties(self):
        v = TestVertex()
        ik = 'some_field' + str(random())
        iv = 'some_value' + str(random())
        pk = 'prop_key' + str(random())
        pv = 'prop_value' + str(random())
        pk2 = 'prop2_key' + str(random())
        pv2 = 'prop2_value' + str(random())
        v[ik] = iv
        v[ik][iv].properties[pk] = pv
        v[ik][iv].properties[pk2] = pv2

        self.query.save(v)

        values = [ik, iv, 'vertex', entity_name(v), str(v), GIZMO_ENTITY, GIZMO_TYPE]
        value_properties = {
            iv: {
                pk: pv,
                pk2: pv2,
            }
        }
        queries = self.query.queries

        self.assertEqual(1, len(queries))

        entry = queries[0]
        params = entry['params']
        expected = build_vertex_create_query(entity=v, values=values, params=params,
            value_properties=value_properties, mapper=self.mapper)

        self.assertEqual(expected, entry['script'])

        # 4 is the number of params added because of the two props defined
        self.assertEqual(len(values) + 4, len(params))

    def test_can_save_vertex_with_two_field_values(self):
        v = TestVertex()
        ik = '__some_field' + str(random())
        iv = 'some_value' + str(random())
        ik2 = '2__some_field' + str(random())
        iv2 = '2some_value' + str(random())
        v[ik] = iv
        v[ik2] = iv2

        self.query.save(v)

        values = [ik, iv, ik2, iv2, 'vertex', entity_name(v), str(v),
            GIZMO_ENTITY, GIZMO_TYPE]
        queries = self.query.queries

        self.assertEqual(1, len(queries))

        entry = queries[0]
        params = entry['params']
        expected = build_vertex_create_query(entity=v, values=values,
            params=params, mapper=self.mapper)
        self.assertEqual(expected, entry['script'])
        self.assertEqual(len(values), len(params))

    def test_can_save_vertex_with_two_fields_value_one_property_on_one_field(self):
        v = TestVertex()
        ik = 'some_field' + str(random())
        iv = 'some_value' + str(random())
        ik2 = '2__some_field' + str(random())
        iv2 = '2some_value' + str(random())
        pk = 'prop_key' + str(random())
        pv = 'prop_value' + str(random())
        v[ik] = iv
        v[ik][iv].properties[pk] = pv
        v[ik2] = iv2

        self.query.save(v)

        values = [ik, iv, iv2, ik2, 'vertex', entity_name(v), str(v),
            GIZMO_ENTITY, GIZMO_TYPE]
        value_properties = {
            iv: {
                pk: pv,
            }
        }
        queries = self.query.queries

        self.assertEqual(1, len(queries))

        entry = queries[0]
        params = entry['params']
        expected = build_vertex_create_query(entity=v, values=values, params=params,
            value_properties=value_properties, mapper=self.mapper)
        self.assertEqual(expected, entry['script'])

        # 2 is the number of params added because of the one property defined
        self.assertEqual(len(values) + 2, len(params))

    def test_can_save_vertex_with_two_fields_value_one_property_on_each_field(self):
        v = TestVertex()
        ik = 'some_field' + str(random())
        iv = 'some_value' + str(random())
        ik2 = '2__some_field' + str(random())
        iv2 = '2some_value' + str(random())
        pk = 'prop_key' + str(random())
        pv = 'prop_value' + str(random())
        pk2 = '2prop_key' + str(random())
        pv2 = '2prop_value' + str(random())
        v[ik] = iv
        v[ik][iv].properties[pk] = pv
        v[ik2] = iv2
        v[ik2][iv2].properties[pk2] = pv2

        self.query.save(v)

        values = [ik, iv, iv2, ik2, 'vertex', entity_name(v), str(v),
            GIZMO_ENTITY, GIZMO_TYPE, pk2, pv2, pk, pv]
        value_properties = {
            iv: {
                pk: pv,
            },
            iv2: {
                pk2: pv2,
            }
        }
        queries = self.query.queries

        self.assertEqual(1, len(queries))

        entry = queries[0]
        params = entry['params']
        expected = build_vertex_create_query(entity=v, values=values, params=params,
            value_properties=value_properties, mapper=self.mapper)
        self.assertEqual(expected, entry['script'])
        self.assertEqual(len(values), len(params))

    def test_can_save_vertex_with_two_fields_value_two_props_on_one_field(self):
        v = TestVertex()
        ik = 'some_field' + str(random())
        iv = 'some_value' + str(random())
        ik2 = '2__some_field' + str(random())
        iv2 = '2some_value' + str(random())
        pk = 'prop_key' + str(random())
        pv = 'prop_value' + str(random())
        pk2 = '2prop_key' + str(random())
        pv2 = '2prop_value' + str(random())
        v[ik] = iv
        v[ik][iv].properties[pk] = pv
        v[ik][iv].properties[pk2] = pv2
        v[ik2] = iv2

        self.query.save(v)

        values = [ik, iv, iv2, ik2, 'vertex', entity_name(v), str(v),
            GIZMO_ENTITY, GIZMO_TYPE, pk2, pv2, pk, pv]
        value_properties = {
            iv: {
                pk: pv,
                pk2: pv2,
            },
        }
        queries = self.query.queries

        self.assertEqual(1, len(queries))

        entry = queries[0]
        params = entry['params']
        expected = build_vertex_create_query(entity=v, values=values, params=params,
            value_properties=value_properties, mapper=self.mapper)
        self.assertEqual(expected, entry['script'])
        self.assertEqual(len(values), len(params))

    def test_can_save_vertex_with_two_fields_value_two_props_on_one_field_one_on_the_other(self):
        v = TestVertex()
        ik = 'some_field' + str(random())
        iv = 'some_value' + str(random())
        ik2 = '2__some_field' + str(random())
        iv2 = '2some_value' + str(random())
        pk = 'prop_key' + str(random())
        pv = 'prop_value' + str(random())
        pk2 = '2prop_key' + str(random())
        pv2 = '2prop_value' + str(random())
        pk3 = '3prop_key' + str(random())
        pv3 = '3prop_value' + str(random())
        v[ik] = iv
        v[ik][iv].properties[pk] = pv
        v[ik][iv].properties[pk2] = pv2
        v[ik2] = iv2
        v[ik2][iv2].properties[pk3] = pv3

        self.query.save(v)

        values = [ik, iv, iv2, ik2, 'vertex', entity_name(v), str(v),
            GIZMO_ENTITY, GIZMO_TYPE, pk2, pv2, pk, pv, pv3, pk3]
        value_properties = {
            iv: {
                pk: pv,
                pk2: pv2,
            },
            iv2: {
                pk3: pv3
            }
        }
        queries = self.query.queries

        self.assertEqual(1, len(queries))

        entry = queries[0]
        params = entry['params']
        expected = build_vertex_create_query(entity=v, values=values, params=params,
            value_properties=value_properties, mapper=self.mapper)
        self.assertEqual(expected, entry['script'])
        self.assertEqual(len(values), len(params))

    def test_can_update_vertex_with_no_field_values(self):
        _id = str(random())
        data = {GIZMO_ID: _id}
        v = TestVertex(data)

        self.query.save(v)

        queries = self.query.queries
        values = ['vertex', entity_name(v), GIZMO_ENTITY, GIZMO_ID]

        self.assertEqual(1, len(queries))

        entry = queries[0]
        params = entry['params']
        expected = build_update_query(entity=v, values=values, mapper=self.mapper,
            params=params)

        self.assertEqual(expected, entry['script'])

        # +1 because we cannot add the _id var to the values list
        self.assertEqual(len(values) + 1, len(params))

    def test_can_update_vertext_with_one_field_and_two_properties(self):
        # we only need one test bc properties are tested for adding vertex
        _id = str(random())
        data = {GIZMO_ID: _id}
        v = TestVertex(data)
        ik = 'some_field' + str(random())
        iv = 'some_value' + str(random())
        pk = 'prop_key' + str(random())
        pv = 'prop_value' + str(random())
        pk2 = '2prop_key' + str(random())
        pv2 = '2prop_value' + str(random())
        v[ik] = iv
        v[ik][iv].properties[pk] = pv
        v[ik][iv].properties[pk2] = pv2

        self.query.save(v)

        values = [ik, iv, 'vertex', entity_name(v), GIZMO_ENTITY, GIZMO_ID,
            pk2, pv2, pk, pv]
        value_properties = {
            iv: {
                pk: pv,
                pk2: pv2,
            },
        }

        queries = self.query.queries

        self.assertEqual(1, len(queries))

        entry = queries[0]
        params = entry['params']
        expected = build_update_query(entity=v, values=values, mapper=self.mapper,
            params=params, value_properties=value_properties)
        self.assertEqual(expected, entry['script'])

        # +1 because we cannot add the _id var to the values list
        self.assertEqual(len(values) + 1, len(params))

    def test_can_update_vertex_with_two_fields_after_deleting_one(self):
        _id = str(random())
        ik = 'key' + str(random())
        iv = 'val' + str(random())
        ik2 = '2key' + str(random())
        iv2 = '2val' + str(random())
        data = {GIZMO_ID: _id, ik: iv, ik2: iv2}
        v = TestVertex(data)

        del v[ik2]

        values = [ik, iv, ik2, iv2, GIZMO_ENTITY, entity_name(v), GIZMO_TYPE, 'vertex']
        deleted = [ik2, ]

        self.query.save(v)
        queries = self.query.queries
        entry = queries[0]
        params = entry['params']
        expected = build_update_query(entity=v, values=values, mapper=self.mapper,
            params=params, deleted=deleted)

        self.assertEqual(expected, entry['script'])

    def test_cannot_delete_vertex(self):
        v = TestVertex()

        self.assertRaises(AstronomerQueryException, self.query.delete, v)

    def test_can_delete_vertex(self):
        _id = str(random())
        d = {GIZMO_ID: _id}
        v = TestVertex(data=d)

        self.query.delete(v)

        queries = self.query.queries
        entry = queries[0]
        expected = build_delete_query(entity=v, mapper=self.mapper,
            params=entry['params'])

        self.assertEqual(expected, entry['script'])
        self.assertEqual(1, len(entry['params']))

    def test_cannot_save_edge(self):
        e = TestEdge()

        self.assertRaises(AstronomerQueryException, self.query.save, e)

    def test_cannot_save_edge_one_end_isnt_defined(self):
        d = {
            'outV': 15,
        }
        e = TestEdge(data=d)

        self.assertRaises(AstronomerQueryException, self.query.save, e)

    def test_can_save_edge_with_ends_being_ids(self):
        d = {
            'outV': 10,
            'inV': 99,
        }
        e = TestEdge(data=d)

        self.query.save(e)

        values = [str(e), e.outV, e.inV,
            entity_name(e)]
        entry = self.query.queries[0]
        expected = build_edge_create_query(entity=e, out_v=e.outV,
            in_v=e.inV, values=values, mapper=self.mapper,
            params=entry['params'])

        self.assertEqual(expected, entry['script'])
        self.assertEqual(len(values) + 1, len(entry['params']))

    def test_can_save_edge_one_end_being_new_entity_other_being_id(self):
        v = TestVertex()
        d = {
            'outV': 15,
            'inV': v,
        }
        e = TestEdge(data=d)

        self.query.save(e)

        in_v = 'var:{}'.format(self.mapper.get_entity_variable(v))
        values = [GIZMO_LABEL[0], str(e), e.outV,
            entity_name(e)]
        entry = self.query.queries[0]
        expected = build_edge_create_query(entity=e, out_v=e.outV,
            in_v=in_v, values=values, mapper=self.mapper,
            params=entry['params'])

        self.assertEqual(expected, entry['script'])
        self.assertEqual(len(values), len(entry['params']))

    def test_can_save_edge_with_two_new_entities(self):
        v = TestVertex()
        v2 = TestVertex()
        d = {
            'outV': v2,
            'inV': v,
        }
        e = TestEdge(data=d)

        self.query.save(e)

        in_v = 'var:{}'.format(self.mapper.get_entity_variable(v))
        out_v = 'var:{}'.format(self.mapper.get_entity_variable(v2))
        values = [GIZMO_LABEL[0], str(e), entity_name(e)]
        entry = self.query.queries[0]
        expected = build_edge_create_query(entity=e, out_v=out_v,
            in_v=in_v, values=values, mapper=self.mapper,
            params=entry['params'])

        self.assertEqual(expected, entry['script'])
        self.assertEqual(len(values), len(entry['params']))


class MapperTests(unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.request = TestRequest()
        self.mapper = Mapper(request=self.request)
        self.query = Query(mapper=self.mapper)
        self.gremlin = Gremlin()
        self.ioloop = asyncio.get_event_loop()

    def test_mapper_instance(self):
        m = Mapper(self.request, self.gremlin)

        self.assertTrue(type(m) == Mapper)

    def test_can_create_vertex(self):
        v = self.mapper.create(entity=TestVertex)

        self.assertTrue(isinstance(v, Vertex))
        self.assertEqual(v[GIZMO_TYPE], 'vertex')

    def test_can_create_vertex_with_data(self):
        d = {'some_field': str(random())}
        v = self.mapper.create(d, TestVertex)
        vd = v.data

        self.assertTrue(isinstance(v, Vertex))
        self.assertIn('some_field', vd)
        self.assertEqual(1, len(vd['some_field']))
        self.assertEqual(d['some_field'], vd['some_field'][0]['value'])

    def test_can_update_existing_vertex(self):
        vid = '1111'
        d = {
            GIZMO_ID: vid,
            'some_field': 'mark',
        }
        v = self.mapper.create(d, TestVertex)
        v['some_field'] = 'xxxx'

        self.mapper.save(v)._build_queries()

        sent_params = copy.deepcopy(self.mapper.params)
        values = ['some_field', 'xxxx', 'vertex', entity_name(v), GIZMO_ENTITY,
            GIZMO_TYPE]
        queries = self.mapper.queries

        self.assertEqual(2, len(queries))

        params = self.mapper.params
        return_var = self.mapper.get_entity_variable(v)
        expected = build_update_query(entity=v, values=values,
            params=params, mapper=self.mapper, return_var=return_var)

        self.assertEqual(expected, queries[0])

        # +1 for id
        self.assertEqual(len(values) + 1, len(params))

    def test_can_queue_save_vertex_with_two_params_query(self):
        d = {
            'some_field': 'mark',
        }
        v = self.mapper.create(d, TestVertex)

        self.mapper.save(v)._build_queries()

        params = copy.deepcopy(self.mapper.params)
        values = ['some_field', 'mark', 'vertex', entity_name(v), GIZMO_ENTITY,
            GIZMO_TYPE, str(v)]
        return_var = self.mapper.get_entity_variable(v)
        expected = build_vertex_create_query(entity=v, values=values,
            params=params, mapper=self.mapper, return_var=return_var)

        self.assertEqual(expected, self.mapper.queries[0])
        self.assertEqual(len(values), len(params))

    def test_can_delete_existing_vertex(self):
        vid = '1111'
        d = {
            GIZMO_ID: vid,
            'some_field': 'mark',
        }
        v = self.mapper.create(d, TestVertex)
        self.mapper.delete(v)._build_queries()
        params = copy.deepcopy(self.mapper.params)
        sent_params = copy.deepcopy(self.mapper.params)
        eyed = get_dict_key(params, vid)
        expected = '{}.V({}).next().remove()'.format(self.mapper.gremlin.gv,
            eyed[0])

        self.assertEqual(expected, self.mapper.queries[0])

    def test_can_delete_multiple_entities(self):
        v1 = {'id': '15'}
        v2 = {'id': '10'}
        out_v = self.mapper.create(v1, TestVertex)
        in_v = self.mapper.create(v2, TestVertex)
        ed = {'outV': out_v, 'inV': in_v, 'id': '44'}
        edge = self.mapper.create(ed, TestEdge)

        self.mapper.delete(out_v)
        self.mapper.delete(in_v)
        self.mapper.delete(edge)
        self.mapper._build_queries()

        params = self.mapper.params
        v1_id = get_dict_key(params, v1['id'])
        v2_id = get_dict_key(params, v2['id'])
        e_id = get_dict_key(params, ed['id'])
        gv = self.mapper.gremlin.gv
        expected = [
            '{}.V({}).next().remove()'.format(gv, v1_id[0]),
            '{}.V({}).next().remove()'.format(gv, v2_id[0]),
            '{}.E({}).next().remove()'.format(gv, e_id[0]),
        ]

        self.assertEqual(3, len(self.mapper.queries))
        self.assertEqual(3, len(self.mapper.params))

        for exp in expected:
            self.assertIn(exp, self.mapper.queries)

    def test_can_call_callback_when_save_method_is_called(self):
        variable = {'v': ''}
        updated = random()

        def save_test_callback(entity):
            variable['v'] = updated

        async def test():
            m = self.mapper.create({}, TestVertex)
            await self.mapper.save(m, callback=save_test_callback).send()

            self.assertEqual(variable['v'], updated)

        self.ioloop.run_until_complete(test())

    def test_can_call_callback_when_delete_method_is_called(self):
        variable = {'v': ''}
        updated = random()

        def delete_test_callback(entity):
            variable['v'] = updated

        async def test():
            m = self.mapper.create({'id': '15'}, TestVertex)
            await self.mapper.delete(m, callback=delete_test_callback).send()

            self.assertEqual(variable['v'], updated)

        self.ioloop.run_until_complete(test())

    def test_can_retrieve_data_from_entity_via_mapper(self):

        class TestCaseVertex1(Vertex):
            allow_undefined = True

        d = {
            'name': 'name{}'.format(str(random()))
        }

        async def test():
            v = self.mapper.create(d, TestCaseVertex1)
            data = await self.mapper.data(v)

            self.assertIn('name', data)
            self.assertEqual(d['name'], data['name'][0]['value'])

        self.ioloop.run_until_complete(test())

    def test_can_retrieve_data_from_collection_via_mapper(self):

        class TestCaseVertex1(Vertex):
            allow_undefined = True

        class C(object):
            data = []

        coll = []
        items = 15

        for i in range(items):
            d = {
                'name': 'name{}'.format(str(random()))
            }
            v = self.mapper.create(d, TestCaseVertex1)

            coll.append(dict(v.data))

        resp = Response()
        resp.result = {'data': coll}

        collection = Collection(self.mapper, resp)

        async def test():
            data = await self.mapper.data(collection)
            self.assertEqual(items, len(data))

            names = [dd['name'] for dd in data]

            for d in coll:
                self.assertIn(d['name'], names)

        self.ioloop.run_until_complete(test())

    def test_can_retrieve_data_from_two_nested_entities_via_custom_mapper_methods(self):
        city = 'city-{}'.format(str(random()))

        class TestCaseVertex2(Vertex):
            allow_undefined = True

        class TestCaseVertex2Mapper(EntityMapper):
            entity = TestCaseVertex2

            async def get_city(self, entity, data):
                data['city'] = city
                return data

        d = {
            'name': 'name{}'.format(str(random()))
        }
        v = self.mapper.create(d, TestCaseVertex2)

        async def test():
            data = await self.mapper.data(v, 'get_city')

            self.assertIn('name', data)
            self.assertEqual(d['name'], data['name'][0]['value'])
            self.assertIn('city', data)
            self.assertEqual(city, data['city'])

        self.ioloop.run_until_complete(test())

    def test_can_assure_saving_vertex_mulitple_times_only_crud_once(self):
        d = {'some_field': str(random())}
        v = self.mapper.create(d, TestVertex)

        self.mapper.save(v).save(v)._build_queries()
        params = copy.deepcopy(self.mapper.params)
        return_var = self.mapper.get_entity_variable(v)
        values = ['some_field', d['some_field'], 'vertex', entity_name(v),
            GIZMO_ENTITY, GIZMO_TYPE, str(v)]
        expected = build_vertex_create_query(entity=v, values=values,
            params=params, mapper=self.mapper, return_var=return_var)
        self.assertEqual(3, len(self.mapper.queries))
        self.assertIn(expected, self.mapper.queries)

    def test_can_assure_saving_edge_mulitple_times_only_crud_once(self):
        d = {
            'outV': 10,
            'inV': 99,
        }
        e = TestEdge(data=d)

        self.mapper.save(e).save(e)._build_queries()

        self.assertEqual(3, len(self.mapper.queries))

    def test_can_assure_saving_edge_and_vertex_mulitple_times_only_crud_once(self):
        v = TestVertex()
        d = {
            'outV': v,
            'inV': 99,
        }
        e = TestEdge(data=d)

        self.mapper.save(e).save(e)._build_queries()

        self.assertEqual(4, len(self.mapper.queries))


class TestCallbackVertex(Vertex):
    allow_undefined = True


test_callback_mapper_on_create_variable = None
test_callback_mapper_on_update_variable = None
test_callback_mapper_on_delete_variable = 'DEL'


class TestCallbackMapper(EntityMapper):
    entity = TestCallbackVertex
    on_create_variable = ''

    def on_create(self, entity):
        global test_callback_mapper_on_create_variable
        test_callback_mapper_on_create_variable = \
            entity['on_create_variable'].data[0]['value']

    def on_update(self, entity):
        global test_callback_mapper_on_update_variable
        test_callback_mapper_on_update_variable = \
            entity['on_update_variable'].values[0]

    def on_delete(self, entity):
        global test_callback_mapper_on_delete_variable
        x = entity['on_delete_variable'].values[0]
        test_callback_mapper_on_delete_variable= \
            entity['on_delete_variable'].values[0]


class CustomMapperTests(unittest.TestCase):

    def setUp(self):
        self.gremlin = Gremlin()
        self.request = TestRequest()
        self.mapper = Mapper(self.request, self.gremlin)
        self.ioloop = asyncio.get_event_loop()

    def test_can_can_on_create_level_callback(self):

        async def test():
            global test_callback_mapper_on_create_variable
            r = random()
            v = TestCallbackVertex({'on_create_variable': r})
            await self.mapper.save(v).send()
            self.assertEqual(r, test_callback_mapper_on_create_variable)

        self.ioloop.run_until_complete(test())

    def test_can_can_on_update_entity_level_callback(self):

        async def test():
            global test_callback_mapper_on_update_variable
            r = random()
            v = TestCallbackVertex({'id': 10, 'on_update_variable': r})
            mapper = self.mapper.get_mapper(v)
            await self.mapper.save(v).send()
            self.assertEqual(r, test_callback_mapper_on_update_variable)

        self.ioloop.run_until_complete(test())

    def test_can_can_on_delete_entity_level_callback(self):

        async def test():
            global test_callback_mapper_on_delete_variable

            r = random()
            v = TestCallbackVertex({'id': 10, 'on_delete_variable': r})
            mapper = self.mapper.get_mapper(v)
            await self.mapper.delete(v).send()
            self.assertEqual(r, test_callback_mapper_on_delete_variable)

        self.ioloop.run_until_complete(test())

    def test_can_can_on_create_level_callback_and_onetime_callback(self):

        async def test():
            global test_callback_mapper_on_create_variable

            variable = {'v': ''}
            updated = random()

            def create_test_callback(entity):
                variable['v'] = updated

            r = random()
            v = TestCallbackVertex({'on_create_variable': r})
            mapper = self.mapper.get_mapper(v)
            await self.mapper.save(v, callback=create_test_callback).send()
            self.assertEqual(r, test_callback_mapper_on_create_variable)
            self.assertEqual(variable['v'], updated)

        self.ioloop.run_until_complete(test())

    def test_can_can_on_update_entity_level_callback_and_onetime_callback(self):

        async def test():
            global test_callback_mapper_on_update_variable

            variable = {'v': ''}
            updated = random()

            def update_test_callback(entity):
                variable['v'] = updated

            r = random()
            v = TestCallbackVertex({'id': 10, 'on_update_variable': r})
            mapper = self.mapper.get_mapper(v)
            await self.mapper.save(v, callback=update_test_callback).send()
            self.assertEqual(r, test_callback_mapper_on_update_variable)
            self.assertEqual(variable['v'], updated)

        self.ioloop.run_until_complete(test())

    def test_can_can_on_delete_entity_level_callback_and_onetime_callback(self):

        async def test():
            global test_callback_mapper_on_delete_variable

            variable = {'v': ''}
            updated = random()

            def delete_test_callback(entity):
                variable['v'] = updated

            r = random()
            v = TestCallbackVertex({'id': 10, 'on_delete_variable': r})
            mapper = self.mapper.get_mapper(v)
            await self.mapper.delete(v, callback=delete_test_callback).send()
            self.assertEqual(r, test_callback_mapper_on_delete_variable)
            self.assertEqual(variable['v'], updated)

        self.ioloop.run_until_complete(test())


if __name__ == '__main__':
    unittest.main()