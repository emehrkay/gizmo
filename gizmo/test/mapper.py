#import pudb; pu.db
import unittest
from random import randrange, random
from time import sleep
from collections import OrderedDict
from gizmo.mapper import Mapper, _GenericMapper, Vertex, Edge
from gizmo.utils import (GIZMO_MODEL, GIZMO_CREATED, GIZMO_MODIFIED,
                         GIZMO_ID, GIZMO_LABEL)
from gremlinpy.gremlin import Gremlin
from gizmo.test.entity import (TestVertex, TestEdge, TestUniqueEdge,
                               TestUndefinedVertex)
import copy


TEST_CALLBACK_VERTEX = 'test_callback_vertex'


class TestUniqieMapper(_GenericMapper):
    model = TestUniqueEdge
    unique = True


def g_v(id):
    return 'g.V({}).next()'.format(id)


def get_dict_key(params, value, unset=False):
    for k, v in params.items():
        if v == value:
            if unset:
                del(params[k])
            return k, params

    return None, dict


def get_entity_entry(entity_queue, entity):
    for i, s in entity_queue.items():
        if s == entity:
            return {i: entity_queue[i]}

    return None


def build_vertex_create_query(entity, params=None, models=None):
    params = copy.deepcopy(params or {})
    _immutable = entity._immutable
    props = []
    entity.field_type = 'graph'

    for k, v in entity.data.items():
        if k not in _immutable:
            value, params = get_dict_key(params, v, True)
            prop = "'%s', %s" % (k, value)
            props.append(prop)

    if models:
        entry_v1 = get_entity_entry(models, entity)
        expected = "%s = g.addV(%s).next()" % \
            (list(entry_v1.keys())[0], ', '.join(props))
    else:
        expected = "g.addV(%s).next()" % \
            (', '.join(props))

    return expected


def build_vertex_update_query(entity, eid, params=None, models=None):
    params = copy.deepcopy(params or {})
    immutable = entity._immutable
    query_ps = []
    entity.field_type = 'graph'

    for k, v in entity.data.items():
        if k not in immutable:
            value, paramsss = get_dict_key(params, v)
            prop = "property('%s', %s)" % (k, value)
            query_ps.append(prop)

    propv, params = get_dict_key(params, eid)

    close = '.'.join(query_ps)

    if models:
        entry_v1 = get_entity_entry(models, entity)
        params = (list(entry_v1.keys())[0], propv, close)
        expected = "%s = g.V(%s).%s.next()" % params
    else:
        params = (propv, close)
        expected = "g.V(%s).%s.next()" % params

    return expected


def build_edge_create_query(entity, out_v, in_v, label, params, models):
    params = copy.deepcopy(params or {})
    entity.field_type = 'graph'
    immutable = entity._immutable
    out = g_v(out_v)
    inn = g_v(in_v)
    l, _ = get_dict_key(params, label)
    a = [l, inn]

    for k, v in entity.data.items():
        if k not in immutable:
            value, paramsss = get_dict_key(params, v or '')
            a.append("'{}'".format(k))
            a.append(value or '""')

    if models:
        entry_v1 = list(get_entity_entry(models, entity))[0]
        return "{} = {}.addEdge({})".format(entry_v1, out, ', '.join(a))
    else:
        return "{}.addEdge({})".format(out, ', '.join(a))

def build_edge_update_query(entity):
    pass


def build_return_query(entities, models):
    ret = []

    for e in entities:
        entry = list(get_entity_entry(models, e))[0]
        s = "'{}': {}".format(entry, entry)
        ret.append(s)

    return '[{}]'.format(', '.join(ret))


class TestRequest(object):

    def __init__(self, *args, **kwargs):
        pass

    def send(self, *args, **kwargs):
        pass


DEFAULT_INSERT_FIELDS = [
    GIZMO_MODEL,
    GIZMO_CREATED,
    GIZMO_MODIFIED,
]

DEFAULT_UPDATE_FIELDS = [GIZMO_ID] + DEFAULT_INSERT_FIELDS


class MapperTests(unittest.TestCase):

    def setUp(self):
        self.gremlin = Gremlin()
        self.request = TestRequest()
        self.mapper = Mapper(self.request, self.gremlin, logger=False)

    def test_mapper_instance(self):
        m = Mapper(self.request, self.gremlin, logger=False)

        self.assertTrue(type(m) == Mapper)

    def test_can_create_vertex(self):
        v = self.mapper.create_model(model_class=TestVertex)

        self.assertTrue(isinstance(v, Vertex))
        self.assertEqual(v._type, 'vertex')

    def test_can_create_vertex_with_data(self):
        d = {'some_field': random()}
        v = self.mapper.create_model(d, TestVertex)
        vd = v.data

        """
        the entity's data will have a _type member
        """
        self.assertTrue(isinstance(v, Vertex))

        for k, v in d.items():
            self.assertIn(k, vd)
            self.assertEqual(v, vd[k])

    def test_can_update_existing_vertex(self):
        vid = '1111'
        d = {
            GIZMO_ID: vid,
            'some_field': 'mark',
        }
        v = self.mapper.create_model(d, TestVertex)
        self.mapper.save(v)._build_queries()

        sent_params = copy.deepcopy(self.mapper.params)
        expected = build_vertex_update_query(v, vid, \
            sent_params, self.mapper.models)
        self.assertEqual(expected, self.mapper.queries[0])
        self.assertEqual(len(d) + len(DEFAULT_INSERT_FIELDS), len(sent_params))

    def test_can_queue_save_vertex_with_two_params_query(self):
        d = {
            'some_field': 'mark',
        }
        v = self.mapper.create_model(d, TestVertex)

        self.mapper.save(v)._build_queries()

        params = copy.deepcopy(self.mapper.params)
        sent_params = copy.deepcopy(self.mapper.params)
        expected = build_vertex_create_query(v, sent_params, \
            self.mapper.models)
        self.assertEqual(expected, self.mapper.queries[0])
        self.assertEqual(len(d) + len(DEFAULT_INSERT_FIELDS), len(sent_params))

    def test_can_delete_existing_vertex(self):
        vid = '1111'
        d = {
            GIZMO_ID: vid,
            'some_field': 'mark',
        }
        v = self.mapper.create_model(d, TestVertex)
        self.mapper.delete(v)._build_queries()
        params = copy.deepcopy(self.mapper.params)
        sent_params = copy.deepcopy(self.mapper.params)
        eyed = get_dict_key(params, vid)
        expected = 'g.V({}).next().remove()'.format(eyed[0])
        self.assertEqual(expected, self.mapper.queries[0])

    def test_can_delete_multiple_entities(self):
        v1 = {'_id': 15}
        v2 = {'_id': 10}
        out_v = self.mapper.create_model(v1, TestVertex)
        in_v = self.mapper.create_model(v2, TestVertex)
        ed = {'out_v': out_v, 'in_v': in_v, '_id': 44}
        edge = self.mapper.create_model(ed, TestEdge)

        self.mapper.delete(out_v)
        self.mapper.delete(in_v)
        self.mapper.delete(edge)
        self.mapper._build_queries()

        params = self.mapper.params
        v1_id = get_dict_key(params, v1['_id'])
        v2_id = get_dict_key(params, v2['_id'])
        e_id = get_dict_key(params, ed['_id'])
        expected = [
            'g.V({}).next().remove()'.format(v1_id[0]),
            'g.V({}).next().remove()'.format(v2_id[0]),
            'g.E({}).next().remove()'.format(e_id[0]),
        ]

        self.assertEqual(3, len(self.mapper.queries))
        self.assertEqual(3, len(self.mapper.params))

        for exp in expected:
            self.assertIn(exp, self.mapper.queries)

    def test_can_create_edge_with_existing_vertices(self):
        v1 = {'_id': 15}
        v2 = {'_id': 10}
        out_v = self.mapper.create_model(v1, TestVertex)
        in_v = self.mapper.create_model(v2, TestVertex)
        ed = {'out_v': out_v, 'in_v': in_v}
        edge = self.mapper.create_model(ed, TestEdge)

        self.assertTrue(isinstance(edge, Edge))
        self.assertTrue(isinstance(edge.out_v, TestVertex))
        self.assertTrue(isinstance(edge.in_v, TestVertex))

    def test_can_create_edge_with_existing_vertices_query(self):
        v1 = {'_id': 15}
        v2 = {'_id': 10, 'some_field': str(random())}
        out_v = self.mapper.create_model(v1, TestVertex)
        in_v = self.mapper.create_model(v2, TestVertex)
        ed = {'out_v': out_v, 'in_v': in_v}
        edge = self.mapper.create_model(ed, TestEdge)
        label = str(TestEdge())

        self.mapper.save(edge)._build_queries()
        params = copy.deepcopy(self.mapper.params)
        queries = self.mapper.queries
        out_v_query = build_vertex_update_query(out_v, v1['_id'], params, \
            self.mapper.models)
        in_v_query = build_vertex_update_query(in_v, v2['_id'], params, \
            self.mapper.models)
        out_entry = list(get_entity_entry(self.mapper.models, out_v).keys())[0]
        in_entry = list(get_entity_entry(self.mapper.models, in_v).keys())[0]
        edge_query = build_edge_create_query(edge, out_entry, in_entry, \
            label, params, self.mapper.models)
        return_query = build_return_query([out_v, in_v, edge], self.mapper.models)

        self.assertEqual(len(queries), 4)
        self.assertEqual(out_v_query, queries[0])
        self.assertEqual(in_v_query, queries[1])
        self.assertEqual(edge_query, queries[2])
        self.assertEqual(return_query, queries[3])

    def test_can_create_edge_with_one_existing_vertex_and_one_new_vertex(self):
        v1 = {'_id': 15}
        v2 = {}
        out_v = self.mapper.create_model(v1, TestVertex)
        self.mapper.save(out_v)
        in_v = self.mapper.create_model(v2, TestVertex)
        ed = {'out_v': out_v, 'in_v': in_v}
        edge = self.mapper.create_model(ed, TestEdge)
        label = str(TestEdge())

        self.mapper.save(edge)._build_queries()
        params = copy.deepcopy(self.mapper.params)
        queries = self.mapper.queries
        out_v_query = build_vertex_update_query(out_v, v1['_id'], params, \
            self.mapper.models)
        in_v_query = build_vertex_create_query(in_v, params, self.mapper.models)
        out_entry = list(get_entity_entry(self.mapper.models, out_v).keys())[0]
        in_entry = list(get_entity_entry(self.mapper.models, in_v).keys())[0]
        edge_query = build_edge_create_query(edge, out_entry, in_entry, \
            label, params, self.mapper.models)
        return_query = build_return_query([out_v, in_v, edge], self.mapper.models)

        self.assertEqual(len(queries), 4)
        self.assertEqual(out_v_query, queries[0])
        self.assertEqual(in_v_query, queries[1])
        self.assertEqual(edge_query, queries[2])
        self.assertEqual(return_query, queries[3])

    def test_can_queue_save_edge_with_existing_vertices(self):
        v1 = {'_id': 15}
        v2 = {'_id': 10}
        out_v = self.mapper.create_model(v1, TestVertex)
        in_v = self.mapper.create_model(v2, TestVertex)
        ed = {'out_v': out_v, 'in_v': in_v}
        edge = self.mapper.create_model(ed, TestEdge)
        label = str(TestEdge())

        self.mapper.save(edge)._build_queries()
        params = copy.deepcopy(self.mapper.params)
        queries = self.mapper.queries
        out_v_query = build_vertex_update_query(out_v, v1['_id'], params, \
            self.mapper.models)
        in_v_query = build_vertex_update_query(in_v, v2['_id'], params, \
            self.mapper.models)
        out_entry = list(get_entity_entry(self.mapper.models, out_v).keys())[0]
        in_entry = list(get_entity_entry(self.mapper.models, in_v).keys())[0]
        edge_query = build_edge_create_query(edge, out_entry, in_entry, \
            label, params, self.mapper.models)
        return_query = build_return_query([out_v, in_v, edge], self.mapper.models)

        self.assertEqual(len(queries), 4)
        self.assertEqual(out_v_query, queries[0])
        self.assertEqual(in_v_query, queries[1])
        self.assertEqual(edge_query, queries[2])
        self.assertEqual(return_query, queries[3])

    def test_can_call_callback_when_save_method_is_called(self):
        variable = {'v': ''}
        updated = random()

        def save_test_callback(model):
            variable['v'] = updated

        m = self.mapper.create_model({}, TestVertex)
        yield self.mapper.save(m, callback=save_test_callback).send()

        self.assertEqual(variable['v'], updated)

    def test_can_call_callback_when_delete_method_is_called(self):
        variable = {'v': ''}
        updated = random()

        def delete_test_callback(model):
            variable['v'] = updated

        m = self.mapper.create_model({'_id': 15}, TestVertex)
        yield self.mapper.delete(m, callback=delete_test_callback).send()

        self.assertEqual(variable['v'], updated)

    def test_can_retrieve_data_from_entity_via_mapper(self):
        self.assertTrue(False)

    def test_can_retrieve_data_from_two_nested_entities_via_custom_mapper_methods(self):
        self.assertTrue(False)


class QueryTests(unittest.TestCase):

    def setUp(self):
        self.gremlin = Gremlin()
        self.request = TestRequest()
        self.mapper = Mapper(self.request, self.gremlin, logger=False)


class TestCallbackVertex(Vertex):
    _allowed_undefined = True


class TestCallbackMapper(_GenericMapper):
    model = TestCallbackVertex
    on_create_variable = ''

    def on_create(self, model):
        TestCallbackMapper.on_create_variable = model['on_create_variable']

    def on_update(self, model):
        TestCallbackMapper.on_update_variable = model['on_update_variable']

    def on_delete(self, model):
        TestCallbackMapper.on_delete_variable = model['on_delete_variable']


class CustomMapperTests(unittest.TestCase):

    def setUp(self):
        self.gremlin = Gremlin()
        self.request = TestRequest()
        self.mapper = Mapper(self.request, self.gremlin, logger=False)

    def test_can_can_on_create_model_level_callback(self):
        r = random()
        v = TestCallbackVertex({'on_create_variable': r})
        yield self.mapper.save(v).send()
        self.assertEqual(r, TestCallbackMapper.on_create_variable)

    def test_can_can_on_update_model_level_callback(self):
        r = random()
        v = TestCallbackVertex({'_id': 10, 'on_update_variable': r})
        mapper = self.mapper.get_mapper(v)
        yield self.mapper.save(v).send()
        self.assertEqual(r, mapper.on_update_variable)

    def test_can_can_on_delete_model_level_callback(self):
        r = random()
        v = TestCallbackVertex({'_id': 10, 'on_delete_variable': r})
        mapper = self.mapper.get_mapper(v)
        yield self.mapper.delete(v).send()
        self.assertEqual(r, mapper.on_delete_variable)

    def test_can_can_on_create_model_level_callback_and_onetime_callback(self):
        variable = {'v': ''}
        updated = random()

        def create_test_callback(model):
            variable['v'] = updated

        r = random()
        v = TestCallbackVertex({'on_create_variable': r})
        mapper = self.mapper.get_mapper(v)
        yield self.mapper.save(v, callback=create_test_callback).send()
        self.assertEqual(r, mapper.on_create_variable)
        self.assertEqual(variable['v'], updated)

    def test_can_can_on_update_model_level_callback_and_onetime_callback(self):
        variable = {'v': ''}
        updated = random()

        def update_test_callback(model):
            variable['v'] = updated

        r = random()
        v = TestCallbackVertex({'_id': 10, 'on_update_variable': r})
        mapper = self.mapper.get_mapper(v)
        yield self.mapper.save(v, callback=update_test_callback).send()
        self.assertEqual(r, mapper.on_update_variable)
        self.assertEqual(variable['v'], updated)

    def test_can_can_on_delete_model_level_callback_and_onetime_callback(self):
        variable = {'v': ''}
        updated = random()

        def delete_test_callback(model):
            variable['v'] = updated

        r = random()
        v = TestCallbackVertex({'_id': 10, 'on_delete_variable': r})
        mapper = self.mapper.get_mapper(v)
        yield self.mapper.delete(v, callback=delete_test_callback).send()
        self.assertEqual(r, mapper.on_delete_variable)
        self.assertEqual(variable['v'], updated)


class EventSourceTests(unittest.TestCase):

    def setUp(self):
        self.gremlin = Gremlin()
        self.request = TestRequest()
        self.mapper = Mapper(self.request, self.gremlin, logger=False)

    def test_can_source_an_event_when_model_is_saved(self):
        pass


if __name__ == '__main__':
    unittest.main()
