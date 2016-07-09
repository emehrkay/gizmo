import json
import copy
from collections import OrderedDict

from six import with_metaclass

from tornado import gen

from gremlinpy.gremlin import Gremlin, Function
from gremlinpy.statement import GetEdge, Conditional

from .field import Timestamp
from .utils import get_qualified_name, get_qualified_instance_name, GIZMO_LABEL
from .utils import camel_to_underscore, GIZMO_VARIABLE
from .utils import IMMUTABLE, GIZMO_MODEL
from .entity import Edge, Vertex, GenericVertex, GenericEdge, _MAP, _BaseEntity
from .error import *


# Holds the entity->mapper mappings for custom mappers
_MAPPER_MAP = {}
_ENTITY_USED = {}
GENERIC_MAPPER = 'generic.mapper'
count = 0
query_count = 0


def get_entity_count(entity):
    name = str(entity)

    if name not in _ENTITY_USED:
        _ENTITY_USED[name] = -1

    _ENTITY_USED[name] += 1

    return _ENTITY_USED[name]


class _RootMapper(type):
    """
    In the case of custom mappers, this metaclass will register the entity name
    with the mapper object. This is done so that when entities are loaded by
    name its mappers is used to CRUD it.

    This only works when the mapper_instance.create() method is used to
    create the entity
    """

    def __new__(cls, name, bases, attrs):
        cls = super(_RootMapper, cls).__new__(cls, name, bases, attrs)
        entity = attrs.pop('entity', None)

        if entity:
            map_name = '%s.%s' % (entity.__module__, entity.__name__)
            _MAPPER_MAP[map_name] = cls
        elif name == '_GenericMapper':
            _MAPPER_MAP[GENERIC_MAPPER] = cls

        return cls


class _GenericMapper(with_metaclass(_RootMapper, object)):
    VARIABLE = GIZMO_VARIABLE
    unique = False
    unique_fields = None
    save_statements = None

    def __init__(self, mapper=None):
        self.mapper = mapper
        self.gremlin = mapper.gremlin

        self.reset()

    def reset(self):
        self.queries = []
        self.return_vars = []
        self.entities = {}
        self.params = {}
        self.callbacks = {}

    def enqueue(self, query, bind_return=True):
        for entry in query.queries:
            global count
            count += 1
            script = entry['script']

            if script in self.queries:
                continue

            if bind_return:
                variable = '%s_%s' % (self.VARIABLE, count)
                script = '%s = %s' % (variable, script)

                if 'entity' in entry:
                    self.entities[variable] = entry['entity']
                    self.return_vars.append(variable)

            self.queries.append(script)
            self.params.update(entry['params'])

        return self

    def _enqueue_callback(self, entity, callback):
        if callback:
            listed = self.callbacks.get(entity, [])

            if isinstance(callback, (list, tuple)):
                listed += list(callback)
            elif callback:
                listed.append(callback)

            self.callbacks[entity] = listed

        return self

    def on_create(self, entity):
        pass

    def on_update(self, entity):
        pass

    def on_delete(self, entity):
        pass

    def by_id(self, _id, entity, bind_return=True):
        query = Query(self.gremlin, self.mapper)

        if isinstance(entity, type):
            entity = entity()

        query.by_id(_id, entity)
        return self.enqueue(query, bind_return)

    def start(self, entity=None):
        return Traversal(self, entity or self.entity)

    def get_or_create(self, entity, field_val, bind_return=False,
                      statement=None):
        """method used to create a simple query that will get an entity
        by matching the field_val pairs or create it based on those pairs.
        If a gizmo.Statement is passed as an argument, it will be applied
        to the get portion of the query.

        expected code:

            g.V().has('field', value).tryNext().orElseGet{
                g.addV('field', value).next()
            }
        """
        params = {}
        queries = []

        if isinstance(entity, type):
            entity = entity()

        # build the create query
        create_query = Query(self.gremlin, self.mapper)

        entity.hydrate(field_val)
        create_query.save(entity)

        # build the get query
        rep = 'E' if entity._type == 'edge' else 'V'
        gremlin = self.gremlin

        gremlin.func(rep)

        for field, val in field_val.items():
            b_field = create_query._entity_variable(entity, field)
            param = gremlin.bind_param(val, b_field)

            gremlin.has('"{}"'.format(field), param[0])

        if statement:
            gremlin.apply_statement(statement)

        for entry in create_query.queries:
            queries.append(entry['script'])
            self.params.update(entry['params'])

        gremlin.tryNext().orElseGet.close(''.join(queries))

        script = str(gremlin)
        self.queries.append(script)
        self.params.update(gremlin.bound_params)

        return self

    @gen.coroutine
    def data(self, entity):
        return entity.data

    def before_save_action(self, entity):
        """method used to run any actions against the entity before it is
        saved"""

        # update any Timestamp fields with right now
        for name, field in entity.fields.items():
            if isinstance(field, Timestamp):
                field.field_value = field.initial_value

        return entity

    def _build_save_statements(self, entity, query, **kwargs):
        statement_query = Query(Gremlin(self.gremlin.gv), self.mapper)
        query_gremlin = Gremlin(self.mapper.gremlin.gv)

        for entry in query.queries:
            query_gremlin.bind_params(entry['params'])

        for statement in self.save_statements:
            instance = statement(entity, self, query, **kwargs)

            query_gremlin.apply_statement(instance)

        statement_query.add_query(str(query_gremlin),
                                  query_gremlin.bound_params, entity=entity)

        return statement_query

    def save(self, entity, bind_return=True, callback=None, *args, **kwargs):
        """callback and be a single callback or a list of them"""
        method = '_save_edge' if entity._type == 'edge' else '_save_vertex'

        if not isinstance(callback, (list, tuple)) and callback:
            callback = [callback]
        else:
            callback = []

        if entity['_id']:
            callback.insert(0, self.on_update)
        else:
            callback.insert(0, self.on_create)

        self._enqueue_callback(entity, callback)
        self.before_save_action(entity)

        return getattr(self, method)(entity=entity, bind_return=bind_return)

    def _save_vertex(self, entity, bind_return=True):
        """
        method used to save a entity. IF both the unique_type and unique_fields
        params are set, it will run a sub query to check to see if an entity
        exists that matches those values
        """
        query = Query(self.gremlin, self.mapper)
        ref = self.mapper.get_entity_variable(entity)

        """
        check to see if the entity has been used already in the current script
        execution.
        If it has use the reference
        if it hasnt, go through the process of saving it
        """
        if ref:
            query.add_query(ref, params=None, entity=entity)

            return self.enqueue(query, bind_return)

        query.save(entity)

        if not entity['_id'] and self.unique_fields:
            from .statement import MapperUniqueVertex

            if not self.save_statements:
                self.save_statements = []

            if MapperUniqueVertex not in self.save_statements:
                self.save_statements.append(MapperUniqueVertex)

        if self.save_statements and len(self.save_statements):
            statement_query = self._build_save_statements(entity, query)

            return self.enqueue(statement_query, bind_return)
        else:
            return self.enqueue(query, bind_return)

    def _save_edge(self, entity, bind_return=True):
        query = Query(self.gremlin, self.mapper)
        save = True
        # TODO: send an edge to be saved multiple times
        edge_ref = self.mapper.get_entity_variable(entity)
        out_v = entity.out_v
        out_v_id = out_v['_id'] if isinstance(out_v, Vertex) else None
        in_v = entity.in_v
        in_v_id = in_v['_id'] if isinstance(in_v, Vertex) else None
        out_v_ref = self.mapper.get_entity_variable(out_v)
        in_v_ref = self.mapper.get_entity_variable(in_v)

        if edge_ref:
            query.add_query(edge_ref, params=None, entity=entity)

            return self.enqueue(query, bind_return)

        """
        both out_v and in_v are checked to see if the entities stored in each
        respective variable has been used.
        If they have not and they are Vertex instances with an empty _id,
            send them to be saved.
        if they have been used, use the reference variable in the create edge
        logic
        """
        if not out_v_ref and isinstance(out_v, Vertex):
            self.mapper.save(out_v)
            out_v = self.mapper.get_entity_variable(out_v)
        else:
            out_v = out_v_ref

        if not in_v_ref and isinstance(in_v, Vertex):
            self.mapper.save(in_v)
            in_v = self.mapper.get_entity_variable(in_v)
        else:
            in_v = in_v_ref

        out_v = out_v['_id'] if isinstance(out_v, Vertex) else out_v
        in_v = in_v['_id'] if isinstance(in_v, Vertex) else in_v

        query.save(entity)

        if not entity['_id'] and self.unique and in_v_id and out_v_id:
            from .statement import MapperUniqueEdge

            if not self.save_statements:
                self.save_statements = []

            if MapperUniqueEdge not in self.save_statements:
                self.save_statements.append(MapperUniqueEdge)

        if self.save_statements and len(self.save_statements):
            statement_query = self._build_save_statements(entity, query,
                out_v_id=out_v_id, in_v_id=in_v_id,
                label=entity[GIZMO_LABEL], direction=self.unique)

            return self.enqueue(statement_query, False)
        else:
            return self.enqueue(query, bind_return)

    def delete(self, entity, lookup=True, callback=None):
        query = Query(self.gremlin, self.mapper)

        if not isinstance(callback, (list, tuple)) and callback:
            callback = [callback]
        else:
            callback = []

        query.delete(entity)
        callback.insert(0, self.on_delete)
        self._enqueue_callback(entity, callback)

        return self.enqueue(query, False)

    def create(self, data=None, entity=None, data_type='python'):
        """
        Method used to create a new entity based on the data that is passed in.
        If the kwarg entity is passed in, it will be used to create the
        entity else if utils.GIZMO_MODEL is in data, that will be used
        finally, entity.GenericVertex or entity.GenericEdge will be used to
        construct the entity
        """
        check = True

        if data is None:
            data = {}

        if entity is not None:
            try:
                entity = entity(data, data_type=data_type)
                check = False
            except Exception as e:
                pass

        if check:
            try:
                if GIZMO_MODEL in data:
                    name = data[GIZMO_MODEL]
                    entity = _MAP[name](data, data_type=data_type)
                else:
                    raise
            except Exception as e:
                # all else fails create a GenericVertex unless _type is 'edge'
                if data.get('_type', None) == 'edge':
                    entity = GenericEdge(data, data_type=data_type)
                else:
                    entity = GenericVertex(data, data_type=data_type)

        if '_id' in data:
            entity.fields['_id'].value = data['_id']

        return entity


class Mapper(object):
    VARIABLE = 'gizmo_var'

    def __init__(self, request, gremlin=None, auto_commit=True, logger=None,
                 graph_instance_name=None):
        if gremlin is None:
            gremlin = Gremlin()

        self.request = request
        self.gremlin = gremlin
        self.auto_commit = auto_commit
        self.graph_instance_name = graph_instance_name

        if not self.auto_commit and not self.graph_instance_name:
            error = ('If auto_commit is set, we need to know the graph'
                     'instance name')
            raise ArgumentError(error)

        if not logger and logger is not False:
            import logging
            logging.basicConfig(format='%(levelname)s:%(message)s')
            logger = logging.getLogger()
            logger.setLevel(logging.DEBUG)

        self.logger = logger
        self.reset()

    def __getattr__(self, magic_method):
        """magic method that works in conjunction with __call__ method
        these two methods are used to shortcut the retrieval of an
        entity's mapper and call a specific method against

        this chain:
        user = User()
        user_mapper = mapper.get_mapper(user)
        emails = user_mapper.get_emails(user)

        can be shortened into:
        user = User()
        emails = mapper.get_emails(user)
        """
        self._magic_method = magic_method

        return self

    def __call__(self, *args, **kwargs):
        mapper = self.get_mapper(args[0])

        return getattr(mapper, self._magic_method)(*args, **kwargs)

    @gen.coroutine
    def data(self, entity, *args):
        """utility method used to retrieve an entity's data. It also allows for
        method chaining in order to augment the resulting data.

        class MyMapper(_GenericMapper):
            @gen.coroutine
            def add_two(self, entity, data):
                data['two'] = 2
                return data

            @gen.coroutine
            def add_three(self, entity, data):
                data['three'] = 3
                return data

        entity = User()
        data = yield mapper.data(user, 'add_two', 'add_three')

        the resulting data will have the data from the User class, plus a two
        and a three member
        """
        collection = isinstance(entity, Collection)

        @gen.coroutine
        def get_data(entity, data):
            retrieved = data

            for method in args:
                mapper = self.get_mapper(entity)

                @gen.coroutine
                def wrapper(entity, data):
                    res = yield getattr(mapper, method)(entity=entity,
                                                        data=data)

                    return res

                retrieved = yield wrapper(entity=entity, data=retrieved)

            return retrieved

        if collection:
            data = []

            for coll_entity in entity:
                mapper = self.get_mapper(coll_entity)
                entity_data = yield mapper.data(coll_entity)
                res = yield get_data(coll_entity, entity_data)

                data.append(res)
        else:
            mapper = self.get_mapper(entity)
            entity_data = yield mapper.data(entity)
            data = yield get_data(entity, entity_data)

        return data

    def reset(self):
        self.gremlin.reset()
        global query_count
        global count
        global _ENTITY_USED

        query_count = 0
        count = 0
        _ENTITY_USED = {}
        self.queries = []
        self.return_vars = []
        self.entities = OrderedDict()  # ensure FIFO for testing
        self.del_entities = {}
        self.params = {}
        self.callbacks = {}
        self._magic_method = None

    def get_entity_variable(self, entity):

        def get_key():
            ret = None
            for key, def_entity in self.entities.items():
                if entity == def_entity:
                    return key

            return ret

        ret_key = get_key()

        return ret_key

    def get_param_key(self, value):
        key = None

        for k, v in self.params.items():
            if value == v:
                key = k
                break

        return key

    def get_mapper(self, entity=None, name=GENERIC_MAPPER):
        if entity is not None:
            if isinstance(entity, _BaseEntity):
                name = get_qualified_instance_name(entity)
            else:
                name = get_qualified_name(entity)

        if name not in _MAPPER_MAP:
            name = GENERIC_MAPPER

        return _MAPPER_MAP[name](self)

    def _enqueue_mapper(self, mapper):
        self.queries += mapper.queries
        self.return_vars += mapper.return_vars
        self.entities.update(mapper.entities)
        self.params.update(mapper.params)

        for entity, callbacks in mapper.callbacks.items():
            exisiting = self.callbacks.get(entity, [])

            self.callbacks[entity] = exisiting + callbacks

        mapper.reset()

        return self

    def by_id(self, _id, entity, bind_return=True):
        mapper = self.get_mapper(entity)

        mapper.by_id(_id=_id, entity=entity, bind_return=bind_return)

        return self._enqueue_mapper(mapper)

    @gen.coroutine
    def get_by_id(self, _id, entity='V'):
        self.gremlin.func(entity, _id)

        res = yield self.query(gremlin=self.gremlin)

        return res.first()

    def get_or_create(self, entity, field_val, bind_return=False,
                      statement=None):
        mapper = self.get_mapper(entity)

        mapper.get_or_create(entity=entity, field_val=field_val,
                             statement=statement)

        return self._enqueue_mapper(mapper)

    @gen.coroutine
    def get_or_create(self, entity, field_val, bind_return=False,
                             statement=None):
        self.get_or_create(entity=entity, field_val=field_val,
                           bind_return=bind_return, statement=statement)

        res = yield self.send()

        return res.first()

    def save(self, entity, bind_return=True, mapper=None,
             callback=None, **kwargs):
        if mapper is None:
            mapper = self.get_mapper(entity)

        mapper.save(entity, bind_return, callback, **kwargs)

        return self._enqueue_mapper(mapper)

    def delete(self, entity, mapper=None, callback=None):
        if mapper is None:
            mapper = self.get_mapper(entity)

        mapper.delete(entity, callback=callback)

        # manually add the deleted entity to the self.entities
        # collection for callbacks
        from random import randrange
        key = 'DELETED_%s_entity' % str(randrange(0, 999999999))
        self.del_entities[key] = entity

        return self._enqueue_mapper(mapper)

    def connect(self, out_v, in_v, label=None, data=None, edge_entity=None,
                data_type='python'):
        """
        method used to connect two vertices and create an Edge object
        the resulting edge is not saved to to graph until it is passed to
        save allowing further augmentation
        """
        if not isinstance(out_v, Vertex):
            if not isinstance(out_v, str):
                err = ['The out_v needs to be eiter a Vertex or string id']
                raise ModelException(err)

        if not isinstance(in_v, Vertex):
            if not isinstance(in_v, str):
                err = 'The in_v needs to be eiter a Vertex or string id'
                raise ModelException([err])

        if data is None:
            data = {}

        data['out_v'] = out_v
        data['in_v'] = in_v
        data['_type'] = 'edge'
        data[GIZMO_LABEL] = label

        return self.create(data=data, entity=edge_entity, data_type=data_type)

    def create(self, data=None, entity=None, data_type='python'):
        if data is None:
            data = {}

        if entity:
            mapper = self.get_mapper(entity)
        else:
            name = data.get(GIZMO_MODEL, GENERIC_MAPPER)
            mapper = self.get_mapper(name=name)

        kwargs = {
            'data': data,
            'entity': entity,
            'data_type': data_type,
        }

        return mapper.create(**kwargs)

    def _build_queries(self):
        if not self.auto_commit:
            commit = '.'.join([self.graph_instance_name, 'tx()', 'commit()'])

            self.queries.append(commit)

        if len(self.return_vars) > 0:
            returns = []

            for k in self.return_vars:
                returns.append("'%s': %s" % (k, k))

            ret = '[%s]' % ', '.join(returns)

            self.queries.append(ret)

        return self

    def start(self, entity):
        mapper = self.get_mapper(entity)

        return mapper.start(entity)

    def apply_statement(self, statement):
        self.gremlin.apply_statement(statement)

        return self

    @gen.coroutine
    def send(self):
        self._build_queries()

        script = ";\n".join(self.queries)
        params = self.params
        entities = self.entities
        callbacks = self.callbacks
        entities.update(self.del_entities)
        self.reset()

        res = yield self.query(script=script, params=params,
                          update_entities=entities, callbacks=callbacks)

        return res

    @gen.coroutine
    def query(self, script=None, params=None, gremlin=None,
              update_entities=None, callbacks=None):

        if gremlin is not None:
            script = str(gremlin)
            params = gremlin.bound_params

            gremlin.reset()

        if script is None:
            script = ''

        if params is None:
            params = {}

        if update_entities is None:
            update_entities = {}

        from .utils import _query_debug

        # TODO: remove this and implement proper logging
        if self.logger:
            from .utils import _query_debug

            self.logger.debug(script)
            self.logger.debug(json.dumps(params))
            self.logger.debug(_query_debug(script, params))

        response = yield self.request.send(script, params, update_entities)

        for k, entity in update_entities.items():
            cbs = callbacks.get(entity, [])
            for c in cbs:
                c(entity)

        return Collection(self, response)


class Query(object):
    QUERY_VAR = 'query_var'

    def __init__(self, gremlin=None, mapper=None):
        if gremlin is None:
            gremlin = Gremlin()

        self.gremlin = gremlin
        self.mapper = mapper
        self.fields = []
        self.queries = []
        self.entity_count = {}

    def _register_entity(self, entity):
        self.entity_count[entity] = get_entity_count(entity)

    def _entity_variable(self, entity, field):
        name = camel_to_underscore(entity.__class__.__name__)
        count = self.entity_count[entity]

        return '%s__%s__%s' % (name, field, count)

    def reset(self):
        self.fields = []

        self.gremlin.reset()

        return self

    def next_var(self, prefix=None):
        global query_count
        query_count += 1
        prefix = prefix or ''

        return '%s%s_%s' % (prefix, self.QUERY_VAR, query_count)

    def add_query(self, script, params=None, entity=None):
        if params is None:
            params = {}

        self.queries.append({
            'script': script,
            'params': params,
            'entity': entity,
        })

        return self

    def add_gremlin_query(self, entity=None):
        script = str(self.gremlin)
        params = self.gremlin.bound_params

        self.add_query(script, params, entity)

        return self.reset()

    def build_fields(self, entity, _immutable):
        gremlin = self.gremlin
        data = entity.fields.data

        for key, val in data.items():
            if key not in _immutable:
                value = val

                if type(val) is dict or type(val) is list:
                    listed = self.iterable_to_graph(val, '', entity)
                    value = "[%s]" % listed
                    entry = "'%s', %s" % (key, value)

                    self.fields.append(entry)
                else:
                    variable = self._entity_variable(entity, key)
                    bound = self.bind_param(value, variable)

                    self.fields.append("'%s', %s" % (key, bound[0]))

        return self

    def bind_param(self, value, key=None):
        key = self.mapper.get_param_key(value) or key

        return self.gremlin.bind_param(value, key)

    def iterable_to_graph(self, iterable, field, entity):
        if isinstance(iterable, dict):
            return self._dict_to_graph(iterable, field, entity)
        else:
            return self._list_to_graph(iterable, field, entity)

    def _dict_to_graph(self, iterable, field, entity):
        gremlin = self.gremlin
        gval = []

        for key, value in iterable.items():
            if type(value) is dict or type(value) is list:
                gval.append(self.iterable_to_graph(value, field + key))
            else:
                variable = self._entity_variable(entity, key)
                bound = self.bind_param(value, variable)

                gval.append("'%s': %s" % (key, bound[0]))

        if not len(iterable):
            gval.append(':')

        return ','.join(gval)

    def _list_to_graph(self, iterable, field, entity):
        gremlin = self.gremlin
        gval = []

        for key, value in enumerate(iterable):
            if type(value) is dict or type(value) is list:
                gval.append(self.iterable_to_graph(value, field, entity))
            else:
                variable = self._entity_variable(entity, field)
                bound = self.bind_param(value, variable)

                gval.append(variable)

        return ','.join(gval)

    def by_id(self, _id, entity, set_variable=None):
        gremlin = self.gremlin
        entity = 'E' if entity._type == 'edge' else 'V'

        getattr(gremlin, entity)(_id).next()

        if set_variable:
            gremlin.set_ret_variable(set_variable)

        return self.add_gremlin_query(entity)

    def add_vertex(self, entity, set_variable=False):
        self._register_entity(entity)

        if entity._type is None:
            err = 'Models need to have a type defined in order to save'
            raise QueryException([err])

        entity.field_type = 'graph'
        gremlin = self.gremlin

        if set_variable:
            gremlin.set_ret_variable(set_variable)

        # use the entity.fields.data instead of entity.data because
        # entity.data can be monkey-patched with custom mappers
        self.build_fields(entity, IMMUTABLE['vertex'])

        script = '%s.addV(%s).next()' % (gremlin.gv, ', '.join(self.fields))

        gremlin.set_graph_variable('').raw(script)

        entity.field_type = 'python'

        return self.add_gremlin_query(entity)

    def add_edge(self, entity, set_variable=False):
        self._register_entity(entity)

        if entity[GIZMO_LABEL] is None:
            raise QueryException(['The edge must have a label before saving'])

        entity.field_type = 'graph'
        g = Gremlin(self.gremlin.gv)
        gremlin = self.gremlin
        out_v, in_v = self._get_or_create_edge_vertices(entity)
        label_var = self.next_var('EDGE_LABEL')
        label_bound = gremlin.bind_param(entity[GIZMO_LABEL], label_var)
        edge_fields = ''

        if set_variable:
            gremlin.set_ret_variable(set_variable)

        self.build_fields(entity, IMMUTABLE['edge'])

        g.unbound('V', in_v).next()
        gremlin.unbound('V', out_v).next()
        gremlin.unbound('addEdge', label_bound[0], str(g),
                        ', '.join(self.fields))

        entity.field_type = 'python'

        return self.add_gremlin_query(entity)

    def _get_or_create_edge_vertices(self, edge):
        out_v = edge.out_v
        in_v = edge.in_v

        if out_v is None or in_v is None:
            error = 'Both out and in vertices must be set before saving \
                the edge'

            raise QueryException([error])

        self._register_entity(out_v)
        self._register_entity(in_v)

        out_v_mod = self.mapper.get_entity_variable(out_v)
        in_v_mod = self.mapper.get_entity_variable(in_v)

        if out_v_mod is None:
            self.mapper.save(out_v)
            out_v_mod = self.mapper.get_entity_variable(out_v)

        if in_v_mod is None:
            self.mapper.save(in_v)
            in_v_mod = self.mapper.get_entity_variable(in_v)

        return out_v_mod, in_v_mod

    def update(self, entity, set_variable=None):
        self._register_entity(entity)

        if entity._type is None:
            err = 'The entity must have a type defined in order to update'
            raise QueryException([err])

        if entity['_id'] is None:
            err = 'The entity must have an _id defined in order to update'
            raise QueryException([err])

        if not len(entity.changed):
            return self.by_id(entity['_id'], entity, set_variable)

        gremlin = self.gremlin
        entity.field_type = 'graph'
        entity_type = 'E' if entity._type == 'edge' else 'V'
        ent_var = 'EDGE_ID' if entity_type == 'E' else 'VERTEX_ID'
        ent_var = self.next_var(ent_var)

        if set_variable:
            gremlin.set_ret_variable(set_variable)

        eye_d = self.bind_param(entity['_id'], ent_var)
        getattr(gremlin, entity_type)(eye_d[0])

        # only update the fields that have changed.
        # @TODO: Make sure to document this behavior
        for k, v in entity.changed.items():
            name = '%s_%s' % (entity.__class__.__name__, k)

            if k not in entity._immutable:
                if type(v) is dict or type(v) is list:
                    field = entity.__class__.__name__
                    gmap = self.iterable_to_graph(v, field, entity)

                    gremlin.unbound('property', "'%s', [%s]" % (k, gmap))
                else:
                    variable = self._entity_variable(entity, k)
                    bound = self.bind_param(v, variable)
                    entry = "it.setProperty('%s', %s)" % (k, bound[0])
                    gremlin.property("'%s'" % k, bound[0])

        gremlin.next()
        entity.field_type = 'python'

        return self.add_gremlin_query(entity)

    def save(self, entity, set_variable=None):
        entity.field_type = 'python'

        if entity._type is None:
            raise EntityException(['The entity does not have a _type defined'])

        if not entity['_id']:
            if entity._type == 'vertex':
                self.add_vertex(entity, set_variable)
            else:
                self.add_edge(entity, set_variable)
        else:
            self.update(entity, set_variable)

        return self

    def delete(self, entity):
        gremlin = self.gremlin

        _id = entity['_id']

        if _id is None:
            err = 'Models must have an _id before they are deleted'
            raise EntityException([err])

        if entity._type is None:
            raise EntityException(['Models need to have a type defined'])

        self._register_entity(entity)

        variable = self._entity_variable(entity, 'id')
        bound = self.bind_param(_id, variable)

        self.fields.append("'%s', %s" % ('id', bound[0]))

        entity = 'E' if entity._type == 'edge' else 'V'
        getattr(gremlin, entity)(bound[0]).next().func('remove')

        return self.add_gremlin_query(entity)


class Traversal(Gremlin):
    """
    class used to start a traversal query based on a given entity
    when the class is created, the entity's _id and type are are
    set on the Gremlin object

    example:

    """

    def __init__(self, mapper, entity):
        graph_variable = mapper.gremlin.gv

        super(Traversal, self).__init__(graph_variable)

        self._mapper = mapper
        self._entity = entity
        ev, _id = entity.get_rep()

        if _id:
            bound_id = self.bind_param(_id, 'EYE_DEE')

            getattr(self, ev)(bound_id[0])
        else:
            _type = get_entity_name(entity)
            bound_type = self.bind_param(_type, 'BOUND_TYPE')
            e_v = 'V' if ev == 'V' else 'E'

            getattr(self, e_v)().hasLabel(bound_type[0])

    def define_traversal(self, traversal):
        if hasattr(traversal, '__call__'):
            self.traversal = traversal

        return self

    def start_bredth(self):
        pass

    def start_depth(self):
        pass

    @gen.coroutine
    def to_collection(self):
        collection = yield self._mapper.send(gremlin=self)

        return collection


class Collection(object):

    def __init__(self, mapper, response):
        self.mapper = mapper
        self.response = response
        self._entities = {}
        self._index = 0
        self._data_type = 'python'

    def first(self):
        return self[0]

    def last(self):
        return self[-1]

    def get_data(self):
        return [x for x in self.response.data]

    data = property(get_data)

    @property
    def entity_data(self):
        """
        this will get the instance data instead of the
        raw data. This will use the mapper to create each
        entity. Which may have a custom data attribute
        """
        return [x.data for x in self]

    def __len__(self):
        return len(self.response.data)

    def __getitem__(self, key):
        entity = self._entities.get(key, None)

        if entity is None:
            try:
                data = self.response[key]

                if data is not None:
                    entity = self.mapper.create(data=data,
                                                data_type=self._data_type)
                    entity.dirty = False
                    self._entities[key] = entity
                else:
                    raise
            except Exception as e:
                raise StopIteration()

        return entity

    def __setitem__(self, key, value):
        self._entities[key] = value

    def __delitem__(self, key):
        if key in self._entities:
            del self._entities[key]
