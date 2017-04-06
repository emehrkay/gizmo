import re

from collections import OrderedDict

from gremlinpy.gremlin import Gremlin, Param, AS

from .entity import (_Entity, Vertex, Edge, GenericVertex, GenericEdge,
    ENTITY_MAP)
from .exception import (AstronomerQueryException, AstronomerMapperException)
from .util import (camel_to_underscore, GIZMO_ID, GIZMO_LABEL, GIZMO_TYPE,
    GIZMO_ENTITY, GIZMO_VARIABLE, entity_name)


ENTITY_MAPPER_MAP = {}
GENERIC_MAPPER = 'generic.mapper'
_count = -1
_query_count = 0
_query_params = {}


def next_query_variable():
    global _count
    _count += 1

    return '{}_{}'.format(GIZMO_VARIABLE, _count)


def get_entity_mapper(entity=None, name=GENERIC_MAPPER):
    if isinstance(entity, _Entity):
        name = get_qualified_instance_name(entity)
    else:
        name = get_qualified_name(entity)

    if name not in ENTITY_MAPPER_MAP:
        name = GENERIC_MAPPER

    return ENTITY_MAPPER_MAP[name](self)


def next_param_name(param):
    param = re.sub('\W', '_', param)

    if param not in _query_params:
        _query_params[param] = -1

    _query_params[param] += 1

    return '{}_{}'.format(param, _query_params[param])


def next_param(param, value):
    if isinstance(value, _Entity):
        value = entity_name(value)

    return Param(next_param_name(param), value)


def next_entity_param(entity, param, value):
    name = entity_name(entity)
    field = '{}_{}'.format(name, param)

    return next_param(field, value)


class Mapper:

    def __init__(self, request, gremlin=None, auto_commit=True,
                 graph_instance_name=None):
        if not gremlin:
            gremlin = Gremlin()

        self.request = request
        self.gremlin = gremlin
        self.auto_commit = auto_commit
        self.graph_instance_name = graph_instance_name

        if not self.auto_commit and not self.graph_instance_name:
            error = ('If auto_commit is set, we need to know the'
                     ' graph instance name')
            raise ArgumentError(error)

        self.reset()

    def reset(self):
        self.gremlin.reset()
        global _query_count
        global _count
        global _query_params

        _query_count = 0
        _count = 0
        _query_params = {}
        self.queries = []
        self.return_vars = []
        self.entities = OrderedDict()  # ensure FIFO for testing
        self.del_entities = {}
        self.params = {}
        self.callbacks = {}
        self._magic_method = None

    def get_entity_variable(self, entity):
        ret = None

        for key, def_entity in self.entities.items():
            if entity == def_entity:
                return key

        return ret

    def get_mapper(self, entity=None, name=GENERIC_MAPPER):
        if entity is not None:
            name = entity_name(entity)

        if name not in ENTITY_MAPPER_MAP:
            name = GENERIC_MAPPER

        return ENTITY_MAPPER_MAP[name](self)

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

    def __getattr__(self, magic_method):
        """magic method that works in conjunction with __call__
        method these two methods are used to shortcut the retrieval
        of an entity's mapper and call a specific method against

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

    async def data(self, entity, *args):
        """utility method used to retrieve an entity's data. It
        also allows for method chaining in order to augment the
        resulting data.

        class MyMapper(_GenericMapper):
            async def add_two(self, entity, data):
                data['two'] = 2
                return data

            async def add_three(self, entity, data):
                data['three'] = 3
                return data

        entity = User()
        data = await mapper.data(user, 'add_two', 'add_three')

        the resulting data will have the data from the User class,
        plus a two and a three member
        """
        collection = isinstance(entity, Collection)

        async def get_data(entity, data):
            retrieved = data

            for method in args:
                mapper = self.get_mapper(entity)

                async def wrapper(entity, data):
                    res = await getattr(mapper, method)(entity=entity,
                                                        data=data)

                    return res

                retrieved = await wrapper(entity=entity,
                    data=retrieved)

            return retrieved

        if collection:
            data = []

            for coll_entity in entity:
                mapper = self.get_mapper(coll_entity)
                entity_data = await mapper.data(coll_entity)
                res = await get_data(coll_entity, entity_data)

                data.append(res)
        else:
            mapper = self.get_mapper(entity)
            entity_data = await mapper.data(entity)
            data = await get_data(entity, entity_data)

        return data

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

    def create(self, data=None, entity=None, data_type='python'):
        if data is None:
            data = {}

        if entity:
            mapper = self.get_mapper(entity)
        else:
            name = data.get(GIZMO_ENTITY, GENERIC_MAPPER)

            if isinstance(name, (list, tuple)):
                name = name[0]['value']

            mapper = self.get_mapper(name=name)

        kwargs = {
            'data': data,
            'entity': entity,
            'data_type': data_type,
        }

        return mapper.create(**kwargs)

    def connect(self, out_v, in_v, label=None, data=None, edge_entity=None,
                data_type='python'):
        """
        method used to connect two vertices and create an Edge object
        the resulting edge is not saved to to graph until it is passed to
        save allowing further augmentation
        """
        if not isinstance(out_v, Vertex):
            if not isinstance(out_v, (str, int)):
                err = ['The out_v needs to be either a Vertex or an id']
                raise AstronomerMapperException(err)

        if not isinstance(in_v, Vertex):
            if not isinstance(in_v, (str, int)):
                err = 'The in_v needs to be either a Vertex or an id'
                raise AstronomerMapperException(err)

        if data is None:
            data = {}

        data['outV'] = out_v
        data['inV'] = in_v
        data[GIZMO_TYPE] = 'edge'
        data[GIZMO_LABEL[0]] = label

        return self.create(data=data, entity=edge_entity, data_type=data_type)

    def start(self, entity):
        mapper = self.get_mapper(entity)

        return mapper.start(entity)

    def _build_queries(self):
        # if not self.auto_commit:
        #     commit = '.'.join([self.graph_instance_name, 'tx()', 'commit()'])
        #
        #     self.queries.append(commit)

        if len(self.return_vars) > 0:
            returns = []

            for k in self.return_vars:
                returns.append("'{}': {}".format(k, k))

            ret = '[{}]'.format(', '.join(returns))

            self.queries.append(ret)

        return self

    def get(self, entity):
        mapper = self.get_mapper(entity)

        return mapper.get(entity)

    def apply_statement(self, statement):
        self.gremlin.apply_statement(statement)

        return self

    async def send(self):
        self._build_queries()

        script = ";\n".join(self.queries)
        params = self.params
        entities = self.entities
        callbacks = self.callbacks
        entities.update(self.del_entities)
        self.reset()

        res = await self.query(script=script, params=params,
                          update_entities=entities, callbacks=callbacks)

        return res

    async def query(self, script=None, params=None, gremlin=None,
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

        response = await self.request.send(script, params, update_entities)

        for k, entity in update_entities.items():
            cbs = callbacks.get(entity, [])
            for c in cbs:
                c(entity)

        return Collection(self, response)


class _RootMapper(type):
    """
    In the case of custom mappers, this metaclass will register the entity name
    with the mapper object. This is done so that when entities are loaded by
    name the associated mapper is used to CRUD it.

    This only works when the Mapper.create method is used to
    create the entity
    """

    def __new__(cls, name, bases, attrs):
        cls = super(_RootMapper, cls).__new__(cls, name, bases, attrs)
        entity = attrs.pop('entity', None)

        if entity:
            map_name = entity_name(entity)
            ENTITY_MAPPER_MAP[map_name] = cls
        elif name == 'EntityMapper':
            ENTITY_MAPPER_MAP[GENERIC_MAPPER] = cls

        return cls

    def __call__(cls, *args, **kwargs):
        mapper = super(_RootMapper, cls).__call__(*args, **kwargs)

        for field in dir(mapper):
            if field.startswith('_'):
                continue

            val = getattr(mapper, field)

            if isinstance(val, EntityMapper):
                val.mapper = mapper.mapper
                val.gremlin = val.mapper.gremlin
                setattr(mapper, field, val)

        return mapper


class EntityMapper(metaclass=_RootMapper):
    VARIABLE = GIZMO_VARIABLE
    unique = False
    unique_fields = None
    save_statements = None

    def __init__(self, mapper=None):
        self.mapper = mapper
        self.gremlin = None

        if self.mapper:
            self.gremlin = mapper.gremlin

        self.reset()

    def reset(self):
        self.queries = []
        self.return_vars = []
        self.entities = {}
        self.params = {}
        self.callbacks = {}

    async def data(self, entity):
        return entity.data

    def get(self, entity):
        trav = self.start(entity)
        vertex = issubclass(self.entity, Vertex)
        param_value = str(self.entity)
        param_name = 'out_{}_{}'.format(entity.__class__.__name__, param_value)
        entity_param = next_param(param_name, param_value)

        if vertex:
            trav.out().hasLabel(entity_param)
        else:
            trav.outE(entity_param)

        return trav

    def enqueue(self, query, bind_return=True):
        for entry in query.queries:
            script = entry['script']

            if script in self.queries:
                continue

            if bind_return:
                variable = next_query_variable()
                script = '{} = {}'.format(variable, script)

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

    def _build_save_statements(self, entity, query, **kwargs):
        statement_query = Query(self.mapper)
        query_gremlin = Gremlin(self.gremlin.gv)

        for entry in query.queries:
            query_gremlin.bind_params(entry['params'])

        for statement in self.save_statements:
            instance = statement(entity, self, query, **kwargs)

            query_gremlin.apply_statement(instance)

        statement_query._add_query(str(query_gremlin),
                                  query_gremlin.bound_params, entity=entity)

        return statement_query

    def start(self, entity=None):
        return Traversal(self.mapper, entity or self.entity)

    def save(self, entity, bind_return=True, callback=None, *args, **kwargs):
        """callback and be a single callback or a list of them"""
        method = '_save_edge' if entity[GIZMO_TYPE] == 'edge' else \
            '_save_vertex'

        if not isinstance(callback, (list, tuple)) and callback:
            callback = [callback]
        else:
            callback = []

        if entity[GIZMO_ID]:
            callback.insert(0, self.on_update)
        else:
            callback.insert(0, self.on_create)

        self._enqueue_callback(entity, callback)

        return getattr(self, method)(entity=entity, bind_return=bind_return)

    def _save_vertex(self, entity, bind_return=True):
        """
        method used to save a entity. IF both the unique_type and unique_fields
        params are set, it will run a sub query to check to see if an entity
        exists that matches those values
        """
        query = Query(self.mapper)
        ref = self.mapper.get_entity_variable(entity)

        """
        check to see if the entity has been used already in the current script
        execution.
        If it has use the reference
        if it hasnt, go through the process of saving it
        """
        if ref:
            query._add_query(ref, params=None, entity=entity)

            return self.enqueue(query, bind_return)

        query.save(entity)

        if not entity[GIZMO_ID] and self.unique_fields:
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
        query = Query(self.mapper)
        save = True
        edge_ref = self.mapper.get_entity_variable(entity)
        out_v = entity.out_v
        out_v_id = out_v[GIZMO_ID] if isinstance(out_v, Vertex) else None
        in_v = entity.in_v
        in_v_id = in_v[GIZMO_ID] if isinstance(in_v, Vertex) else None
        out_v_ref = self.mapper.get_entity_variable(out_v)
        in_v_ref = self.mapper.get_entity_variable(in_v)

        if edge_ref:
            query._add_query(edge_ref, params=None, entity=entity)

            return self.enqueue(query, bind_return)

        """
        both out_v and in_v are checked to see if the entities stored in each
        respective variable has been used.
        If they have not and they are Vertex instances with an empty _id,
            send them to be saved.
        if they have been used, use the reference variable in the create edge
        logic
        """
        query.save(entity)

        if not entity[GIZMO_ID] and self.unique and in_v_id and out_v_id:
            from .statement import MapperUniqueEdge

            if not self.save_statements:
                self.save_statements = []

            if MapperUniqueEdge not in self.save_statements:
                self.save_statements.append(MapperUniqueEdge)

        if self.save_statements and len(self.save_statements):
            statement_query = self._build_save_statements(entity, query,
                out_v_id=out_v_id, in_v_id=in_v_id,
                label=entity[GIZMO_LABEL[0]], direction=self.unique)

            return self.enqueue(statement_query, False)
        else:
            return self.enqueue(query, bind_return)

    def delete(self, entity, lookup=True, callback=None):
        query = Query(self.mapper)

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
        entity else if utils.GIZMO_ENTITY is in data, that will be used
        finally, entity.GenericVertex or entity.GenericEdge will be used to
        construct the entity
        """
        check = True

        if data is None:
            data = {}

        if entity is not None:
            try:
                label = data.get(GIZMO_LABEL[0], None)
                entity = entity(data=data, data_type=data_type)
                check = False
            except Exception as e:
                pass

        if check:
            try:
                if GIZMO_ENTITY in data:
                    name = data[GIZMO_ENTITY]

                    if isinstance(name, (list, tuple)):
                        name = name[0]['value']

                    entity = ENTITY_MAP[name](data=data, data_type=data_type)
                else:
                    raise
            except Exception as e:
                # all else fails create a GenericVertex unless _type is 'edge'
                if data.get(GIZMO_TYPE, None) == 'edge':
                    entity = GenericEdge(data=data, data_type=data_type)
                else:
                    entity = GenericVertex(data=data, data_type=data_type)

        if GIZMO_ID in data:
            entity[GIZMO_ID] = data[GIZMO_ID]

        return entity

    def delete(self, entity, lookup=True, callback=None):
        query = Query(self.mapper)

        if not isinstance(callback, (list, tuple)) and callback:
            callback = [callback]
        else:
            callback = []

        query.delete(entity)
        callback.insert(0, self.on_delete)
        self._enqueue_callback(entity, callback)

        return self.enqueue(query, False)


class Query:

    def __init__(self, mapper):
        self.mapper = mapper
        self.gremlin = Gremlin(self.mapper.gremlin.gv)
        self.queries = []
        self.fields = []

        self.reset()

    def reset(self):
        self.fields = []
        return self

    def _add_query(self, script, params=None, entity=None):
        if params is None:
            params = {}

        self.queries.append({
            'script': script,
            'params': params,
            'entity': entity,
        })

        return self

    def _add_gremlin_query(self, entity=None):
        script = str(self.gremlin)
        params = self.gremlin.bound_params

        self._add_query(script, params, entity)

        return self.reset()

    def _field_changes(self, gremlin, entity, ignore=None):
        ignore = ignore or []
        entity_name = str(entity)
        entity_alias = '{}_alias'.format(entity_name)
        entity_alias = next_param(entity_alias, entity_alias)

        def add_field(field, data):
            values = data.get('values', data.get('value', None))

            if not isinstance(values, (list, tuple,)):
                values = [values, ]

            for i, value in enumerate(values):
                name = '{}_{}_{}'.format(entity_name, field, i)
                prop = "'{}'".format(field)

                gremlin.property(prop, Param(name, value))

        def add_property(field, value, properties=None, ignore=None):
            ignore = ignore or []

            if field.startswith('T.'):
                val_param = next_param('{}_{}'.format(entity_name,
                    field), value)
                gremlin.unbound('property', field, val_param)
                return

            field_name = '{}_{}'.format(entity_name, field)
            prop = next_param(field_name, field)
            value_name = '{}_value'.format(field_name)
            value_param = next_param(value_name, value)
            params = [prop, value_param]

            if properties:
                for key, val in properties.items():
                    prop_key = next_param('{}_{}'.format(prop.name,
                        key), key)
                    prop_val = next_param('{}_{}_val'.format(prop.name,
                        key), val)
                    params += [prop_key, prop_val]

            gremlin.property(*params)

        for field, changes in entity.changes.items():
            if field in ignore:
                continue

            if changes['immutable']:
                for val in changes['values']['values']:
                    add_property(field, val)
            elif changes['deleted']:
                prop = next_param('{}_{}'.format(entity_name, field), field)
                remove = Gremlin('').it.get().func('remove')

                gremlin.AS(entity_alias).properties(prop)
                gremlin.sideEffect.close(remove)
                gremlin.select(entity_alias)
            else:
                for action, value in changes['values'].items():
                    if action == 'added':
                        for val in value:
                            add_property(field, val['value'],
                                val['properties'])

    def _add_vertex(self, entity, set_variable=None):
        entity.data_type = 'graph'
        gremlin = self.gremlin
        label = None
        ignore = ['T.label', 'label']

        if entity['label']:
            label = next_entity_param(entity, 'label', entity['label'])
            gremlin.unbound('addV', 'T.label', label)
        else:
            gremlin.addV()

        if set_variable:
            gremlin.set_ret_variable(set_variable, ignore=[GIZMO_ID, ])

        self._field_changes(gremlin, entity, ignore=ignore)
        gremlin.func('next')

        entity.data_type = 'python'

        return self._add_gremlin_query(entity)

    def _update_entity(self, entity, set_variable=None):
        entity.data_type = 'graph'
        gremlin = self.gremlin
        entity_type, entity_id = entity.get_rep()

        if not entity_id:
            raise Exception()

        _id = next_param('{}_ID'.format(str(entity)), entity_id)
        ignore = [GIZMO_ID, GIZMO_LABEL[1]]
        alias = '{}_{}_updating'.format(entity_type, entity_id)
        alias = next_param(alias, alias)

        getattr(gremlin, entity_type.upper())(_id)
        gremlin.AS(alias)

        self._field_changes(gremlin, entity, ignore=ignore)
        gremlin.select(alias).next()

        entity.data_type = 'python'

        return self._add_gremlin_query(entity)

    def _add_edge(self, entity, set_variable=None):
        if not entity[GIZMO_LABEL[0]]:
            msg = 'A label is required in order to create an edge'
            raise AstronomerQueryException(msg)

        def get_or_create_ends():
            """this function will determine if the edge has both ends. If
            either end is an _Entity object it will get the reference to
            the object or save it and create a reference. Either the entity's
            id or reference will be used when saving the edge.
            """
            out_v = entity.out_v
            out_v_ref = None
            in_v = entity.in_v
            in_v_ref = None

            if out_v is None or in_v is None:
                error = ('Both out and in vertices must be set before'
                         ' saving the edge')
                raise AstronomerQueryException(error)

            if isinstance(out_v, _Entity):
                if out_v[GIZMO_ID]:
                    out_v = out_v[GIZMO_ID]
                else:
                    out_v_ref = self.mapper.get_entity_variable(out_v)

                    if not out_v_ref:
                        self.mapper.save(out_v)
                        out_v_ref = self.mapper.get_entity_variable(out_v)

                    if out_v_ref:
                        out_v = out_v_ref

            if isinstance(in_v, _Entity):
                if in_v[GIZMO_ID]:
                    in_v = in_v[GIZMO_ID]
                else:
                    in_v_ref = self.mapper.get_entity_variable(in_v)

                    if not in_v_ref:
                        self.mapper.save(in_v)
                        in_v_ref = self.mapper.get_entity_variable(in_v)

                    if in_v_ref:
                        in_v = in_v_ref

            return {
                'out': {
                    'is_ref': out_v_ref,
                    'v': out_v,
                },
                'in': {
                    'is_ref': in_v_ref,
                    'v': in_v,
                },
            }

        ends = get_or_create_ends()
        name = str(entity)
        gremlin = self.gremlin
        g = Gremlin(gremlin.gv)
        label = next_param('{}_label'.format(name), entity[GIZMO_LABEL[0]])

        """
        g.V($OUT_ID).next().addEdge($LABEL, g.V($IN_ID).next()).property(....)
        """
        in_v = ends['in']
        out_v = ends['out']

        if in_v['is_ref']:
            g.unbound('V', in_v['v'])
        else:
            in_id = next_param('{}_in'.format(name), in_v['v'])

            g.V(in_id)

        g.func('next')

        if out_v['is_ref']:
            gremlin.unbound('V', out_v['v'])
        else:
            out_id = next_param('{}_out'.format(name), out_v['v'])

            gremlin.V(out_id)

        ignore = [GIZMO_LABEL[0], GIZMO_LABEL[1], GIZMO_TYPE]
        edge_args = [label, g]

        # edge properites only get one value and no meta-properties
        for field, changes in entity.changes.items():
            if field in ignore:
                continue

            try:
                if changes['immutable']:
                    value = changes['values']['values'][-1]
                else:
                    value = changes['values'][-1]
            except:
                continue

            field_param = next_param('{}_{}'.format(name, field), field)
            field_value = next_param('{}_value'.format(field_param.name),
                value)
            edge_args += [field_param, field_value]

        gremlin.func('next').addEdge(*edge_args)

        return self._add_gremlin_query(entity)

    def save(self, entity, set_variable=None):
        if not entity[GIZMO_TYPE]:
            msg = 'The entity does not have a type defined'
            raise AstronomerQueryException(msg)

        entity_type = entity[GIZMO_TYPE]

        if not entity[GIZMO_ID]:
            if entity_type == 'vertex':
                self._add_vertex(entity, set_variable)
            else:
                self._add_edge(entity, set_variable)
        else:
            self._update_entity(entity, set_variable)

    def delete(self, entity):
        entity_type, _id = entity.get_rep()

        if not _id:
            msg = ('The entity does not have an id defined and'
                   ' connot be deleted')
            raise AstronomerQueryException(msg)

        if not entity[GIZMO_TYPE]:
            msg = 'The entity does not have a type defined'
            raise AstronomerQueryException(msg)

        delete = next_param('{}_ID'.format(str(entity)), _id)

        getattr(self.gremlin, entity_type)(delete).next().func('remove')

        return self._add_gremlin_query(entity)


class Collection(object):

    def __init__(self, mapper, response=None):
        self.mapper = mapper

        if not response:
            response = lambda: None
            response.data = []

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

    @property
    async def mapper_data(self):
        """this will get the data from the entity's mapper if it has a
        custom mapper
        """
        data = []

        if len(self):
            mapper = self.mapper.get_mapper(self[0])

            for entity in self:
                data.append(await mapper.data(entity))

        return data

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
                    raise StopIteration()
            except Exception as e:
                raise StopIteration()

        return entity

    def __setitem__(self, key, value):
        self._entities[key] = value

    def __delitem__(self, key):
        if key in self._entities:
            del self._entities[key]

    async def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return self.__next__()
        except:
            raise StopAsyncIteration()

    def __iter__(self):
        return self
    
    def __next__(self):
        entity = self[self._index]
        self._index += 1

        return entity


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
        self._collection = None
        _id = None
        _base = isinstance(entity, _Entity)

        if _base:
            ev, _id = entity.get_rep()

        if _id:
            bound_id = next_param('{}_EYE_DEE'.format(str(entity)), _id)

            getattr(self, ev)(bound_id)
        else:
            if _base:
                _type = entity.__class__.__name__
            else:
                _type = entity.__name__
                ev, _ = entity().get_rep()

            _type = camel_to_underscore(_type)
            bound_type = self.bind_param(_type, 'BOUND_TYPE')

            getattr(self, ev)().hasLabel(bound_type[0])

    async def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._collection:
            self._collection = await self.to_collection()

        return await self._collection.__anext__()

    async def to_collection(self):
        collection = await self._mapper.query(gremlin=self)

        return collection
