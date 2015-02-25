from utils import get_qualified_name, get_qualified_instance_name
from utils import IMMUTABLE, GIZMO_MODEL, GIZMO_NODE_TYPE
from entity import Edge, Vertex, GenericVertex, GenericEdge, _MAP, _BaseEntity
from gremlinpy.gremlin import Gremlin, Function
from gremlinpy.statement import GetEdge
from exception import *
import json

#Holds the model->mapper mappings for custom mappers
_MAPPER_MAP = {}
GENERIC_MAPPER = 'generic.mapper'
count = 0
query_count = 0


class _RootMapper(type):
    """
    In the case of custom mappers, this metaclass will register the model name
    with the mapper object. This is done so that when models are loaded by name
    its mappers is used to CRUD it.

    This only works when the mapper_instance.create_model() method is used to
    create the model
    """

    def __new__(cls, name, bases, attrs):
        cls = super(_RootMapper, cls).__new__(cls, name, bases, attrs)
        model = attrs.pop('model', None)

        if model:
            map_name = '%s.%s' % (model.__module__, model.__name__)
            _MAPPER_MAP[map_name] = cls
        elif name == '_GenericMapper':
            _MAPPER_MAP[GENERIC_MAPPER] = cls

        return cls


class _GenericMapper(object):
    __metaclass__ = _RootMapper
    VARIABLE = 'gizmo_var'
    unique = False
    unique_fields = None
    error_on_non_unique = False

    def __init__(self, gremlin=None, mapper=None):
        if gremlin is None:
            gremlin = Gremlin()

        self.gremlin = gremlin
        self.mapper = mapper
        self.reset()

    def reset(self):
        self.queries = []
        self.models = {}
        self.params = {}

    def enqueue(self, query, bind_return=True):
        for entry in query.queries:
            global count
            count += 1
            script = entry['script']

            if bind_return:
                variable = '%s_%s' % (self.VARIABLE, count)
                script = '%s = %s' % (variable, script)

                if 'model' in entry:
                    self.models[variable] = entry['model']

            self.queries.append(script)
            self.params.update(entry['params'])

        return self

    def save(self, model, bind_return=True, *args, **kwargs):
        method = '_save_edge' if model._type == 'edge' else '_save_vertex'

        return getattr(self, method)(model=model, bind_return=bind_return)

    def _save_vertex(self, model, bind_return=True):
        """
        method used to save a model. IF both the unique_type and unique_fields
        params are set, it will run a sub query to check to see if an entity
        exists that matches those values
        """
        query = Query(self.gremlin, self.mapper)
        save = True
        ref = self.mapper.get_model_variable(model)

        """
        check to see if the model has been used already in the current script
        execution.
        If it has use the reference
        if it hasnt, go through the process of saving it
        """
        if ref:
            query.add_query(ref, params=None, model=model)
            save = False

        if model['_id'] is None and self.unique_fields is not None:
            gremlin = Gremlin(self.gremlin.gv)
            node_type = "'%s'" % GIZMO_NODE_TYPE

            if '*' in self.unique_fields:
                self.unique_fields = model.fields.keys()

            gremlin.V.has(node_type, 'T.eq', model._node_type)

            for field in self.unique_fields:
                g_field = '"%s"' % field

                gremlin.has(g_field, 'T.eq', model[field])

            try:
                first = self.mapper.query(gremlin=gremlin).first()

                if self.error_on_non_unique:
                    message = 'The fields: %s are not unique' % ', '.join(self.unique_fields)
                    raise MapperException([message])

                model.fields['_id'].value = first['_id']
                query.by_id(model['_id'], model)

                save = False
            except StopIteration, e:
                pass

        if save:
            query.save(model)

        return self.enqueue(query, bind_return)

    def _save_edge(self, model, bind_return=True):
        query = Query(self.gremlin, self.mapper)
        save = True
        #TODO: send an edge to be saved multiple times
        edge_ref = self.mapper.get_model_variable(model)
        out_v = model.out_v
        in_v = model.in_v
        out_v_ref = self.mapper.get_model_variable(out_v)
        in_v_ref = self.mapper.get_model_variable(in_v)

        """
        both out_v and in_v are checked to see if the models stored in each
        respective variable has been used.
        If they have not and they are Vertex instances with an empty _id,
            send them to be saved.
        if they have been used, use the reference variable in the create edge
        logic
        """
        if not out_v_ref and isinstance(out_v, Vertex) and out_v['_id'] is None:
            self.mapper.save(out_v)
            out_v = self.mapper.get_model_variable(out_v)
        else:
            out_v = out_v_ref

        if not in_v_ref and isinstance(in_v, Vertex) and in_v['_id'] is None:
            self.mapper.save(in_v)
            in_v = self.mapper.get_model_variable(in_v)
        else:
            in_v = in_v_ref

        out_v = out_v['_id'] if isinstance(out_v, Vertex) else out_v
        in_v = in_v['_id'] if isinstance(in_v, Vertex) else in_v

        if model['_id'] and self.unique:
            edge = GetEdge(out_v, in_v, model['_label'])
            gremlin = Gremlin(self.gremlin.gv)

            gremlin.apply_statement(edge)

            try:
                edge = self.mapper.query(gremlin=gremlin).first()
                save = False

                query.by_id(edge['_id'], model)
            except Exception, e:
                save = True

        if save:
            query.save(model)

        return self.enqueue(query, bind_return)

    def delete(self, model, lookup=True):
        query = Query(self.gremlin, self.mapper)

        query.delete(model)

        return self.enqueue(query, False)

    def create_model(self, data=None, model_class=None, data_type='python'):
        """
        Method used to create a new model based on the data that is passed in.
        If the kwarg model_class is passed in, it will be used to create the model
        else if utils.GIZMO_MODEL is in data, that will be used
        finally, entity.GenericVertex or entity.GenericEdge will be used to construct the model

        """
        check = True

        if data is None:
            data = {}

        if model_class is not None:
            try:
                model = model_class(data, data_type=data_type)
                check = False
            except Exception as e:
                pass

        if check:
            try:
                if GIZMO_MODEL in data:
                    name = data[GIZMO_MODEL]
                    model = _MAP[name](data, data_type=data_type)
                else:
                    raise
            except Exception as e:
                # all else fails create a GenericVertex unless _type is 'edge'
                if data.get('_type', None) == 'edge':
                    model = GenericEdge(data, data_type=data_type)
                else:
                    model = GenericVertex(data, data_type=data_type)

        return model


class Mapper(object):
    VARIABLE = 'gizmo_var'

    def __init__(self, request, gremlin=None, auto_commit=False, logger=None):
        if gremlin is None:
            gremlin = Gremlin()

        self.request = request
        self.gremlin = gremlin
        self.auto_commit = auto_commit
        self.logger = logger
        self.reset()

    def reset(self):
        self.gremlin.reset()

        self.queries = []
        self.models = {}
        self.params = {}

    def get_model_variable(self, model):
        ret_key = None

        for key, def_model in self.models.iteritems():
            if model == def_model:
                ret_key = key
                break

        return ret_key

    def get_mapper(self, model=None, name=GENERIC_MAPPER):
        if model is not None:
            if isinstance(model, _BaseEntity):
                name = get_qualified_instance_name(model)
            else:
                name = get_qualified_name(model)

        if name not in _MAPPER_MAP:
            name = GENERIC_MAPPER

        return _MAPPER_MAP[name](self.gremlin, self)

    def _enqueue_mapper(self, mapper):
        self.queries += mapper.queries
        self.models.update(mapper.models)
        self.params.update(mapper.params)
        mapper.reset()

        return self

    def save(self, model, bind_return=True, mapper=None, **kwargs):
        if mapper is None:
            mapper = self.get_mapper(model)

        mapper.save(model, bind_return, **kwargs)

        return self._enqueue_mapper(mapper)

    def delete(self, model, mapper=None):
        if mapper is None:
            mapper = self.get_mapper(model)

        mapper.delete(model)

        return self.enqueue(mapper)

    def connect(self, out_v, in_v, label=None, data=None, edge_model=None, data_type='python'):
        """
        method used to connect two vertices and create an Edge object
        the resulting edge is not saved to to graph until it is passed to save allowing
        further augmentation
        """
        if not isinstance(out_v, Vertex):
            if not isinstance(out_v, str):
                raise ModelException(['The out_v needs to be eiter a Vertex or string id'])

        if not isinstance(in_v, Vertex):
            if not isinstance(in_v, str):
                raise ModelException(['The in_v needs to be eiter a Vertex or string id'])

        if data is None:
            data = {}

        data['out_v'] = out_v
        data['in_v'] = in_v
        data['_type'] = 'edge'
        data['_label'] = label

        return self.create_model(data=data, model_class=edge_model, data_type=data_type)

    def create_model(self, data=None, model_class=None, data_type='python'):
        if data is None:
            data = {}

        if model_class:
            mapper = self.get_mapper(model_class)
        else:
            name = data.get(GIZMO_MODEL, GENERIC_MAPPER)
            mapper = self.get_mapper(name=name)

        kwargs = {'data': data, 'model_class': model_class, 'data_type': data_type}

        return mapper.create_model(**kwargs)

    def _build_queries(self):
        if self.auto_commit is False:
            commit = '.'.join([self.gremlin.gv, 'commit()'])

            self.queries.append(commit)

        if len(self.models) > 0:
            returns = []

            for k in self.models.keys():
                returns.append("'%s': %s" % (k, k))

            ret = '[%s]' % ','.join(returns)

            self.queries.append(ret)

        return self

    def start(self, model):
        return Traversal(self, model)

    def apply_statement(self, statement):
        self.gremlin.apply_statement(statement)

        return self

    def send(self):
        self._build_queries()

        script = ";\n".join(self.queries)
        params = self.params
        models = self.models

        self.reset()

        return self.query(script=script, params=params, update_models=models)

    def query(self, script=None, params=None, gremlin=None, update_models=None):
        if gremlin is not None:
            script = str(gremlin)
            params = gremlin.bound_params

            gremlin.reset()

        if script is None:
            script = ''

        if params is None:
            params = {}

        if update_models is None:
            update_models = {}
        # print script
        # print params
        if self.logger:
            self.logger.debug(script)
            self.logger.debug(json.dumps(params))

        response = self.request.send(script, params, update_models)

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

    def reset(self):
        self.fields = []

        self.gremlin.reset()

        return self

    def next_var(self):
        global query_count
        query_count += 1

        return '%s_%s' % (self.QUERY_VAR, query_count)

    def add_query(self, script, params=None, model=None):
        if params is None:
            params = {}

        self.queries.append({
            'script': script,
            'params': params,
            'model': model,
        })

        return self

    def add_gremlin_query(self, model=None):
        script = str(self.gremlin)
        params = self.gremlin.bound_params

        self.add_query(script, params, model)

        return self.reset()

    def build_fields(self, data, _immutable, prefix=''):
        gremlin = self.gremlin

        for key, val in data.iteritems():
            name = '%s_%s' % (prefix, key)

            if key not in _immutable:
                value = val

                if type(val) is dict or type(val) is list:
                    listed = self.iterable_to_graph(val, prefix)
                    value = "[%s]" % listed
                    entry = "'%s': %s" % (key, value)

                    self.fields.append(entry)
                else:
                    bound = gremlin.bind_param(value)

                    self.fields.append("'%s': %s" % (key, bound[0]))

        return self

    def update_fields(self, data, _immutable, prefix=''):
        gremlin = self.gremlin

        for k, v in data.iteritems():
            name = '%s_%s' % (prefix, k)

            if k not in _immutable:
                if type(v) is dict or type(v) is list:
                    #import pudb; pu.db
                    gmap = self.iterable_to_graph(v, prefix)
                    entry = "it.setProperty('%s', [%s])" % (k, gmap)
                else:
                    bound = self.gremlin.bind_param(v)
                    entry = "it.setProperty('%s', %s)" % (k, bound[0])

                self.fields.append(entry)

        return self

    def iterable_to_graph(self, iterable, prefix=''):
        if isinstance(iterable, dict):
            return self._dict_to_graph(iterable, prefix)
        else:
            return self._list_to_graph(iterable, prefix)

    def _dict_to_graph(self, iterable, prefix=''):
        gremlin = self.gremlin
        gval = []

        for key, value in iterable.iteritems():
            if type(value) is dict or type(value) is list:
                gval.append(self.iterable_to_graph(value, prefix))
            else:
                bound = gremlin.bind_param(value)

                gval.append("'%s': %s" % (key, bound[0]))

        return ','.join(gval)

    def _list_to_graph(self, iterable, prefix=''):
        gremlin = self.gremlin
        gval = []

        for key, value in enumerate(iterable):
            if type(value) is dict or type(value) is list:
                gval.append(self.iterable_to_graph(value, prefix))
            else:
                bound = gremlin.bind_param(value)

                gval.append(bound[0])

        return ','.join(gval)

    def by_id(self, _id, model, set_variable=None):
        gremlin = self.gremlin
        entity = 'e' if model['_type'] == 'edge' else 'v'

        getattr(gremlin, entity)(_id)

        if set_variable is not None:
            gremlin.set_ret_variable(set_variable)

        return self.add_gremlin_query(model)

    def add_vertex(self, model, set_variable=False):
        if model._type is None:
            raise QueryException(['Models need to have a type defined in order to save'])

        model.field_type = 'graph'
        gremlin = self.gremlin

        if set_variable:
            gremlin.set_ret_variable(set_variable)

        # use the model.fields.data instead of model.data because
        # model.data can be monkey-patched with custom mappers
        self.build_fields(model.fields.data, IMMUTABLE['vertex'])

        script = '%s.addVertex([%s])' % (gremlin.gv, ', '.join(self.fields))

        gremlin.set_graph_variable('').raw(script)

        model.field_type = 'python'

        return self.add_gremlin_query(model)

    def add_edge(self, model, set_variable=False):
        if model['_label'] is None:
            raise QueryException(['The edge must have a label before saving'])

        model.field_type = 'graph'
        gremlin = self.gremlin
        out_v, in_v = self._get_or_create_edge_vertices(model)
        label_bound = gremlin.bind_param(model['_label'])
        edge_fields = ''

        if set_variable:
            gremlin.set_ret_variable(set_variable)

        self.build_fields(model.fields.data, IMMUTABLE['edge'])

        if len(self.fields) > 0:
            edge_fields = ', [%s]' % ', '.join(self.fields)

        script = '%s.addEdge(%s, %s, %s%s)' % (gremlin.gv, out_v, in_v, label_bound[0], edge_fields)

        gremlin.set_graph_variable('').raw(script)

        model.field_type = 'python'

        return self.add_gremlin_query(model)

    def _get_or_create_edge_vertices(self, edge):
        out_v = edge.out_v
        in_v = edge.in_v

        if out_v is None or in_v is None:
            error = 'Both out and in vertices must be set before saving \
                the edge'

            raise QueryException([error])

        out_v_mod = self.mapper.get_model_variable(out_v)
        in_v_mod = self.mapper.get_model_variable(in_v)

        if out_v_mod is None:
            self.mapper.save(out_v)
            out_v_mod = self.mapper.get_model_variable(out_v)

        if in_v_mod is None:
            self.mapper.save(in_v)
            in_v_mod = self.mapper.get_model_variable(in_v)

        return out_v_mod, in_v_mod

    def update(self, model, set_variable=None):
        if model._type is None:
            raise QueryException(['The model must have a type defined in order to update'])

        if model['_id'] is None:
            raise QueryException(['The model must have an _id defined in order to update'])

        if model.dirty == False:
            return self.by_id(model['_id'], model, set_variable)

        gremlin = self.gremlin
        model.field_type = 'graph'
        model_type = 'e' if model._type == 'edge' else 'v'

        if set_variable:
            gremlin.set_ret_variable(set_variable)

        self.update_fields(model.fields.data, model._immutable, prefix=model.__class__.__name__)

        next_func = Function(gremlin, 'next')

        getattr(gremlin, model_type)(model['_id'])._().sideEffect.close('; '.join(self.fields)).add_token(next_func)

        model.field_type = 'python'

        return self.add_gremlin_query(model)

    def save(self, model, set_variable=None):
        model.field_type = 'python'

        if model._type is None:
            raise EntityException(['The model does not have a _type defined'])

        if not model['_id']:
            if model._type == 'vertex':
                self.add_vertex(model, set_variable)
            else:
                self.add_edge(model, set_variable)
        else:
            self.update(model, set_variable)

        return self

    def delete(self, model):
        gremlin = self.gremlin

        _id = model['_id']

        if _id is None:
            raise EntityException(['Models must have an _id before they are deleted'])

        if model._type is None:
            raise EntityException(['Models need to have a type defined'])

        entity = 'e' if model._type == 'edge' else 'v'

        getattr(gremlin, entity)(_id).remove()

        return self.add_query(str(gremlin), gremlin.bound_params, model)


class Traversal(Gremlin):
    """
    class used to start a traversal query based on a given model
    when the class is created, the model's _id and type are are
    set on the Gremlin object

    example:

    """

    def __init__(self, mapper, model):
        graph_variable = mapper.gremlin.gv

        super(Traversal, self).__init__(graph_variable)

        self._mapper = mapper
        self._model = model
        entity, _id = model.get_rep()

        getattr(self, entity)(_id)

    def define_traversal(self, traversal):
        if hasattr(traversal, '__call__'):
            self.traversal = traversal

        return self

    def start_bredth(self):
        pass

    def start_depth(self):
        pass

    def to_collection(self):
        return self._mapper.send(gremlin=self)


class Collection(object):

    def __init__(self, mapper, response):
        self.mapper = mapper
        self.response = response
        self._models = {}
        self._index = 0
        self._data_type = 'graph'

    def first(self):
        return self[0]

    def last(self):
        return self[-1]

    @property
    def data(self):
        """
        method used to return the raw data from the
        response
        """
        return [x for x in self.response.data]

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
        model = self._models.get(key, None)

        if model is None:
            try:
                data = self.response[key]

                if data is not None:
                    model = self.mapper.create_model(data=data, data_type=self._data_type)
                    model.dirty = False
                    self._models[key] = model
                else:
                    raise
            except Exception, e:
                raise StopIteration()

        return model

    def __setitem__(self, key, value):
        self._models[key] = value

    def __delitem__(self, key):
        if key in self._models:
            del self._models[key]
