import copy
import uuid

from .field import (Field, FieldManager, GremlinID, Integer, String,
                    GremlinLabel, GremlinType, GIZMOEntity)
from .util import (camel_to_underscore, entity_name, GIZMO_ID, GIZMO_LABEL,
    GIZMO_TYPE, GIZMO_ENTITY)


ENTITY_MAP = {}


class _EntityType(type):

    def __new__(cls, name, bases, attrs):

        def __init__(self, data=None, data_type='python'):

            if isinstance(self, Edge):
                if data and 'outV' in data:
                    self.outV = data['outV']

                    del data['outV']
                else:
                    self.outV = None

                if data and 'inV' in data:
                    self.inV = data['inV']

                    del data['inV']
                else:
                    self.inV = None

            data = copy.deepcopy(data or {})
            fields = {}
            _all_attrs = {}
            self._data_type = data_type

            def def_fields(obj_attrs):
                keys = obj_attrs.keys()

                for key, val in obj_attrs.items():
                    if isinstance(val, Field):
                        fields[key] = copy.deepcopy(val)

                        if not fields[key].name:
                            fields[key].name = key

            def walk(bases):
                for base in bases:
                    _all_attrs.update(base.__dict__)
                    walk(base.__bases__)

            walk(bases)
            def_fields(_all_attrs)
            def_fields(attrs)
            _all_attrs.update(attrs)

            ast_entity = entity_name(self)
            allow_undefined = _all_attrs.get('allow_undefined', False)
            label = attrs.get('label', str(self))
            _id = attrs.get('id', attrs.get('_id', None))
            _type = 'vertex' if isinstance(self, Vertex) else 'edge'
            fields[GIZMO_LABEL[0]] = GremlinLabel(GIZMO_LABEL[1], values=label)
            fields[GIZMO_ID] = GremlinID(GIZMO_ID, values=_id)
            fields[GIZMO_TYPE] = GremlinType(GIZMO_TYPE, values=_type)
            fields[GIZMO_ENTITY] = GIZMOEntity(GIZMO_ENTITY, values=ast_entity)
            self.fields = FieldManager(fields=fields,
                                       allow_undefined=allow_undefined,
                                       data_type=data_type)

            if GIZMO_LABEL[0] in data:
                del data[GIZMO_LABEL[0]]

            self.hydrate(data, True)

        attrs['__init__'] = __init__
        cls = type.__new__(cls, name, bases, attrs)
        map_name = '{}.{}'.format(cls.__module__, cls.__name__)
        ENTITY_MAP[map_name] = cls

        return cls


class _Entity(metaclass=_EntityType):

    def hydrate(self, data=None, reset_initial=False):
        from pprint import pprint
        self.fields.hydrate(data, reset_initial)

        return self

    def __str__(self):
        return self.__unicode__()

    def __unicode__(self):
        return camel_to_underscore(self.__class__.__name__)

    def __getitem__(self, field):
        return self.fields[field]

    def __setitem__(self, field, value):
        self.fields[field] = value

        return self.fields[field]

    def __delitem__(self, field):
        del self.fields[field]

    def _get_data_type(self):
        return self._data_type

    def _set_data_type(self, data_type):
        self._data_type = data_type
        self.fields.data_type = data_type

    data_type = property(_get_data_type, _set_data_type)

    @property
    def data(self):
        return self.fields.data

    @property
    def changes(self):
        return self.fields.changes

    @property
    def changed(self):
        return self.fields.changed

    @property
    def values(self):
        return self.fields.values

    @property
    def deleted(self):
        return self.fields.deleted

    def get_rep(self):
        entity = 'E' if self[GIZMO_TYPE] == 'edge' else 'V'

        return entity, self['id']

    def empty(self):
        self.fields.empty()

        return self


class Vertex(_Entity):
    pass


class GenericVertex(Vertex):
    allow_undefined = True


class Edge(_Entity):

    @property
    def out_v(self):
        return self.outV

    @property
    def in_v(self):
        return self.inV


class GenericEdge(Edge):
    allow_undefined = True
