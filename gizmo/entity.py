from inspect import isfunction
from copy import deepcopy

from .field import String, DateTime, List, Integer, Float
from .field import Map, _Fields, Field, Enum
from .utils import get_qualified_instance_name, IMMUTABLE
from .utils import GIZMO_MODEL, GIZMO_CREATED, GIZMO_LABEL
from .utils import GIZMO_MODIFIED, GIZMO_ID
from .utils import current_date_time, camel_to_underscore


#Holds the model->object mappings
_MAP = {}
DEFAULT_MODEL_FIELDS = [
    GIZMO_MODEL,
    GIZMO_CREATED,
    GIZMO_MODIFIED,
    GIZMO_ID,
]


class _RootEntity(type):
    """
    maps all models during definition to their object so that it can be
    loaded later
    overwrites the __init__ method. Models cannot define one
    """

    def __new__(cls, name, bases, attrs):

        def new_init__(self, data=None, data_type='python'):
            if data is None:
                data = {}

            self.dirty = False
            self.data_type = data_type

            if hasattr(self, '_node_label'):
                cls_label = self._node_label
            else:
                cls_label = str(self)

            if '_allowed_undefined' in attrs:
                self._allowed_undefined = attrs['_allowed_undefined']

            if '_atomic_changes' in attrs:
                self._atomic_changes = attrs['_atomic_changes']

            # the modified field is a microsecond later than the created
            # this is done for testing purposes
            modified = lambda: current_date_time(0.001)
            self.fields = _Fields({
                GIZMO_MODEL: String(get_qualified_instance_name(self),\
                    data_type=data_type, track_changes=False),
                GIZMO_CREATED: DateTime(value=current_date_time,\
                    data_type=data_type, set_max=1, track_changes=False),
                GIZMO_MODIFIED: DateTime(value=modified,\
                    data_type=data_type, track_changes=False),
                GIZMO_LABEL: String(cls_label, data_type=data_type,\
                    track_changes=False),
                GIZMO_ID: String(data_type=data_type,\
                    track_changes=False),
            })

            if isinstance(self, Edge):
                if 'out_v' in data:
                    self.out_v = data['out_v']

                    del data['out_v']
                else:
                    self.out_v = None

                if '_outV' in data:
                    self.outV = data['_outV']

                    del data['_outV']
                else:
                    self.outV = None

                if 'in_v' in data:
                    self.in_v = data['in_v']

                    del data['in_v']

                if '_inV' in data:
                    self.inV = data['_inV']

                    del data['_inV']
                else:
                    self.inV = None

                label = data.get('label', None)

                if label is None:
                    label = cls_label

                self.fields[GIZMO_LABEL] = String(value=label, \
                    data_type=data_type)

            """"
            build the properties for the instance
            ignore things that start with an underscore and methods
            this is done for all of the bases first, then the actual model
            """
            undefined = deepcopy(data)

            def update_fields(obj):
                for name, field in obj.items():
                    if not name.startswith('_'):
                        if isinstance(field, Field):
                            value = field.value

                            if name in data:
                                value = data[name]
                                del(undefined[name])

                            if name not in DEFAULT_MODEL_FIELDS:
                                self.dirty = True

                            kwargs = {
                                'value': value,
                                'data_type': field.data_type,
                                'set_max': field.set_max,
                                'track_changes': field.track_changes,
                            }

                            if isinstance(field, Enum):
                                kwargs['allowed'] = field.allowed

                            instance = field.__class__(**kwargs)
                            self.fields[name] = instance
                        elif isfunction(field) == False:
                            setattr(self, name, field)

            for b in bases:
                update_fields(b.__dict__)

            update_fields(attrs)
            self.hydrate(undefined)

            if data is not None and GIZMO_ID in data:
                self.fields[GIZMO_ID].field_value = data[GIZMO_ID]

        attrs['__init__'] = new_init__
        cls = super(_RootEntity, cls).__new__(cls, name, bases, attrs)
        map_name = '%s.%s' % (cls.__module__, cls.__name__)
        _MAP[map_name] = cls

        return cls


class _BaseEntity(metaclass=_RootEntity):
    _immutable = IMMUTABLE['vertex']
    _allowed_undefined = False
    _atomic_changes = False

    def hydrate(self, data=None):
        if data is None:
            data = {}

        for field, value in data.items():
            self[field] = value

        return self

    def _add_undefined_field(self, name, value):
        if isinstance(value, dict):
            field = Map(value, self.data_type)
        elif isinstance(value, list):
            field = List(value, self.data_type)
        elif isinstance(value, int):
            field = Integer(value, self.data_type)
        elif isinstance(value, float):
            field = Float(value, self.data_type)
        else:
            field = String(value, self.data_type)

        self.fields[name] = field

        return field

    def __setitem__(self, name, value):
        if name not in self._immutable and name in self.fields:
            self.fields[name].value = value
            self.dirty = True
        elif self._allowed_undefined:
            self._add_undefined_field(name, value)
            self.dirty = True

        return self

    def __getitem__(self, name):
        value = None

        if name in self.fields:
            value = self.fields[name].value
        elif self._allowed_undefined:
            value = self._add_undefined_field(name, value)

        return value

    def __str__(self):
        return self.__unicode__()

    def __unicode__(self):
        return camel_to_underscore(self.__class__.__name__)

    def _get_data_type(self):
        return self.data_type

    def _set_data_type(self, data_type):
        self.data_type = data_type
        self.fields.data_type = data_type

    field_type = property(_get_data_type, _set_data_type)

    def get_rep(self):
        entity = 'E' if self._type == 'edge' else 'V'

        return entity, self['_id']

    def get_data(self, full=False):
        return self.fields.get_data(full=full)

    data = property(get_data)

    @property
    def label(self):
        return self.__getitem__(GIZMO_LABEL)

    @property
    def changed(self):
        return self.fields.changed

    @property
    def unchanged(self):
        return self.fields.unchanged

    @property
    def removed(self):
        return self.fields.removed


class Vertex(_BaseEntity):
    _type = 'vertex'


class GenericVertex(Vertex):
    _allowed_undefined = True


class Edge(_BaseEntity):
    _type = 'edge'
    _immutable = IMMUTABLE['edge']


class GenericEdge(Edge):
    _allowed_undefined = True
