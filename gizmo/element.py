from field import String, DateTime, Boolean, List, Map, _Fields
from utils import get_qualified_name, get_qualified_instance_name, TYPES, IMMUTABLE
from utils import GIZMO_MODEL, GIZMO_CREATED, GIZMO_MODIFIED, GIZMO_NODE_TYPE, GIZMO_TYPE, GIZMO_ID, GIZMO_LABEL

#Holds the model->object mappings
_MAP = {}

class _RootElement(type):
    """
    maps all models during definition to their object so that it can be
    loaded later
    """
    def __new__(cls, name, bases, attrs):
        cls = super(_RootElement, cls).__new__(cls, name, bases, attrs)
        map_name = '%s.%s' % (cls.__module__, cls.__name__)
        _MAP[map_name] = cls
        
        return cls


class _BaseElement(object):
    __metaclass__ = _RootElement
    
    def __init__(self, data=None):
        self.data_type = 'python'
        self.allow_undefined = True
        
        if not hasattr(self, '_immutable'):
            self._immutable = IMMUTABLE['vertex']

        self.fields = _Fields({
            GIZMO_MODEL     : String(get_qualified_instance_name(self)),
            GIZMO_CREATED   : DateTime(),
            GIZMO_MODIFIED  : DateTime(),
            GIZMO_NODE_TYPE : String(self._node_type),
            GIZMO_ID        : String(),
        })
            
        self.hydrate(data)
        
        if data is not None and GIZMO_ID in data:
            self.fields[GIZMO_ID].field_value = data[GIZMO_ID]
    
    def hydrate(self, data=None):
        if data is None:
            data = {}
        
        for field, value in data.iteritems():
            self[field] = value
        
        return self
    
    def _add_undefined_field(self, name, value):
        if type(value) is dict:
            field = Map(value, self.data_type)
        elif type(value) is list:
            field = List(value, self.data_type)
        else:
            field = String(value, self.data_type)
        
        self.fields[name] = field
        
        return field
    
    def __setitem__(self, name, value):
        if name not in self._immutable and name in self.fields:
            self.fields[name].field_value = value
        elif self.allow_undefined:
            self._add_undefined_field(name, value)
        
        return self
        
    def __getitem__(self, name):
        value = None

        if name in self.fields:
            value = self.fields[name].value
        elif self.allow_undefined:
            field = self._add_undefined_field(name, value)
            
        return value
    
    @property
    def _node_type(self):
        raise NotImplementedError('Vertices and Edges need a _node_type defined')
        
    @property
    def data(self):
        return self.fields.data
        
    def get_rep(self):
        element = 'e' if self._type == 'edge' else 'v'
        
        return element, self['_id']


class Vertex(_BaseElement):
    @property
    def _type(self):
        return 'vertex'


class General(Vertex):
    @property
    def _node_type(self):
        return 'General'


class Edge(_BaseElement):
    def __init__(self, data=None, label=None):
        if data is None:
            data = {}
            
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

        self._immutable = IMMUTABLE['edge']
        
        super(Edge, self).__init__(data)

        self.fields.update({
            GIZMO_LABEL : String(label),
        })
        
        if GIZMO_LABEL in data:
            self.fields[GIZMO_LABEL].field_value = data[GIZMO_LABEL]
            
        self.hydrate(data)

    @property
    def _type(self):
        return 'edge'

