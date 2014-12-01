from field import String, DateTime, Boolean, List, Map, _Fields
from utils import get_qualified_name, TYPES, IMMUTABLE

GIZMO_MODEL     = '__gizmo_model'
GIZMO_CREATED   = '__gizmo_created'
GIZMO_MODIFIED  = '__gizmo_modified'
GIZMO_NODE_TYPE = '__gizmo_node_type'
GIZMO_TYPE      = '_type'
GIZMO_ID        = '_id'
GIZMO_LABEL     = '_label'


class _RootElement(type):
    def __new__(cls, name, bases, attrs):
        old = None
        
        if '__init__' in attrs:
            old = attrs['__init__']
            
        #ensure a unique _Fields object is set with each instance
        def __init_wraper(self, data):
            self.data_type = 'python'
            self.allow_undefined = True
            self._immutable = IMMUTABLE['vertex']
            self.fields = _Fields({
                GIZMO_MODEL     : String(get_qualified_name(self)),
                GIZMO_CREATED   : DateTime(),
                GIZMO_MODIFIED  : DateTime(),
                GIZMO_NODE_TYPE : String(self._node_type),
                GIZMO_TYPE      : String(self._type),
                GIZMO_ID        : String(),
            })
            
            if old is not None:
                old(self, data)

            self.hydrate(data)
        
            if data is not None and GIZMO_ID in data:
                self.fields[GIZMO_ID].field_value = data[GIZMO_ID]
            
        attrs['__init__'] = __init_wraper
        
        return super(_RootElement, cls).__new__(cls, name, bases, attrs)


class _BaseElement(object):
    __metaclass__ = _RootElement
    
    def hydrate(self, data=None):
        if data is None:
            data = {}
        
        for field, value in data.iteritems():
            if field in self.fields:
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


class Vertex(_BaseElement):
    @property
    def _type(self):
        return 'vertex'

class General(Vertex):
    @property
    def node_type(self):
        return 'General'


class Edge(_BaseElement):
    def __init__(self, data=None):
        self._immutable = IMMUTABLE['edge']
        
        self.fields.append({
            GIZMO_LABEL : String(self._label),
        })
        
        if GIZMO_LABEL in data:
            self.fields[GIZMO_LABEL].field_value = data[GIZMO_LABEL]

        super(Edge, self).__init__(self, data)

    @property
    def _type(self):
        return 'edge'
        
    @property
    def _label(self):
        raise NotImplementedError('Edges need a _label defined')
        
    def set_out(self, out_v):
        self._out_v = out_v
    
    def set_in(self, in_v):
        self._in_v = in_v


class Collection(dict):
    def __init__(self, *args, **kwargs):
        self.update(*args, **kwargs)
        self.elements = []
    
    def __getitem__(self, field):
        data = dict.__getitem__(self, field)
        
        return obj
    
    def __setitem__(self, field, value):
        dict.__setitem__(self, field, value)
    
    def update(self, *args, **kwargs):
        for field, obj in dict(*args, **kwargs).iteritems():
            self[field] = obj
