from collections import OrderedDict


class _Fields(dict):
    data_type = 'python'
    
    def __init__(self, *args, **kwargs):
        self.update(*args, **kwargs)
    
    def __getitem__(self, field):
        obj = dict.__getitem__(self, field)
        
        return obj
    
    def __setitem__(self, field, value):
        dict.__setitem__(self, field, value)
    
    def update(self, *args, **kwargs):
        for field, obj in dict(*args, **kwargs).iteritems():
            self[field] = obj
    
    @property
    def data(self):
        data = {}
        
        for name, field in self.iteritems():
            field.data_type = self.data_type
            data[name] = field.value
        
        # sorting field names do not matter
        # this is done for testing purposes
        return OrderedDict(sorted(data.items()))


class Field(object):
    data_type = 'python'
    
    def __init__(self, value=None, data_type='python'):
        self.field_value = value
        self.data_type   = data_type

    @property
    def value(self):
        if self.data_type == 'python':
            value = self.to_python()
        else:
            value = self.to_graph()
            
        return value

    def to_python(self):
        return self.field_value
        
    def to_graph(self):
        return '' if self.field_value is None else self.field_value


class String(Field):
    pass

class Integer(Field):
    pass


class Float(Field):
    pass


class Boolean(Field):
    pass


class Map(Field):
    pass


class List(Field):
    pass


class DateTime(Field):
    pass


class Enum(Field):
    def __init__(self, value=None, allwed = None, data_type='python'):
        if allowed is None:
            allowed = []
            
        self.allowed = allowed
        
        if value is None:
            value = self.allowed[0]
        
        super(Enum, self).__init__(value, data_type)
