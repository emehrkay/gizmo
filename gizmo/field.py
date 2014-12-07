from collections import OrderedDict


class _Fields(dict):
    def __init__(self, *args, **kwargs):
        self.update(*args, **kwargs)
        self.data_type = 'python'
    
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
    def __init__(self, value=None, data_type='python', set_max=None):
        self.field_value = value
        self.data_type   = data_type
        self.set_count   = 0
        self.set_max     = set_max
        self.value       = value

    def _get_value(self):
        if self.data_type == 'python':
            value = self.to_python()
        else:
            value = self.to_graph()
            
        return value
    
    def _set_value(self, value):
        if self._can_set():
            if hasattr(value, '__call__'):
                value = value()
            
            self.field_value = value
            
    def _del_value(self):
        self.field_value = None

    def _can_set(self):
        can_set = True

        if self.set_max is not None:
            can_set = self.set_count <= self.set_max
            self.set_count += 1
        
        return can_set

    value = property(_get_value, _set_value, _del_value)

    def to_python(self):
        return self.field_value
        
    def to_graph(self):
        return '' if self.field_value is None else self.field_value


class String(Field):
    pass

class Integer(Field):
    def to_python(self):
        return int(self.field_value)


class Float(Field):
    pass


class Boolean(Field):
    def to_python(self):
        return bool(self.field_value)


class Map(Field):
    pass


class List(Field):
    pass


class DateTime(Field):
    def to_graph(self):
        return int(self.field_value)
    
    def to_python(self):
        return int(self.field_value) / 1000


class TimeStamp(DateTime):
    pass


class Enum(Field):
    def __init__(self, allowed, value, data_type='python', set_max=None):
        if allowed is None:
            allowed = []
            
        self.allowed = allowed
        
        if value is None:
            value = self.allowed[0]
        
        super(Enum, self).__init__(value=value, data_type=data_type, set_max=set_max)
    
    def _set_value(self, value):
        if self._can_set() and value in self.allowed:
            self.value = value

