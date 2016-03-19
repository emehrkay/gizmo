"""
Rexster datatypes are formatted based on what is defined here:
https://github.com/tinkerpop/rexster/wiki/Property-Data-Types
"""
from collections import OrderedDict
import json


class _Fields(dict):

    def __init__(self, *args, **kwargs):
        self.update(*args, **kwargs)
        self.data_type = 'python'
        self._initial_load = True
        self._spied = []

    def __getitem__(self, field):
        obj = dict.__getitem__(self, field)

        return obj

    def __setitem__(self, field, value):
        dict.__setitem__(self, field, value)

        if isinstance(value, Mirror):
            self._spied.append(value)

    def set(self, name, value, allow_undefined=False):
        if name in self:
            field = self[name]
            field.value = value
        elif allow_undefined:
            field = self._add_undefined_field(name, value)

        if len(self._spied):
            data = self.data

            for field in self:
                self[field].data = data

        return self

    def get(self, name, allow_undefined=False):
        value = None

        if name in self:
            value = self[name].value
        elif allow_undefined:
            value = self._add_undefined_field(name, None).value

        return value

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

        self[name] = field

        return field

    def update(self, *args, **kwargs):
        for field, obj in dict(*args, **kwargs).items():
            self[field] = obj

    def _get_data(self, data=None):
        if not data:
            data = {}

        for name, field in self.items():
            field.data_type = self.data_type
            data[name] = field.value

        return data

    def get_data(self):
        data = self._get_data()

        return OrderedDict(sorted(data.items()))

    data = property(get_data)

    @property
    def changed(self):
        changed = {}

        for name, field in self.items():
            if field.track_changes and field.changed():
                field.data_type = self.data_type
                changed[name] = field.value

        return OrderedDict(sorted(changed.items()))

    @property
    def unchanged(self):
        changed = {}

        for name, field in self.items():
            if field.track_changes and not field.changed():
                field.data_type = self.data_type
                changed[name] = field.value

        return OrderedDict(sorted(changed.items()))

    @property
    def removed(self):
        removed = []

        for name, field in self.items():
            if not field.value and field.track_changes and field.changed():
                removed.append(name)

        return removed


class Field(object):

    def __init__(self, value=None, data_type='python', set_max=None,
                 track_changes=True):
        if not value:
            value = self.default_value

        self._changes = [value]
        self._initial_value = value
        self.set_count = 0
        self.field_value = value
        self.data_type = data_type
        self.set_max = set_max
        self.value = value
        self.track_changes = track_changes

    @property
    def default_value(self):
        return None

    def changed(self):
        return self.initial_value != self.value

    @property
    def initial_value(self):
        if hasattr(self._initial_value, '__call__'):
            return self._initial_value()
        else:
            return self._initial_value

    def _get_value(self):
        if self.data_type == 'python':
            value = self.to_python()
        else:
            value = self.to_graph()

        return value

    def _set_value(self, value):
        if self._can_set(value):
            if hasattr(value, '__call__'):
                value = value()

            if value != self.field_value:
                self._changes.append(value)
            self.field_value = value

    def _del_value(self):
        self.field_value = None

    def _can_set(self, *args, **kwargs):
        can_set = True

        if self.set_max is not None:
            can_set = self.set_count <= self.set_max

        self.set_count += 1

        return can_set

    value = property(_get_value, _set_value, _del_value)

    def to_python(self):
        return self.field_value

    def to_graph(self):
        return '' if self.to_python() is None else self.to_python()


class String(Field):

    def to_graph(self):
        return '' if self.field_value is None else str(self.field_value)


class Integer(Field):

    def to_python(self):
        try:
            return int(float(self.field_value)) if self.field_value else 0
        except:
            return 0

    def to_graph(self):
        return self.to_python()


class Increment(Integer):

    def to_graph(self):
        val = self.field_value if self.field_value else 0
        self.field_value = int(val) + 1

        return self.field_value


class Float(Field):

    def to_python(self):
        try:
            return float(self.field_value) if self.field_value else 0.0
        except:
            return 0.0

    def to_graph(self):
        return self.to_python()


class Boolean(Field):

    def __init__(self, value=None, data_type='python', set_max=None,
                 track_changes=True):
        super(Boolean, self).__init__(value=value, data_type=data_type,
                                      set_max=set_max,
                                      track_changes=track_changes)

        if not value:
            value = False

        if str(value).lower().strip() not in ['true', 'false',]:
            value = bool(value)

        value = str(value).lower().strip()
        self.field_value = bool(json.loads(value))

    def to_python(self):
        try:
            value = str(self.field_value).lower().strip()
            return bool(json.loads(value))
        except:
            return False

    def to_graph(self):
        return 'true' if self.field_value else 'false'


class Map(Field):

    @property
    def default_value(self):
        return {}

    def to_python(self):
        if (isinstance(self.field_value, str) and
            len(self.field_value.replace(" ", ""))):
            return json.load(self.field_value)
        else:
            return self.field_value


class List(Map):

    @property
    def default_value(self):
        return []


class DateTime(Field):

    @property
    def default_value(self):
        from gizmo.utils import current_date_time
        return current_date_time

    def to_graph(self):
        value = '' if not self.field_value else int(float(self.field_value))

        return value

    def to_python(self):
        if self.initial_value != self.field_value:
            value = 0 if not self.field_value else self.field_value
            value = int(float(value))
        else:
            value = self.field_value

        return value


class Timestamp(DateTime):
    pass


class Enum(Field):

    def __init__(self, allowed, value=None, data_type='python', set_max=None,
                 track_changes=True):
        if allowed is None:
            allowed = []

        self.allowed = allowed

        if value is None or value not in self.allowed:
            value = self.allowed[0]

        super(Enum, self).__init__(value=value, data_type=data_type,
                                   set_max=set_max,
                                   track_changes=track_changes)

    def _can_set(self, value):
        if super(Enum, self)._can_set(value) and value in self.allowed:
            self.field_value = value


class Mirror(Field):
    data = {}
    fields = []

    def to_python(self):
        fields = [self.data[field] for field in self.fields
                  if field in self.data and self.data[field]]

        return self.values(fields)

    def __init__(self, value=None, data_type='python', set_max=None,
                 track_changes=True, fields=None, callback=None):
        if not isinstance(fields, (list, tuple,)):
            raise ValueError('fields must be a list')

        if not fields:
            fields = []

        self.fields = fields

        if callback and not hasattr(callback, '__call__'):
            raise ValueError('callback must be a callable function')
        elif not callback:
            callback = self._callback()

        self.callback = callback

        super(Mirror, self).__init__(value=value, data_type=data_type,
                                     set_max=set_max,
                                     track_changes=track_changes)

    def _callback(self):
        return lambda values: values


class StringMirror(Mirror, String):

    def values(self, fields):
        return self.callback([str(value) for value in fields if value])

    def _callback(self):
        return lambda values: ', '.join(values)


class MapMirror(Mirror, Map):

    def to_python(self):
        return self.values(self.data)

    def values(self, fields):
        return self.callback(fields)


class ListMirror(Mirror, List):

    def values(self, fields):
        return self.callback(fields)

