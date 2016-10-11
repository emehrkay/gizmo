import copy
import json

from collections import OrderedDict
from datetime import datetime

from .util import is_gremlin_entity


class FieldManager:

    def __init__(self, fields=None, data_type='python', allow_undefined=False):
        self.fields = fields or {}
        self._data_type = data_type
        self.allow_undefined = allow_undefined

        for name, field in self.fields.items():
            if not field.name:
                field.name = name

    def _set_data_type(self, data_type):
        for name, field in self.fields.items():
            field.data_type = data_type

    def _get_data_type(self):
        return self._data_type

    data_type = property(_get_data_type, _set_data_type)

    def __getitem__(self, name):
        value = None

        if name in self.fields:
            value = self.fields[name]

            if not value.name:
                value.name = name

            if isinstance(value, _ImmutableField):
                return value.data
        elif self.allow_undefined:
            value = self._add_undefined_field(name, None)

        return value

    def __setitem__(self, name, value):
        if name in self.fields:
            if isinstance(value, Field):
                self.fields[name] = value
            else:
                field = self.fields[name].empty()
                field + value
        elif self.allow_undefined:
            field = self._add_undefined_field(name, value)

        if not self.fields[name].name:
            self.fields[name].name = name

        return self.fields[name]

    def __delitem__(self, name):
        self.fields[name].deleted = True

    def _add_undefined_field(self, name, value):
        max_values = None
        overwrite_last_value = False
        gremlin_entity = is_gremlin_entity(value)
        original_value = copy.deepcopy(value)

        if gremlin_entity:
            value = value[0]['value']

        if isinstance(value, dict):
            field = Map
            max_values = 1
            overwrite_last_value = True
        elif isinstance(value, (list, tuple,)):
            field = List
            max_values = 1
            overwrite_last_value = True
        elif isinstance(value, bool):
            field = Boolean
            max_values = 1
            overwrite_last_value = True
        elif isinstance(value, int):
            field = Integer
        elif isinstance(value, float):
            field = Float
        else:
            field = String

        """the field will not be initialized with a value so that it
        will register as 'added'. We also need to determine if the value is
        a response from the Gremlin server or if it is a plain value
        """
        f = field(name=name, data_type=self.data_type, max_values=max_values,
                  overwrite_last_value=overwrite_last_value)

        # if isinstance(value, (list, tuple)) and len(value) and \
        #     isinstance(value[0], dict) and 'value' in value[0]:
        if gremlin_entity:
            for val in original_value:
                val_obj = Value(value=val.get('value', None),
                    properties=val.get('properties', None), id=val.get('id', None))
                f + val_obj
        else:
            f + value

        self.fields[name] = f

        return self.fields[name]

    def hydrate(self, data, reset_initial=False):
        for key, val in data.items():
            if key in self.fields:
                if is_gremlin_entity(val):
                    for v in val:
                        self.fields[key] + Value(value=v.get('value', None),
                            properties=v.get('properties', None),
                            id=v.get('id', None))
                else:
                    self.fields[key] + val
            elif self.allow_undefined:
                self._add_undefined_field(key, val)

        return self

    def empty(self):
        for name, field in self.fields.items():
            field.empty()

        return self

    @property
    def data(self):
        data = {field.name: field.data for name, field in self.fields.items()
                    if field.data}

        return OrderedDict(sorted(data.items()))

    @property
    def values(self):
        values = {field.name: field.values for name, field in self.fields.items()
                    if field.values}

        return OrderedDict(sorted(values.items()))

    @property
    def changes(self):
        changes = {field.name: field.changes for name, field in self.fields.items()
                    if field.changes}

        return OrderedDict(sorted(changes.items()))

    @property
    def changed(self):
        return [name for name, changes in self.changes.items() if
            changes['deleted'] or changes['values']]

    @property
    def deleted(self):
        return [name for name, changes in self.changes.items() if
            changes['deleted']]


class Field:

    def __init__(self, name=None, values=None, data_type='python', max_values=None,
                 overwrite_last_value=False):
        self.name = name
        self.immutable = isinstance(self, _ImmutableField)
        self._values = ValueManager(values=values, data_type=data_type,
                                    to_python=self.to_python,
                                    to_graph=self.to_graph,
                                    default_value=self.default_value,
                                    max_values=max_values,
                                    overwrite_last_value=overwrite_last_value,
                                    can_set=self.can_set)
        self._data_type = data_type
        self.deleted = False

    def __getitem__(self, value):
        return self._values[value]

    def __setitem__(self, key, value):
        val = self[key]
        val[key] = value

    def __delitem__(self, value):
        if self.can_set(value):
            del self._values[value]

    def __add__(self, value):
        if self.can_set(value):
            return self._values + value

    def _set_data_type(self, data_type):
        self._values.data_type = data_type

    def _get_data_type(self):
        return self._data_type

    data_type = property(_get_data_type, _set_data_type)

    @property
    def values(self):
        return self._values.values

    @property
    def value(self):
        values = self.values

        return values[-1] if values else None

    @property
    def data(self):
        return self._values.data

    @property
    def properties(self):
        return self._values.properties

    @property
    def changes(self):
        return {
            'values': self._values.changes,
            'deleted': self.deleted,
            'immutable': self.immutable,
        }

    @property
    def default_value(self):
        return None

    def empty(self):
        self._values.empty()

        return self

    def to_python(self, value):
        return value._value

    def to_graph(self, value):
        return self.to_python(value)

    def can_set(self, value):
        return True


class ValueManager:

    def __init__(self, values=None, data_type='python', reset_initial=True,
                 to_python=None, to_graph=None, filter_field=None,
                 default_value=None, max_values=None, overwrite_last_value=False,
                 can_set=None):
        self._values = []
        self._deleted = []
        self._data_type = data_type
        self.max_values = max_values
        self.overwrite_last_value = overwrite_last_value

        if not values and default_value is not None:
            values = [default_value,]

        if not to_python:
            to_python = lambda x: x

        if not to_graph:
            to_graph = lambda x: x

        if not can_set:
            can_set = lambda x: True

        self._to_python = to_python
        self._to_graph = to_graph
        self._can_set = can_set
        self.filter_field = filter_field

        self.hydrate(values=values, reset_initial=reset_initial)

        self.data_type = self._data_type

    def hydrate(self, values=None, reset_initial=False):
        if values:
            if not isinstance(values, (list, tuple,)):
                values = (values,)

            for value in values:
                if not isinstance(value, dict):
                    value = {'value': value}

                properties = value.get('properties', None)
                value = value.get('value', None)

                self.add_value(value=value, properties=properties)

        if reset_initial:
            self._initial = self._values[:]

        return self

    def __add__(self, value):
        if isinstance(value, Value):
            if not self._can_set(value.value):
                return self

            self.add_value(value.value, value.properties, value.id)

            return self
        else:
            if not self._can_set(value):
                return self

            return self.add_value(value, properties=None)

    def __len__(self):
        return len(self.filtered_values)

    def __getitem__(self, value):
        if value not in self.values:
            self + value

        manager = ValueManager(data_type=self.data_type,
                               to_python=self._to_python,
                               to_graph=self._to_graph,
                               filter_field=value,
                               max_values=self.max_values,
                               overwrite_last_value=self.overwrite_last_value)
        manager._values = self._values

        return manager

    def __setitem__(self, key, value):
        if not self._can_set(value):
            return self

        for val in self.filtered_values:
            if val.value == key:
                val.value = value

    def __delitem__(self, value):
        for i, val in enumerate(self._values):
            if val.value == value:
                deleted = self._values.pop(i)
                self._deleted.append(deleted)

    def _set_data_type(self, data_type):
        converter = self._to_python if data_type == 'python'\
            else self._to_graph

        for value in self._values:
            value.converter = converter

    def _get_data_type(self):
        return self._data_type

    data_type = property(_get_data_type, _set_data_type)

    def empty(self):
        for value in self.values:
            del self[value]

        return self

    def add_value(self, value, properties=None, id=None):
        if not self._can_set(value):
            return self

        val = Value(value=value, properties=properties, id=id)
        x = self.max_values
        l = len(self._values)

        if self.max_values and len(self._values) >= self.max_values:
            if self.overwrite_last_value and len(self._values) == self.max_values:
                self._values[-1] = val
            elif len(self._values) < self.max_values:
                self._values.append(val)
        else:
            self._values.append(val)

        return self

    @property
    def properties(self):
        properties = [v.properties for v in self.filtered_values]

        return PropertyManager(properties=properties)

    @property
    def filtered_values(self):
        if self.filter_field:
            return [v for v in self._values
                if self.filter_field == v.value]
        else:
            return [v for v in self._values]

    @property
    def values(self):
        return [v.value for v in self.filtered_values]

    @property
    def data(self):
        return [v.data for v in self.filtered_values]

    @property
    def changes(self):
        changed = {'values': self.values}
        changes = []
        added = []

        for v in self.filtered_values:
            if v not in self._initial:
                added.append(v.data)

        if added:
            changed['added'] = added

        for v in self.filtered_values:
            if v not in added:
                v_changes = v.changes

                if v_changes:
                    changes.append(v_changes)

        if changes:
            changed['changes'] = changes

        if self._deleted:
            changed['deleted'] = [v.data for v in self._deleted]

        return changed

    @property
    def default_value(self):
        return None


class Value:

    def __init__(self, value, properties=None, id=None):
        self._value = value
        self._callable = callable(value)
        self._initial = copy.deepcopy(value)
        self.id = id
        self.properties = properties or {}
        self.converter = lambda value: value._value

    def __setitem__(self, key, value):
        self._properties[key] = value

    def __getitem__(self, key):
        return self._properties.get(key, None)

    def __getattribute__(self, attr):
        val = object.__getattribute__(self, attr)

        if attr == '_value' and self._callable:
            return val()
        else:
            return val

    def get_value(self):
        return self.converter(self)

    def set_value(self, value):
        self._callable = callable(value)
        self._value = value

    value = property(get_value, set_value)

    @property
    def data(self):
        return {
            'value': self.converter(self),
            'properties': self.properties,
        }

    @property
    def changes(self):
        changes = {}
        properties = self.properties

        if self.value != self._initial:
            changes['value'] = {'from': self._initial, 'to': self.value}

        if properties:
            if 'value' not in changes:
                changes['value'] = self._initial

            changes['properties'] = properties

        return changes


class PropertyManager:

    def __init__(self, properties):
        self.properties = properties

    def __setitem__(self, key, val):
        for prop in self.properties:
            prop[key] = val

        return val

    def __getitem__(self, key):
        return [prop[key] for prop in self.properties if key in prop]

    def __delitem__(self, key):
        for prop in self.properties:
            if key in prop:
                del prop[key]

    @property
    def data(self):
        return [prop for prop in self.properties if prop]


class Property:

    def __init__(self, properties=None):
        self._properties = properties or {}
        self._initial = copy.deepcopy(self._properties)

    def __getitem__(self, key):
        return self._properties[key]

    def __setitem__(self, key, value):
        self._properties[key] = value

    def __delitem__(self, key):
        del self._properties[key]

    def __contains__(self, key):
        return key in self._properties

    @property
    def data(self):
        return self._properties

    @property
    def changes(self):
        added = {}
        deleted = {}
        changed = {}
        changes = []

        for k, v in self._properties.items():
            if k not in self._initial:
                added[k] = v

        for k, v in self._properties.items():
            if k not in added and k in self._initial and self._initial[k] != v:
                changes.append({
                    'from': self._initial[k],
                    'to': v,
                    'key': k,
                })

        for k, v in self._initial.items():
            if k not in self._properties:
                deleted[k] = v

        if added:
            changed['added'] = added

        if changes:
            changed['changes'] = changes

        if deleted:
            changed['deleted'] = deleted

        return changed


class _ImmutableField:
    default = ''

    @property
    def values(self):
        return self.data

    @property
    def data(self):
        try:
            return self.to_python(self._values._values[-1])
        except:
            return self.default


class String(Field):

    def to_python(self, value):
        try:
            return str(value._value)
        except:
            return ''


class Integer(Field):

    def to_python(self, value):
        try:
            return int(float(value._value))
        except:
            return 0


class Increment(Integer):

    @property
    def default_value(self):
        return 0

    def to_graph(self, value):
        value.value = value._value + 1

        return value._value


class Float(Field):

    def to_python(self, value):
        try:
            return float(value._value)
        except:
            return 0.0


class Boolean(Field):

    @property
    def default_value(self):
        return False

    def _convert(self, value):
        if str(value).lower().strip() not in ['true', 'false',]:
            value = bool(value)

        value = str(value).lower().strip()

        return bool(json.loads(value))

    def to_python(self, value):
        try:
            val = self._convert(value._value)
            val = str(val).lower().strip()

            return bool(json.loads(val))
        except:
            return False

    def to_graph(self, value):
        val = self._convert(value._value)

        return 'true' if val else 'false'


class Map(Field):

    def __init__(self, name=None, values=None, data_type='python', *args, **kwargs):
        if isinstance(self, Map) and isinstance(values, dict) \
            and 'value' not in values:
            values = {'value': values}
        elif isinstance(self, List) and isinstance(values, (list, tuple,))\
            and (len(values) and not isinstance(values[0], dict)):
            values = {'value': values}

        super().__init__(name=name, values=values, data_type=data_type, *args,
                         **kwargs)

    @property
    def default_value(self):
        return {'value': {}}

    def to_python(self, value):
        if (isinstance(value._value, str) and
            len(value._value.replace(' ', ''))):
            return json.loads(value._value)
        else:
            return value._value


class List(Map):

    @property
    def default_value(self):
        return {'value': []}

    @property
    def default_value(self):
        return []


class Option(Field):

    def __init__(self, options, name=None, values=None, data_type='python',
                 *args, **kwargs):
        self.options = options

        super().__init__(name=name, values=values, data_type=data_type, *args,
                         **kwargs)

    def can_set(self, value):
        if isinstance(value, Value):
            value = value.value

        return value in self.options


class DateTime(Float):

    def to_python(self, value):
        val = value._value

        if isinstance(val, datetime):
            val = val.timestamp()

        v = Value(value=val, properties=value.properties, id=value.id)

        return super().to_python(value=v)


class TimeStamp(DateTime):

    def __init__(self, name=None, values=None, data_type='python'):

        def default():
            return datetime.now()

        values = values or default

        super().__init__(name=name, values=values, data_type=data_type, max_values=1,
                         overwrite_last_value=False)


class GremlinID(_ImmutableField, String):
    pass


class GremlinLabel(GremlinID):
    pass


class GremlinType(GremlinLabel):
    pass


class GIZMOEntity(GremlinID):
    pass
