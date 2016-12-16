import re
import time


GIZMO_ID = 'id'
GIZMO_LABEL = ('label', 'T.label')
GIZMO_TYPE = 'type'
GIZMO_ENTITY = '__GIZMO_ENTITY__'
GIZMO_VARIABLE = 'gizmo_var'


def camel_to_underscore(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)

    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def entity_name(entity):
    if isinstance(entity, type):
        return '{}.{}'.format(entity.__module__, entity.__name__)
    else:
        return '{}.{}'.format(entity.__class__.__module__,
            entity.__class__.__name__)


def is_gremlin_entity(data):
    """utility method used to check to see if a value from a gremlin response
    is supposed to be an entity or not
    """
    try:
        if not isinstance(data, (list, tuple, dict)):
            return False

        if isinstance(data, dict):
            data = [data, ]

        if isinstance(data, (list, tuple)) and len(data) and\
            'value' in data[0]:
            return True
    except:
        return False

    return False


def _query_debug(script, params): # pragma: no cover
    if not len(params):
        return script

    pattern = re.compile(r'\b(' + '|'.join(params.keys()) + r')\b')

    def su(x):
        x = str(params[x.group()]) if params[x.group()] else ''
        return "'%s'" % x

    return pattern.sub(su, script)


def current_date_time(offset=0): # pragma: no cover
    return (int(time.time()) + offset)
