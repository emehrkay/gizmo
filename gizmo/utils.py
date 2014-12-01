VERTEX = 'vertex'
EDGE = 'edge'
TYPES = {VERTEX: VERTEX, EDGE: EDGE}
IMMUTABLE = {
    VERTEX: ['_id', '_type'],
}

IMMUTABLE[EDGE] = IMMUTABLE[VERTEX].append('_label')

def get_object_items(obj):
    return [a for a in dir(obj) if not a.startswith('__') and not callable(getattr(obj, a))]


def get_qualified_name(obj):
    return '%s.%s' % (obj.__class__.__module__, obj.__class__.__name__)


def gizmo_import(name):
    parts = name.split('.')
    mod   = __import__(parts[0])
    
    for part in parts[1:]:
        mod = getattr(mod, part)

    return mod


def pylist_to_groovy(list):
    pass

    
def pydict_to_groovy(dict):
    pass

