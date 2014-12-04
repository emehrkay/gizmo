GIZMO_MODEL     = '__gizmo_model'
GIZMO_CREATED   = '__gizmo_created'
GIZMO_MODIFIED  = '__gizmo_modified'
GIZMO_NODE_TYPE = '__gizmo_node_type'
GIZMO_TYPE      = '_type'
GIZMO_ID        = '_id'
GIZMO_LABEL     = '_label'
VERTEX          = 'vertex'
EDGE            = 'edge'
TYPES           = {VERTEX: VERTEX, EDGE: EDGE}
IMMUTABLE       = {VERTEX: [GIZMO_ID, GIZMO_TYPE], EDGE: [GIZMO_ID, GIZMO_TYPE, GIZMO_LABEL]}


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

