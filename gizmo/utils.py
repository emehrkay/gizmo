import time
import re


GIZMO_MODEL = 'gizmo_model'
GIZMO_CREATED = 'gizmo_created'
GIZMO_MODIFIED = 'gizmo_modified'
GIZMO_ID = '_id'
GIZMO_LABEL = '_label'
GIZMO_TYPE = '_type'
VERTEX = 'vertex'
EDGE = 'edge'
TYPES = {VERTEX: VERTEX, EDGE: EDGE}
IMMUTABLE = {
    VERTEX: [GIZMO_ID],
    EDGE: [GIZMO_ID, GIZMO_LABEL, '_inV', '_outV']}


def camel_to_underscore(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def get_object_items(obj):
    return [a for a in dir(obj) if not a.startswith('__') and
            not callable(getattr(obj, a))]


def get_qualified_name(obj):
    return '%s.%s' % (obj.__module__, obj.__name__)


def get_qualified_instance_name(obj):
    return '%s.%s' % (obj.__class__.__module__, obj.__class__.__name__)


def current_date():
    pass


def current_time():
    pass


def current_date_time(offset=0):
    return (int(time.time()) + offset) * 1000


def get_logger(address='/var/run/syslog'):
    import logging
    from logging.handlers import SysLogHandler

    logger = logging.getLogger('gizmo_logger')
    handler = SysLogHandler(address=address)

    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    return logger
