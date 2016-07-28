import time
import re


GIZMO_VARIABLE = 'gizmo_var'
GIZMO_MODEL = 'gizmo_entity'
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


def blocking(callback, *args, **kwargs):
    from tornado.ioloop import IOLoop
    from tornado import gen

    ioloop = IOLoop(make_current=False)
    r = {'response': ''}

    async def run():
        fut = callback(*args, **kwargs)
        r['response'] = await gen.maybe_future(fut)
    resp = ioloop.run_sync(run)
    return r['response']


def _query_debug(script, params):
    if not len(params):
        return script

    pattern = re.compile(r'\b(' + '|'.join(params.keys()) + r')\b')

    def su(x):
        x = str(params[x.group()]) if params[x.group()] else ''
        return "'%s'" % x

    return pattern.sub(su, script)


def camel_to_underscore(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def get_object_items(obj):
    return [a for a in dir(obj) if not a.startswith('__') and
            not callable(getattr(obj, a))]


def get_entity_name(entity):
    from .mapper import _BaseEntity

    if isinstance(entity, _BaseEntity):
        return get_qualified_instance_name(entity)
    else:
        return get_qualified_name(entity)


def get_qualified_name(obj):
    return '%s.%s' % (obj.__module__, obj.__name__)


def get_qualified_instance_name(obj):
    return '%s.%s' % (obj.__class__.__module__, obj.__class__.__name__)


def current_date():
    pass


def current_time():
    pass


def current_date_time(offset=0):
    return (int(time.time()) + offset)


def get_logger(address='/var/run/syslog'):
    import logging
    from logging.handlers import SysLogHandler

    logger = logging.getLogger('gizmo_logger')
    handler = SysLogHandler(address=address)

    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    return logger
