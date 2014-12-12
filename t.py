from gizmo.entity import Vertex
from gizmo.request import BinaryRequest
from gizmo.mapper import Mapper
from gremlinpy import Gremlin

class V(Vertex):
    @property
    def _node_type(self):
        return 'V'

v = V()
v['__gizmo_node_type'] = 'lllllllllll'


REQUEST = Binary('localhost', 'komm', port=8984)
GREMLIN = Gremlin()
MAPPER = Mapper(REQUEST, GREMLIN, auto_commit=True)


print v.fields.data, v['_type']