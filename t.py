from gizmo.element import Vertex
from gizmo.request import BinaryRequest
from gizmo.mapper import Mapper
from gremlinpy import Gremlin

class V(Vertex):
    @property
    def _node_type(self):
        return 'V'

v = V()
v['__gizmo_node_type'] = 'lllllllllll'

r = BinaryRequest('localhost', 8984, 'komm')
g = Gremlin()
m = Mapper(r, g)

print v.fields.data, v['_type']