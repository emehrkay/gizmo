
class GizmoException(Exception):
    pass


class EntityException(GizmoException):
    pass


class VertexException(EntityException):
    pass


class EdgeException(EntityException):
    pass


class MapperException(GizmoException):
    pass


class QueryException(MapperException):
    pass


class CollectionException(MapperException):
    pass


class TraversalException(MapperException):
    pass

