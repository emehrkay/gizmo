

class GizmoException(Exception):

    def __init__(self, errors=None):

        if not errors:
            errors = []
        self.errors = errors


class EntityException(GizmoException):
    pass


class VertexException(EntityException):
    pass


class EdgeException(EntityException):
    pass


class MapperException(GizmoException):
    pass


class ModelException(GizmoException):
    pass


class QueryException(MapperException):
    pass


class CollectionException(MapperException):
    pass


class TraversalException(MapperException):
    pass
