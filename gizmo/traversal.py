from gremlinpy import Gremlin

from .util import camel_to_underscore


class Traversal(Gremlin):
    """
    class used to start a traversal query based on a given entity
    when the class is created, the entity's _id and type are are
    set on the Gremlin object
    example:
    """

    def __init__(self, mapper, entity):
        self._mapper = mapper
        self._entity = entity
        self._collection = None

    def set_mapper(self, mapper):
        if mapper:
            graph_variable = mapper.gremlin.gv

            super(Traversal, self).__init__(graph_variable)

        self.__mapper = mapper

    def get_mapper(self):
        return self.__mapper

    _mapper = property(get_mapper, set_mapper)

    def get_entity(self):
        return self.__entity

    def set_entity(self, entity):
        self.__entity = entity

        self._build_initial_query()

    _entity = property(get_entity, set_entity)

    def _build_initial_query(self):
        from .entity import _Entity
        from .mapper import next_param

        if not self._entity:
            return False

        entity = self._entity
        _id = None
        _base = isinstance(entity, _Entity)

        if _base:
            ev, _id = entity.get_rep()

        if _id:
            bound_id = next_param('{}_EYE_DEE'.format(str(entity)), _id)

            getattr(self, ev)(bound_id)
        else:
            if _base:
                _type = entity.__class__.__name__
            else:
                _type = entity.__name__
                ev, _ = entity().get_rep()

            _type = camel_to_underscore(_type)
            bound_type = self.bind_param(_type, 'BOUND_TYPE')

            getattr(self, ev)().hasLabel(bound_type[0])

        return self

    async def __aiter__(self):
        return self

    async def __anext__(self):
        await self.to_collection()

        try:
            return next(self._collection)
        except:
            self._collection = None

            raise StopAsyncIteration()

    async def to_collection(self):
        if not self._collection:
            self._collection = await self._mapper.query(gremlin=self)

        return self._collection
