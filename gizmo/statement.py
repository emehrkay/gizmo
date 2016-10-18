from gremlinpy import Statement, Gremlin, Param
from gremlinpy.statement import GetEdge

from .mapper import Query, next_param
from .util import GIZMO_LABEL


class MapperStatement(Statement):
    """Any sub-classed Statement will be applied to an empty Gremlin instance.
    The statement is given the current gizmo.mapper.Query instance for the
    entity that is currently being evaluated."""

    def __init__(self, entity, mapper, query, **kwargs):
        self.entity = entity
        self.mapper = mapper
        self.query = query


class MapperUniqueVertex(MapperStatement):
    """statement used to build a gremlin query that will check for the
    existence of a entity before adding the entity

    example query built:

        g.V().has("field", value).tryNext().orElseGet{
            g.addV("field", value).next()
        }

    """

    def build(self):
        gremlin = self.gremlin
        gremlin.V()
        save_query = self.query.queries[0]
        name = str(self.entity)

        for field in self.mapper.unique_fields:
            holder = '{}_{}'.format(name, field)
            g_field = next_param(holder, field)
            param = next_param(holder + '_value',
                self.entity[field].value)

            gremlin.has(g_field, param)

        gremlin.tryNext().orElseGet.close(save_query['script'])


class MapperUniqueEdge(MapperStatement):
    """Statement used to build a gremlin query that will check for the
    existence of an edge between two entities before creating the edge.

    example query built:
    """

    def __init__(self, entity, mapper, query, out_v_id, in_v_id, label,
                 direction):
        super(MapperUniqueEdge, self).__init__(entity=entity, mapper=mapper,
                                               query=query)
        self.out_v_id = out_v_id
        self.in_v_id = in_v_id
        self.label = label
        self.direction = direction

    def build(self):
        save_query = self.query.queries[0]
        edge = GetEdge(self.out_v_id, self.in_v_id, self.label,
                       self.mapper.unique)
        gremlin = self.gremlin

        gremlin.apply_statement(edge)
        gremlin.tryNext().orElseGet.close(save_query['script'])
