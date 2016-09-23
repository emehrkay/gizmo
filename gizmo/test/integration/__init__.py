"""This file holds all of the test cases for the integration tests
"""
import random

from gizmo import Mapper, Request, Collection, Vertex, Edge
from gizmo.mapper import EntityMapper
from gizmo.util import GIZMO_ENTITY, GIZMO_ID, GIZMO_LABEL


class ConnectionTestCases(object):

    def test_can_establish_mapper(self):

        async def test():
            await self.purge()
            c = '%s.V()' % self.gremlin.gv
            r = await self.mapper.query(script=c)

            self.assertEqual(0, len(r))

        self.ioloop.run_until_complete(test())

    def test_can_send_request_and_retrieve_collection_objec(self):

        async def test():
            script = 'a = 1'
            r = await self.mapper.query(script=script)

            self.assertIsInstance(r, Collection)
            self.assertIsInstance(r[0], Vertex)

        self.ioloop.run_until_complete(test())

    def test_can_get_database_time(self):

        async def test():
            script = 'def x = new Date(); x'
            r = await self.mapper.query(script=script)

            self.assertTrue(r[0]['response'] != '')
            self.assertIsInstance(r[0]['response'].values[0], int)

        self.ioloop.run_until_complete(test())

    def test_can_send_math_equation_to_server_and_retrieve_genderic_vertex_with_respnose_to_result(self):

        async def test():
            script = 'b = 1 + 1;'
            r = await self.mapper.query(script=script)
            r1 = r[0]

            self.assertIsInstance(r1, Vertex)
            self.assertIn('response', r1.data)
            self.assertEqual(2, r1['response'].values[0])

        self.ioloop.run_until_complete(test())


class EntityTestCases(object):

    def entity_save_assertions(self, entity):

        empty = ['', 0, None]
        non_zero = ['', None]
        fields = [GIZMO_ENTITY, GIZMO_ID, GIZMO_LABEL[0]]

        for f in fields:
            if f is GIZMO_ID:
                check = non_zero
            else:
                check = empty

            self.assertIsNotNone(entity[f])
            self.assertNotIn(entity[f], check, msg='{} is `{}`'.format(f, entity[f]))

    def test_can_save_generic_vertex_and_update_itsid(self):

        async def test():
            data = {'name': 'mark', 'sex': 'male'}
            v = self.mapper.create(data=data)

            self.mapper.save(v)

            res = await self.mapper.send()
            self.entity_save_assertions(v)

        self.ioloop.run_until_complete(test())

    def test_can_save_generic_vertex_and_get_response_entity_withid(self):

        async def test():
            data = {'name': 'mark', 'sex': 'male'}
            v = self.mapper.create(data=data)
            self.mapper.save(v)
            r = await self.mapper.send()
            v1 = r.first()

            self.entity_save_assertions(v1)

        self.ioloop.run_until_complete(test())

    def test_can_save_defined_vertex_and_update_itsid(self):
        class TestVertex(Vertex):
            allow_undefined = True

        data = {'name': 'mark', 'sex': 'male'}
        v = self.mapper.create(data=data, entity=TestVertex)

        async def test():
            await self.mapper.save(v).send()
            self.entity_save_assertions(v)

        self.ioloop.run_until_complete(test())

    def test_can_save_defined_vertex_and_get_response_entity_withid(self):

        class TestVertex(Vertex):
            allow_undefined = True


        async def test():
            data = {'name': 'mark', 'sex': 'male'}
            v = self.mapper.create(data=data, entity=TestVertex)
            self.mapper.save(v)
            r = await self.mapper.send()
            v1 = r.first()

            self.entity_save_assertions(v1)

        self.ioloop.run_until_complete(test())

    def test_can_save_generic_edge_with_two_generic_vertices_all_at_once_and_update_allids(self):

        async def test():
            label = 'some_label'
            v1 = self.mapper.create()
            v2 = self.mapper.create()
            e = self.mapper.connect(v1, v2, label)

            await self.mapper.save(e).send()
            self.entity_save_assertions(v1)
            self.entity_save_assertions(v2)
            self.entity_save_assertions(e)

        self.ioloop.run_until_complete(test())

    def test_can_save_generic_edge_with_one_generic_vertex_all_at_once_and_update_allids(self):
        class TestVertex(Vertex):
            allow_undefined = True


        async def test():
            label = 'some_label'
            v1 = self.mapper.create(entity=TestVertex)
            v2 = self.mapper.create()
            e = self.mapper.connect(v1, v2, label)

            await self.mapper.save(e).send()
            self.entity_save_assertions(v1)
            self.entity_save_assertions(v2)
            self.entity_save_assertions(e)

        self.ioloop.run_until_complete(test())

    def test_can_save_generic_edge_with_two_defined_vertices_all_at_once_and_update_allids(self):
        class TestVertex(Vertex):
            allow_undefined = True

        class TestVertex2(Vertex):
            allow_undefined = True


        async def test():
            label = 'some_label'
            v1 = self.mapper.create(entity=TestVertex)
            v2 = self.mapper.create(entity=TestVertex2)
            e = self.mapper.connect(v1, v2, label)

            await self.mapper.save(e).send()
            self.entity_save_assertions(v1)
            self.entity_save_assertions(v2)
            self.entity_save_assertions(e)

        self.ioloop.run_until_complete(test())

    def test_can_save_defined_edge_with_two_defined_vertices_all_at_once_and_update_allids(self):
        class TestVertex(Vertex):
            allow_undefined = True

        class TestVertex2(Vertex):
            allow_undefined = True

        class TestEdge(Edge):
            pass

        async def test():
            label = 'some_label'
            v1 = self.mapper.create(entity=TestVertex)
            v2 = self.mapper.create(entity=TestVertex2)
            e = self.mapper.connect(v1, v2, label, edge_entity=TestEdge)

            await self.mapper.save(e).send()
            self.entity_save_assertions(v1)
            self.entity_save_assertions(v2)
            self.entity_save_assertions(e)

        self.ioloop.run_until_complete(test())

    def test_can_add_vertex_and_update_it(self):

        async def test():
            await self.purge()

            data = {
                'name': 'before_update',
            }
            updated = 'updated named {}'.format(str(random.random()))

            class UpdateVertex(Vertex):
                allow_undefined = True

            v = self.mapper.create(data=data, entity=UpdateVertex)
            x = await self.mapper.save(v).send()
            first = x.first()

            self.entity_save_assertions(first)
            self.assertEqual(v['id'], first['id'])

            first['name'] = updated

            await self.mapper.save(first).send()

            self.assertEqual(first['name'].values[0], updated)

        self.ioloop.run_until_complete(test())


    def test_can_add_vertex_and_remove_it(self):

        async def test():
            await self.purge()

            class RemoveVertex(Vertex):
                pass

            v = self.mapper.create(entity=RemoveVertex)
            x = await self.mapper.save(v).send()
            all_v = self.mapper.gremlin.V()
            res = await self.mapper.query(gremlin=self.mapper.gremlin)

            self.assertEqual(1, len(res))

            await self.mapper.delete(v).send()

            all_v = self.mapper.gremlin.V()
            res = await self.mapper.query(gremlin=self.mapper.gremlin)

            self.assertEqual(0, len(res))

        self.ioloop.run_until_complete(test())

    def test_can_add_vertices_with_edge_delete_vertices_and_edge_is_automatically_gone(self):

        async def test():
            await self.purge()

            class RemoveVertex2(Vertex):
                pass

            class RemoveEdge2(Edge):
                pass

            v1 = self.mapper.create(entity=RemoveVertex2)
            v2 = self.mapper.create(entity=RemoveVertex2)
            e = self.mapper.connect(v1, v2, edge_entity=RemoveEdge2)
            await self.mapper.save(e).send()

            all_v = self.mapper.gremlin.V()
            all_v_res = await self.mapper.query(gremlin=self.mapper.gremlin)
            all_e = self.mapper.gremlin.E()
            all_e_res = await self.mapper.query(gremlin=self.mapper.gremlin)

            self.assertEqual(2, len(all_v_res))
            self.assertEqual(1, len(all_e_res))

            await self.mapper.delete(v1).send()
            await self.mapper.delete(v2).send()

            all_v = self.mapper.gremlin.V()
            all_v_res = await self.mapper.query(gremlin=self.mapper.gremlin)
            all_e = self.mapper.gremlin.E()
            all_e_res = await self.mapper.query(gremlin=self.mapper.gremlin)

            self.assertEqual(0, len(all_v_res))
            self.assertEqual(0, len(all_e_res))

        self.ioloop.run_until_complete(test())

    def test_can_add_vertices_with_edge_delete_one_vertext_and_edge_is_automatically_gone(self):

        async def test():
            await self.purge()

            class RemoveVertex2(Vertex):
                pass

            class RemoveEdge2(Edge):
                pass

            v1 = self.mapper.create(entity=RemoveVertex2)
            v2 = self.mapper.create(entity=RemoveVertex2)
            e = self.mapper.connect(v1, v2, edge_entity=RemoveEdge2)
            await self.mapper.save(e).send()

            all_v = self.mapper.gremlin.V()
            all_v_res = await self.mapper.query(gremlin=self.mapper.gremlin)
            all_e = self.mapper.gremlin.E()
            all_e_res = await self.mapper.query(gremlin=self.mapper.gremlin)

            self.assertEqual(2, len(all_v_res))
            self.assertEqual(1, len(all_e_res))

            await self.mapper.delete(v1).send()

            all_v = self.mapper.gremlin.V()
            all_v_res = await self.mapper.query(gremlin=self.mapper.gremlin)
            all_e = self.mapper.gremlin.E()
            all_e_res = await self.mapper.query(gremlin=self.mapper.gremlin)

            self.assertEqual(1, len(all_v_res))
            self.assertEqual(0, len(all_e_res))

        self.ioloop.run_until_complete(test())


class MapperTestCases(object):

    def test_can_utilitze_custom_mapper(self):
        variable = str(random.random())

        class MapperTestVertexCutsom(Vertex):
            allow_undefined = True


        class MapperTestMapperCustom(EntityMapper):
            entity = MapperTestVertexCutsom

            def create(self, *args, **kwargs):
                entity = super(MapperTestMapperCustom, self).create(*args, **kwargs)
                entity['variable'] = variable
                return entity

        v = self.mapper.create(entity=MapperTestVertexCutsom)
        d = v.data

        self.assertIsInstance(v, MapperTestVertexCutsom)
        self.assertIn('variable', d)
        self.assertEqual(d['variable'][0]['value'], variable)

    def test_can_restrict_entity_creation_based_on_duplicate_field_values(self):

        async def test():
            await self.purge()

            class MapperTestVertexDuplicate(Vertex):
                allow_undefined = True


            class MapperTestMapper(EntityMapper):
                entity = MapperTestVertexDuplicate
                unique_fields = ['first_name',]


            d = {'first_name': 'mark' + str(random.random())}
            v1 = self.mapper.create(data=d, entity=MapperTestVertexDuplicate)
            v2 = self.mapper.create(data=d, entity=MapperTestVertexDuplicate)

            r = await self.mapper.save(v1).send()
            r2 = await self.mapper.save(v2).send()

            gremlin = self.mapper.gremlin.V()
            res = await self.mapper.query(gremlin=gremlin)

            self.assertEqual(1, len(res))

        self.ioloop.run_until_complete(test())

    def test_can_restrict_multiple_entity_connections_both_direction(self):

        async def test():
            await self.purge()

            class MapperTestVertexRestrict(Vertex):
                allow_undefined = True

            class MapperTestEdgeRestrict(Edge):
                allow_undefined = True

            class MapperTestEdgeMapperRestrict(EntityMapper):
                entity = MapperTestEdgeRestrict
                unique = 'both'

            d = {'first_name': 'mark' + str(random.random())}
            v1 = self.mapper.create(data=d, entity=MapperTestVertexRestrict)
            v2 = self.mapper.create(data=d, entity=MapperTestVertexRestrict)
            e = self.mapper.connect(v1, v2, edge_entity=MapperTestEdgeRestrict)
            e2 = self.mapper.connect(v1, v2, edge_entity=MapperTestEdgeRestrict)
            res = await self.mapper.save(e).send()
            res2 = await self.mapper.save(e2).send()
            gremlin = self.mapper.gremlin.E()
            result = await self.mapper.query(gremlin=gremlin)

            self.assertEqual(1, len(result))

        self.ioloop.run_until_complete(test())

    def test_can_restrict_multiple_entity_connections_in_direction(self):

        async def test():
            await self.purge()

            class MapperTestVertexRestrictIn(Vertex):
                allow_undefined = True

            class MapperTestEdgeRestrictIn(Edge):
                allow_undefined = True

            class MapperTestEdgeMapperRestrictIN(EntityMapper):
                entity = MapperTestEdgeRestrictIn
                unique = 'in'

            d = {'first_name': 'mark' + str(random.random())}
            v1 = self.mapper.create(data=d, entity=MapperTestVertexRestrictIn)
            v2 = self.mapper.create(data=d, entity=MapperTestVertexRestrictIn)
            e = self.mapper.connect(v1, v2, edge_entity=MapperTestEdgeRestrictIn)
            e2 = self.mapper.connect(v2, v1, edge_entity=MapperTestEdgeRestrictIn)
            res = await self.mapper.save(e).send()
            res2 = await self.mapper.save(e2).send()
            gremlin = self.mapper.gremlin.E()
            result = await self.mapper.query(gremlin=gremlin)

            self.assertEqual(1, len(result))

        self.ioloop.run_until_complete(test())

    def test_can_restrict_multiple_entity_connections_out_direction(self):

        async def test():
            await self.purge()

            class MapperTestVertexRestrictOut(Vertex):
                allow_undefined = True

            class MapperTestEdgeRestrictOut(Edge):
                allow_undefined = True

            class MapperTestEdgeMapperRestrictOut(EntityMapper):
                entity = MapperTestEdgeRestrictOut
                unique = 'in'

            d = {'first_name': 'mark' + str(random.random())}
            v1 = self.mapper.create(data=d, entity=MapperTestVertexRestrictOut)
            v2 = self.mapper.create(data=d, entity=MapperTestVertexRestrictOut)
            e = self.mapper.connect(v1, v2, edge_entity=MapperTestEdgeRestrictOut)
            e2 = self.mapper.connect(v2, v1, edge_entity=MapperTestEdgeRestrictOut)
            res = await self.mapper.save(e).send()
            res2 = await self.mapper.save(e2).send()
            gremlin = self.mapper.gremlin.E()
            result = await self.mapper.query(gremlin=gremlin)

            self.assertEqual(1, len(result))

        self.ioloop.run_until_complete(test())

    def test_can_save_edge_on_vertices_that_were_used_in_previous_connection_when_unique_is_true(self):

        async def test():
            await self.purge()

            class MapperTestVertexRestrictAgain(Vertex):
                allow_undefined = True

            class MapperTestEdgeRestrictAgain(Edge):
                allow_undefined = True

            class MapperTestEdgeMapperRestrictAgain(EntityMapper):
                entity = MapperTestEdgeRestrictAgain
                unique = 'both'

            d = {'first_name': 'mark' + str(random.random())}
            v1 = self.mapper.create(data=d, entity=MapperTestVertexRestrictAgain)
            v2 = self.mapper.create(data=d, entity=MapperTestVertexRestrictAgain)
            v3 = self.mapper.create(data=d, entity=MapperTestVertexRestrictAgain)
            e = self.mapper.connect(v1, v2, edge_entity=MapperTestEdgeRestrictAgain)
            e2 = self.mapper.connect(v1, v3, edge_entity=MapperTestEdgeRestrictAgain)
            e3 = self.mapper.connect(v1, v3, edge_entity=MapperTestEdgeRestrictAgain)
            res = await self.mapper.save(e).send()
            res2 = await self.mapper.save(e2).send()
            res2 = await self.mapper.save(e3).send()
            gremlin = self.mapper.gremlin.E()
            result = await self.mapper.query(gremlin=gremlin)

            self.assertEqual(2, len(result))

        self.ioloop.run_until_complete(test())

    # def test_can_get_or_create(self):
    # this is not in the mapper yet
    #     async def test():
    #         await self.purge()
    #
    #         class GoCVertex(Vertex):
    #             allow_undefined = True
    #
    #         goc_v = await self.mapper.get_or_create(GoCVertex, field_val={'name': 'mark'})
    #         goc_v2 = await self.mapper.get_or_create(GoCVertex, field_val={'name': 'mark'})
    #
    #         ins = GoCVertex()
    #         g = self.mapper.gremlin
    #         g.V().has('"_label"', str(ins))
    #         res = await self.mapper.query(gremlin=g)
    #
    #         self.assertEqual(goc_v['id'], goc_v2['id'])
    #         self.assertEqual(1, len(res))
    #
    #     self.ioloop.run_until_complete(test())
