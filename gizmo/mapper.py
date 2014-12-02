from utils import gizmo_import, IMMUTABLE
from element import Edge, Vertex, General
from gremlinpy.gremlin import Function

class Query(object):
    def __init__(self, gremlin):
        self.gremlin = gremlin
        self.fields  = []
        self.queries = []
        
    def reset(self):
        self.fields = []
        
    def add_query(self, script, params=None, model=None):
        if params is None:
            params = {}
            
        self.queries.append({
            'script': script,
            'params': params,
            'model': model
        })
        
        return self
        
    def build_fields(self, data, immutable, gremlin):
        for key, val in data.iteritems():
            if key not in immutable:
                value = val
                if type(val) is dict or type(val) is list:
                    listed = self.iterable_to_map(val, gremlin)
                    value  = "[%s]" % listed
                    
                    self.fields.append(value)
                else:
                    bound = gremlin.bind_param(value)

                    self.fields.append("'%s': %s" % (key, bound[0]))
        
        return self
        
    def update_fields(self, data, immutable, gremlin):
        for k, v in data.iteritems():
            if k not in immutable:
                if type(v) is dict or type(v) is list:
                    gmap  = self.iterable_to_map(v, gremlin)
                    entry = "it.setProperty('%s', %s)" % (k, gmap)
                else:
                    bound = self.gremlin.bind_param(v)
                    entry = "it.setProperty('%s', %s)" % (k, bound[0])
                    
                self.fields.append(entry)
                
        return self
        
    def iterable_to_map(self, iterable, gremlin):
        if gremlin is None:
            gremlin = self.gremlin
            
        gmap = []
        
        for k, v in enumerate(iterable):
            if type(v) is dict or type(v) is list:
                gmap.append(self.iterable_to_map(v, gremlin))
            else:
                bound = gremlin.bind_param(v)
                entry = "'%s': %s" % (k, bound[0])
                
                gmap.append(entry)
        
        return ','.join(gmap)
        
    def add_vertex(self, model, gremlin=None):
        if model['_type'] is None:
            raise Exception('Models need to have a type defined')
        
        model.fields.data_type = 'graph'
        
        if gremlin is None:
            gremlin = self.gremlin
        
        self.build_fields(model.data, IMMUTABLE['vertex'], gremlin)
        
        script = 'g.addVertex([%s])' % ', '.join(self.fields)

        gremlin.set_graph_variable('').raw(script)

        script = str(gremlin)
        params = gremlin.bound_params
        
        gremlin.reset()
        
        return self.add_query(script, params, model)
        
    def add_edge(self, model, gremlin=None):
        if model['_label'] is None:
            raise Exception('The edge must have a label before saving')
        
        if gremlin is None:
            gremlin = self.gremlin
        
        label_bound = gremlin.bind_param(model['_label'])
        edge_fields = ''
        
        self.build_fields(model.data, IMMUTABLE['edge'], gremlin)
        
        if len(fields) > 0:
            edge_fields = ', [%s]' % ', '.join(fields)

        script = 'g.addEdge(%s, %s, %s%s)' % (out_v_id, in_v_id, label_bound[0], edge_fields)
        
        gremlin.set_graph_variable('').raw(script)
        
        return self
    
    def update(self, model, immutable, gremlin=None):
        if model['_type'] is None:
            raise Exception()
            
        if model['_id'] is None:
            raise Exception()
            
        if gremlin is None:
            gremlin = self.gremlin
        
        model.fields.data_type = 'graph'
        
        model_type = 'e' if model['_type'] == 'edge' else 'v'
        
        self.update_fields(model.data, model._immutable, gremlin)

        next_func = Function(gremlin, 'next')
        
        getattr(gremlin, model_type)(model['_id'])._().sideEffect.close('; '.join(self.fields)).add_token(next_func)
        
        script = str(gremlin)
        params = gremlin.bound_params
        
        gremlin.reset()
        
        return self.add_query(script, params, model)

    def save(self, model, gremlin=None):
        if model['_type'] is None:
            raise Exception('The model does not have a _type defined')
            
        if gremlin is None:
            gremlin = self.gremlin
        
        id        = model['_id']
        immutable = model._immutable

        if id is None:
            if model['_type'] == 'vertex':
                self.add_vertex(model, gremlin)
            else:
                out_v_id, in_v_id = self._get_or_create_edge_vertices(model)
            
                self.add_edge(out_v, in_v, data, gremlin)
        else:
            print 'updating', model
            self.update(model, immutable, gremlin)
            
        return self
        
        # model_type = 'e' if model['_type'] == 'edge' else 'v'
        # create     = 'addEdge' if model['_type'] == 'e' else 'addVertex'
        # data       = model.data
        # immutable  = model._immutable
        # fields     = []
        #
        # def build_fields(data):
        #     for key, val in data.iteritems():
        #         if key not in immutable:
        #             value = val
        #             bound = gremlin.bind_param(value)
        #
        #             fields.append("'%s': %s" % (key, bound[0]))
        #
        # if(model_type == 'v'):
        #     build_fields(data)
        #
        #     script = 'g.addVertex([%s])' % ', '.join(fields)
        #
        #     gremlin.set_graph_variable('').raw(script)
        # else:
        #     if model['_label'] is None:
        #         raise Exception('The edge must have a label before saving')
        #
        #     out_v_id, in_v_id = self._get_or_create_edge_vertices(model)
        #     label_bound       = gremlin.bind_param(model['_label'])
        #     edge_fields       = ''
        #
        #     build_fields(data)
        #
        #     if len(fields) > 0:
        #         edge_fields = ', [%s]' % ', '.join(fields)
        #
        #     script = 'g.addEdge(%s, %s, %s%s)' % (out_v_id, in_v_id, label_bound[0], edge_fields)
        #
        #     gremlin.set_graph_variable('').raw(script)
        #
        # return self

    def _get_or_create_edge_vertices(self, edge):
        out_v = edge.out_v
        in_v  = edge.in_v
        
        if out_v is None or in_v is None:
            error = 'Both out and in vertices must be set before saving \
                the edge'
                
            raise Exception(error)
        
        if out_v['_id'] is None:
            self.save(out_v)
            
            out_v_id = self.capture_variable(False)
        else:
            gremlin = Gremlin().v(out_v['_id'])
            
            self.enque_script(gremlin, out_v, False)
            
            out_v_id = self.scripts[-1]
            
        if in_v['_id'] is None:
            self.save(in_v)
            
            in_v_id = self.capture_variable(False)
        else:
            gremlin = Gremlin().v(in_v['_id'])
            
            self.enque_script(gremlin, in_v, False)
            
            in_v_id = self.scripts[-1]
            
        return out_v_id, in_v_id

    def delete(self, model, gremlin=None):
        id = model['_id']
        
        if id is None:
            raise Exception('Models must have an _id before they are deleted')
            
        if model['_type'] is None:
            raise Exception('Models need to have a type defined')
        
        if gremlin is None:
            gremlin = self.gremlin
        
        element = 'e' if model['_type'] == 'edge' else 'v'
        
        getattr(gremlin, element)(id).remove()
        
        return self.add_query(str(gremlin), gremlin.bound_params, model)


class Mapper(object):
    VARIABLE = 'gizmo_var'
    registrations = {}
    
    def __init__(self, request, gremlin=None, auto_commit=False):
        if gremlin is None:
            gremlin = Gremlin()

        self.count = -1
        self.request = request
        self.gremlin = gremlin
        self.auto_commit = auto_commit
        self.reset()
        
    def reset(self):
        self.queries = []
        self.models  = {}
        self.params  = {}
        
    def enqueue(self, query, bind_return=True):
        for entry in query.queries:
            self.count += 1
            script = entry['script']
            
            if bind_return:
                variable = '%s_%s' % (self.VARIABLE, self.count)
                script   = '%s = %s' % (variable, script)
                
                if 'model' in entry:
                    self.models[variable] = entry['model']
            
            self.queries.append(script)
            self.params.update(entry['params'])
        
        return self
        
    def save(self, model, bind_return=True, lookup=True):
        query = Query(self.gremlin)
        query.save(model)
        
        return self.enqueue(query, bind_return)
    
    def delete(self, model, lookup=True):
        query = Query(self.gremlin)
        
        query.delete(model)
        
        return self.enqueue(query, False)

    def create_model(self, data=None, model_class=None, lookup=True):
        """
        Method used to create a new model based on the data that is passed in.
        If the kwagrg model_class is passed in, it will be used to create the model
        else if pygwai.element.PYGWAI_MODEL is in data, that will be used
        finally, pygwai.model.element.General will be used to construct the model
        
        """
        check = True
        
        if data is None:
            data = {}

        if model_class is not None:
            try:
                model = model_class(data)
                check = False
            except Exception as e:
                check = True

        if check:
            try:
                if GIZMO_MODEL in data:
                    model = gizmo_import(data[PYGWAI_MODEL])(data)
                else:
                    raise
            except Exception as e:
                model = General(data)

        return model
        
    def _build_queries(self):
        if self.auto_commit is False:
            commit = '.'.join([self.gremlin.gv, 'commit()'])
            
            self.queries.append(commit)
            
        if len(self.models) > 0:
            returns = []
            
            for k in self.models.keys():
                returns.append("'%s': %s" % (k ,k))

            ret = '[%s]' % ','.join(returns)
            
            self.queries.append(ret)
        
        return self
        
    def send(self, script=None, params=None, gremlin=None):
        if gremlin is not None:
            script = str(gremlin)
            params = gremlin.bound_params
        elif script is None:
            self._build_queries()
            
            script = ';'.join(self.queries)
            params = self.params

        if script is None:
            script = ''
            
        if params is None:
            params = {}

        response = self.request.send(script, params)
        
        if len(self.models) > 0:
            response.update_models(self.models)
        
        print response









