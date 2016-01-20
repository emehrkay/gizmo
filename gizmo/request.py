import copy


class xAsync(object):

    def __init__(self, uri, graph, username=None, password=None, port=8184):
        import asyncio

        from aiogremlin import GremlinClient

        self._ws_uri = 'ws://%s:%s/%s' % (uri, port, graph)
        self.loop = asyncio.get_event_loop()
        self.connection = GremlinClient(loop=self.loop)

    def __del__(self):
        if self.loop:
            self.loop.run_until_complete(self.connection.close())

    def send(self, script=None, params=None, update_models=None,
             rebindings=None, *args, **kwargs):

        if not params:
            params = {}

        if not update_models:
            update_models = {}

        execute = self.connection.execute(script, bindings=params)
        resp = self.loop.run_until_complete(execute)
        data = resp[0].data if resp[0].data else {}
        response = Response(data, update_models)
        response.script = script
        response.params = params

        return response


class Async(object):

    def __init__(self, uri, graph, username=None, password=None, port=8184):
        self._ws_uri = 'ws://%s:%s/%s' % (uri, port, graph)

    def send(self, script=None, params=None, update_models=None, *args,
             **kwargs):
        from tornado import gen
        from tornado.ioloop import IOLoop
        from gremlinclient.client import submit

        loop = IOLoop.current()

        if not params:
            params = {}

        if not update_models:
            update_models = {}

        resp_data = {'data': []}

        @gen.coroutine
        def run():
            resp = yield submit(gremlin=script, bindings=params)

            while True:
                msg = yield resp.read()

                if msg is None:
                    break
                
                if msg.data:
                    resp_data['data'] += msg.data

        loop.run_sync(run)
        return Response(resp_data['data'], update_models)


class Response(object):
    def __init__(self, data=None, update_models=None):
        if not update_models:
            update_models = {}

        self.original_data = data
        self.update_models = update_models
        self.data = self._fix_data(data)

    def _fix_data(self, resp):
        # TODO: clean up this shit show
        if not resp:
            resp = {}
        response = []
        update_keys = list(self.update_models.keys())

        def has_update(keys):
            # TODO: look into why subtracting sets doesnt work for single entry
            # items
            # c = list(set(update_keys) - set(keys))
            # return len(c) > 0
            for k in keys:
                if k in update_keys:
                    return True

            return False

        def fix_properties(data_set):
            if isinstance(data_set, dict) and 'properties' in data_set:
                prop = data_set['properties']
                del data_set['properties']
                data_set.update(prop)

            return data_set

        for arg in resp:
            if not hasattr(arg, '__iter__'):
                response = [{'response': arg}]
            elif isinstance(arg, dict):
                if has_update(arg.keys()):
                    for k, v in arg.items():
                        if k in self.update_models:
                            model = self.update_models[k]
                            data = {}
                            fix_properties(v)

                            for field, value in v.items():
                                data[field] = value[-1]['value'] \
                                    if type(value) is list\
                                    and len(value) else value

                            if 'id' in data:
                                data['_id'] = data['id']
                                model.fields['_id'].value = data['id']
                                del(data['id'])

                            response.append(data)
                            model.hydrate(data)
                else:
                    data = fix_properties(arg)
                    for field, value in data.items():
                        data[field] = value[-1]['value'] \
                            if type(value) is list else value

                    if 'id' in data:
                        data['_id'] = data['id']
                        del(data['id'])

                    response.append(data)

        return response

    def __getitem__(self, key):
        val = None

        try:
            data = self.data[key]
            val = copy.deepcopy(data)

            if '_properties' in data:
                del val['_properties']
                val.update(data['_properties'])
        except:
            pass

        return val

    def update_models(self, mappings):
        fixed = copy.deepcopy(self.data)

        for var, model in mappings.items():
            if var in self.data:
                model.hydrate(self.data[var])

                try:
                    del fixed[var]
                except:
                    pass

                fixed.update(self.data[var])

        self.data = fixed

        return self

    def __setitem__(self, key, val):
        self.data[key] = val

        return self
