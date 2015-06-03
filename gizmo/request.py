import copy


class _Request(object):

    def __init__(self, uri, graph, username=None, password=None):
        self.responses = []
        self.uri = uri
        self.graph = graph
        self.username = username
        self.password = password

    def reset(self):
        pass

    def send(self, script=None, params=None, *args, **kwargs):
        return _Response()


class _Response(object):

    def __init__(self, data=None, update_models=None):
        self.original_data = data
        self.update_models = update_models
        self.data = self._fix_data(data)

    def _fix_data(self, arg):

        def fix_properties(data_set):
            if isinstance(data_set, dict) and '_properties' in data_set:
                prop = data_set['_properties']
                del data_set['_properties']
                data_set.update(prop)

            return data_set

        if not hasattr(arg, '__iter__'):
            data = [{'response': arg}]
        elif isinstance(arg, dict):
            if len(self.update_models) > 0:
                data = []

                for k, model in self.update_models.items():
                    val = fix_properties(arg.get(k, None))

                    if isinstance(val, dict):
                        """
                        the _id field is _immutable in the interface,
                        but for newly created
                        entites it is passed back with the response.
                        Set it here
                        """
                        if '_id' in val:
                            model.fields['_id'].value = val['_id']

                        model.hydrate(val)
                        data.append(val)
                        model.dirty = False
                        del arg[k]

                data.append(arg)
            else:
                data = [fix_properties(arg)]
        else:
            data = list(map(fix_properties, arg))

        return data

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


class Binary(_Request):

    def __init__(self, uri, graph, username=None, password=None, port=8184):
        from rexpro import RexProConnection

        super(Binary, self).__init__(uri, username, password)

        self.connection = RexProConnection(uri, port, graph)

    def send(self, script=None, params=None, update_models=None):
        if params is None:
            params = {}

        if update_models is None:
            update_models = {}

        self.connection.open()
        resp = self.connection.execute(script, params)
        self.connection.close()
        response = _Response(resp, update_models)
        response.script = script
        response.params = params

        return response


class BinaryResponse(_Response):
    pass


class Http(_Request):

    def __init__(self, uri, graph, username=None, password=None):
        import request

        super(Binary, self).__init__(uri, username, password)

        self.connection = request.Request()

    def send(self, script=None, params=None, update_models=None):
        if params is None:
            params = {}

        if update_models is None:
            update_models = {}

        resp = self.connection.post(script, params)

        return HttpResponse(resp, update_models)


class HttpResponse(_Response):
    pass
