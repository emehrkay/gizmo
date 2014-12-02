import copy
from rexpro import RexProConnection


class _Request(object):
    def __init__(self, uri, graph, username=None, password=None):
        self.responses = []
        self.uri = uri
        self.graph = graph
        self.username = username
        self.password = password
        
    def reset(self):
        pass
        
    def send(script=None, params=None):
        pass


class _Response(object):
    def __init__(self, data=None, response=None):
        if data is None:
            data = {}
            
        self.data = data
        self.resonse = response
        
    def __getitem__(self, key):
        val = None
        
        
        
        return val
    
    def update_models(self, mappings):
        for var, model in mappings.iteritems():
            if var in self.data:
                model.hydrate(self.data[var])
        
        return self
        
    def __setitem__(self, key, val):
        self.data[key] = val
        
        return self


class Binary(_Request):
    def __init__(self, uri, graph, port=8184, username=None, password=None):
        super(Binary, self).__init__(uri, username, password)
        
        self.connection = RexProConnection(uri, port, graph)
    
    def send(self, script=None, params=None):
        if params is None:
            params = {}

        resp = self.connection.execute(script, params)

        return BinaryResponse(resp)


class BinaryResponse(_Response):
    def __init__(self, data=None, response=None):
        self.data = data
        self.response = response


