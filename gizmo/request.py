import copy
import collections
import json
import uuid

from tornado import gen
from tornado.concurrent import Future
from tornado.websocket import websocket_connect


Message = collections.namedtuple(
    "Message",
    ["status_code", "data", "message", "metadata"])


class GremlinClient(object):
    """Main interface for interacting with the Gremlin Server.
    :param str url: url for Gremlin Server (optional). 'http://localhost:8182/'
        by default
    :param loop:
    :param str lang: Language of scripts submitted to the server.
        "gremlin-groovy" by default
    :param str op: Gremlin Server op argument. "eval" by default.
    :param str processor: Gremlin Server processor argument. "" by default.
    :param float timeout: timeout for establishing connection (optional).
        Values ``0`` or ``None`` mean no timeout
    :param connector: A class that implements the method ``ws_connect``.
        Usually an instance of ``aiogremlin.connector.GremlinConnector``
    """
    def __init__(self, url='ws://localhost:8182/', loop=None,
                 lang="gremlin-groovy", processor="", timeout=None,
                 username="", password=""):
        self._lang = lang
        self._processor = processor
        self._closed = False
        self._session = None
        self._url = url
        self._timeout = timeout
        self._username = username
        self._password = password
        self._response = GremlinResponse

    @property
    def processor(self):
        """Readonly property. The processor argument for Gremlin
        Server"""
        return self._processor

    @property
    def lang(self):
        """Readonly property. The language used for Gremlin scripts"""
        return self._lang

    @property
    def url(self):
        """Getter/setter for database url used by the client"""
        return self._url

    @url.setter
    def url(self, value):
        self._url = value

    @property
    def closed(self):
        """Readonly property. Return True if client has been closed"""
        pass

    def close(self):
        pass

    def submit(self, gremlin, bindings=None, lang=None, rebindings=None,
               op="eval", processor=None, session=None,
               timeout=None, mime_type="application/json", handler=None):
        """
        :ref:`coroutine<coroutine>` method.
        Submit a script to the Gremlin Server.
        :param str gremlin: Gremlin script to submit to server.
        :param dict bindings: A mapping of bindings for Gremlin script.
        :param str lang: Language of scripts submitted to the server.
            "gremlin-groovy" by default
        :param dict rebindings: Rebind ``Graph`` and ``TraversalSource``
            objects to different variable names in the current request
        :param str op: Gremlin Server op argument. "eval" by default.
        :param str processor: Gremlin Server processor argument. "" by default.
        :param float timeout: timeout for establishing connection (optional).
            Values ``0`` or ``None`` mean no timeout
        :param str session: Session id (optional). Typically a uuid
        :param loop: :ref:`event loop<asyncio-event-loop>` If param is ``None``
            `asyncio.get_event_loop` is used for getting default event loop
            (optional)
        :returns: :py:class:`gremlinclient.client.GremlinResponse` object
        """
        lang = lang or self.lang
        processor = processor or self.processor
        if session is None:
            session = self._session
        if timeout is None:
            timeout = self._timeout
        if rebindings is None:
            rebindings = {}

        message = self._prepare_message(
            gremlin, bindings=bindings, lang=lang, rebindings=rebindings,
            op=op, processor=processor, session=session)
        message = self._set_message_header(message, mime_type)

        future = Future()
        future_conn = websocket_connect(self.url)

        def send_message(f):
            conn = f.result()
            conn.write_message(message, binary=True)
            future.set_result(self._response(conn, handler=handler))

        future_conn.add_done_callback(send_message)

        return future

    @staticmethod
    def _prepare_message(gremlin, bindings, lang, rebindings, op, processor,
                         session):
        message = json.dumps({
            "requestId": str(uuid.uuid4()),
            "op": op,
            "processor": processor,
            "args": {
                "gremlin": gremlin,
                "bindings": bindings,
                "language":  lang,
                "rebindings": rebindings
            }
        })
        if session is None:
            if processor == "session":
                raise RuntimeError("session processor requires a session id")
        else:
            message["args"].update({"session": session})
        return message

    @staticmethod
    def _set_message_header(message, mime_type):
        if mime_type == "application/json":
            mime_len = b"\x10"
            mime_type = b"application/json"
        else:
            raise ValueError("Unknown mime type.")
        return b"".join([mime_len, mime_type, message.encode("utf-8")])


class GremlinResponse(object):

    def __init__(self, conn, session=None, loop=None, username="",
                 password="", handler=None):
        self._conn = conn
        self._closed = False
        self._username = username
        self._password = password
        self._handler = handler

    def add_handler(self, func):
        self._handler = func

    def read(self):
        future = Future()
        if self._closed:
            future.set_result(None)
        else:
            future_resp = self._conn.read_message()

            def parser(f):
                message = json.loads(f.result().decode("utf-8"))
                message = Message(message["status"]["code"],
                                  message["result"]["data"],
                                  message["status"]["message"],
                                  message["result"]["meta"])
                if self._handler is None:
                    self._handler = lambda x: x
                if message.status_code == 200:
                    future.set_result(self._handler(message))
                    self._conn.close(code=1000)
                    self._closed = True
                elif message.status_code == 206:
                    future.set_result(self._handler(message))
                elif message.status_code == 407:
                    # Set up auth/ssl here
                    pass
                elif message.status_code == 204:
                    future.set_result(self._handler(message))
                    self._conn.close(code=1000)
                    self._closed = True
                else:
                    future.set_exception(RuntimeError(
                        "{0} {1}".format(message.status_code, message.message)))
                    self._conn.close(code=1006)
                    self._closed = True

            future_resp.add_done_callback(parser)
        return future


def submit(gremlin,
           url='ws://localhost:8182/',
           bindings=None,
           lang="gremlin-groovy",
           rebindings=None,
           op="eval",
           processor="",
           timeout=None,
           session=None,
           loop=None,
           username="",
           password="",
           handler=None):

    gc = GremlinClient(url=url, username=username, password=password)
    try:
        future_resp = gc.submit(gremlin, bindings=bindings, lang=lang,
                                rebindings=rebindings, op=op,
                                processor=processor, session=session,
                                timeout=timeout, handler=handler)
        return future_resp
    finally:
        gc.close()


class Request(object):

    def __init__(self, uri, graph, username=None, password=None, port=8184):
        self._ws_uri = 'ws://%s:%s/%s' % (uri, port, graph)

    async def send(self, script=None, params=None, update_entities=None, *args,
             **kwargs):
        if not params:
            params = {}

        if not update_entities:
            update_entities = {}

        data = []
        resp = await submit(gremlin=script, bindings=params, url=self._ws_uri)

        while True:
            msg = await resp.read()

            if not msg:
                break

            if msg.data:
                data += msg.data

        response = Response(data, update_entities, script, params)

        return response


class Response(object):

    def __init__(self, data=None, update_entities=None, script=None, 
                 params=None):
        if not update_entities:
            update_entities = {}

        self.original_data = data
        self.update_entities = update_entities
        self.data = self._fix_data(self._fix_titan_data(data))
        self.script = script
        self.params = params

    def _fix_titan_data(self, data):
        """temp method to address a titan bug where it returns maps in a
        differnt manner than other tinkerpop instances. This will be fixed
        in a later version of titan"""
        if isinstance(data, (list, tuple,)):
            fixed = []

            for ret in data:
                if isinstance(ret, dict):
                    if 'key' in ret and 'value' in ret:
                        fixed.append({ret['key']: ret['value']})

            if len(data) and not len(fixed):
                return data
            else:
                return fixed
        else:
            return data

    def _fix_data(self, resp):
        # TODO: clean up this shit show
        if not resp:
            resp = {}
        response = []
        update_keys = list(self.update_entities.keys())

        def has_update(keys):
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
                        if k in self.update_entities:
                            entity = self.update_entities[k]

                            if not entity:
                                continue

                            data = {}
                            fix_properties(v)

                            for field, value in v.items():
                                data[field] = value[-1]['value'] \
                                    if type(value) is list\
                                    and len(value) else value

                            if 'id' in data:
                                data['_id'] = data['id']
                                entity.fields['_id'].value = data['id']
                                del(data['id'])

                            response.append(data)
                            entity.hydrate(data)
                else:
                    data = fix_properties(arg)
                    for field, value in data.items():
                        data[field] = value[-1]['value'] \
                            if type(value) is list and len(value) else value

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

    def update_entities(self, mappings):
        fixed = copy.deepcopy(self.data)

        for var, entity in mappings.items():
            if var in self.data:
                entity.hydrate(self.data[var])

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
