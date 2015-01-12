from entity import Vertex, _GenericMapper
from types import MethodType


TRIGGERED_SOURCE_EVENT = 'triggered_source_event'
SOURCE_EVENT_ENTRY = 'source_event_entry'
SOURCE_EVENT = 'source_event'


class Entity(Vertex):
    _node_type = SOURCE_EVENT
    allow_undefined = True


class EntityMapper(_GenericMapper):
    model = Entity


class EventSourceException(Exception):
    pass


class MapperMixin(object):
    """
    this class is used to add event sourcing functionality
    (http://martinfowler.com/eaaDev/EventSourcing.html)
    to Gizmo entities. Simply define a custom mapper and subclass
    this class to get the added functionality.

    The source must be defined before saving the model.
    """

    def create_model(self, data=None, model_class=None, data_type='python'):
        """
        This method is used to create the model defined in the original
        mapper. It captures all value changes on the node and stores them
        in an gizmo.event.Entity vertex
        """
        model = super(MapperMixin, self).create_model(data=data,\
            model_class=model_class, data_type=data_type)
        self.event = event = self.mapper.create_model(model_class=Entity,\
            data_type=data_type)
        set_item = model.__setitem__

        def set_item_override(self, name, value):
            if model[name] != value:
                event[name] = value

            return set_item(name, value)

        new_setter = MethodType(set_item_override, model, type(model))

        setattr(model, '__setitem__', new_setter)

        return model

    def set_source(self, source):
        """
        The source is the out vertex, or the thing that triggered the change
        in the model to be saved
        """
        self.source_model = source

        return self

    def save(self, model, bind_return=True):
        """
        Method used to save the original model and to add the 
        source -> event and
        model -> event
        relationships
        """
        if self.source_model is None:
            error = 'There must be a source defined before saving.'
            raise EventSourceException(error)

        super(EventSource, self).save(model=model, bind_return=bind_return)

        source_edge = self.mapper.connect(out_v=self.source_model, in_v=self.event,\
            label=TRIGGERED_SOURCE_EVENT)
        event_edge = self.mapper.connect(out_v=model, in_v=model,\
            label=SOURCE_EVENT_ENTRY)

        self.mapper.save(source_edge, bind_return=False)
        self.mapper.save(event_edge, bind_return=False)
