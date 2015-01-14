from entity import Vertex
from mapper import _GenericMapper
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

    source_model = None

    def create_model(self, data=None, model_class=None, data_type='python'):
        """
        This method is used to create the model defined in the original
        mapper. It captures all value changes on the node and stores them
        in an gizmo.event.Entity vertex
        """
        model = super(MapperMixin, self).create_model(data=data,\
            model_class=model_class, data_type=data_type)
        model._event = event = self.mapper.create_model(model_class=Entity,\
            data_type=data_type)
        set_item = model._set_item

        def set_item_override(self, name, value):
            if self._initial_load is False and model[name] != value:
                event[name] = value

            return set_item(name, value)

        new_setter = MethodType(set_item_override, model, type(model))

        setattr(model, '_set_item', new_setter)

        return model

    def save(self, model, bind_return=True, source=None):
        """
        Method used to save the original model and to add the
        source -> event and
        model -> event
        relationships
        """

        super(MapperMixin, self).save(model=model, bind_return=bind_return)

        if source is not None:
            event = self.mapper.create_model(model_class=Entity,\
                data_type=model.data_type)
            
            for field, change in model.changes.iteritems():
                event[field] = change
            
            if model.atomic_changes:
                pass
            
            source_edge = self.mapper.connect(out_v=source,\
                in_v=event, label=TRIGGERED_SOURCE_EVENT)
            event_edge = self.mapper.connect(out_v=model, in_v=event,\
                label=SOURCE_EVENT_ENTRY)

            self.mapper.save(source_edge, bind_return=True)
            self.mapper.save(event_edge, bind_return=True)

        return self

    def get_event_history(self, model, range_start=None, range_end=None):
        gremlin = self.mapper.gremlin
        #TODO: fill this query out
