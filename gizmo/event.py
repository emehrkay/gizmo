from gizmo.entity import Vertex
from gizmo.mapper import _GenericMapper


TRIGGERED_SOURCE_EVENT = 'triggered_source_event'
SOURCE_EVENT_ENTRY = 'source_event_entry'
SOURCE_EVENT = 'source_event'


class Entity(Vertex):
    _node_type = SOURCE_EVENT
    _allowed_undefined = True


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

    When using this mixin there are a few things that need to be
    considered:
        * If you're creating an entity for the first time and
        want the changes to be captured, you do not pass the
        values in with construction, but rather hydrate
        *
    """

    def save(self, model, bind_return=True, callback=None, source=None,
             *args, **kwargs):
        """
        Method used to save the original model and to add the
        source -> event and
        model -> event
        relationships
        """
        super(MapperMixin, self).save(model=model, bind_return=bind_return,
                                      callback=callback, *args, **kwargs)
        self.mapper._enqueue_mapper(self)

        if source is not None:
            fields_changed = len(model.changed) > 0
            fields_removed = len(model.removed) > 0

            # only create the source event if there were actual changes
            if fields_changed or fields_removed:
                self.event = event = \
                    self.mapper.create_model(model_class=Entity,
                                             data_type=model.data_type)

                for field, change in model.changed.items():
                    event[field] = change

                if model._atomic_changes and fields_removed:
                    # TODO: track the fields that were removed
                    pass

                source_edge = self.mapper.connect(out_v=source,
                                                  in_v=event,
                                                  label=TRIGGERED_SOURCE_EVENT)
                event_edge = self.mapper.connect(out_v=model, in_v=event,
                                                 label=SOURCE_EVENT_ENTRY)

                self.mapper.save(source_edge, bind_return=True)
                self.mapper.save(event_edge, bind_return=True)

        return self

    def get_event_history(self, model, range_start=None, range_end=None):
        # TODO: fill this query out
        pass
