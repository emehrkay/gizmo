from gizmo.entity import Vertex, Edge
from gizmo.mapper import _GenericMapper


class SourcedEvent(Vertex):
    _allowed_undefined = True


class SourcedEventMapper(_GenericMapper):
    entity = SourcedEvent


class TriggedSourceEvent(Edge):
    pass


class SourceEventEntry(Edge):
    pass


class EventSourceException(Exception):
    pass


class EventSourceMixin(object):
    """
    this class is used to add event sourcing functionality
    (http://martinfowler.com/eaaDev/EventSourcing.html)
    to Gizmo entities. Simply define a custom mapper and subclass
    this class to get the added functionality.

    The source must be defined before saving the entity.

    When using this mixin there are a few things that need to be
    considered:
    """

    def save(self, entity, bind_return=True, callback=None, source=None,
             *args, **kwargs):
        """
        Method used to save the original entity and to add the
        source -> event and
        entity -> event
        relationships
        """
        super(EventSourceMixin, self).save(entity=entity, bind_return=bind_return,
                                           callback=callback, *args, **kwargs)
        self.mapper._enqueue_mapper(self)

        if source is not None:
            fields_changed = len(entity.changed) > 0
            fields_removed = len(entity.removed) > 0

            # only create the source event if there were actual changes
            if fields_changed or fields_removed:
                self.event = event = \
                    self.mapper.create(entity=SourcedEvent,
                                             data_type=entity.data_type)

                for field, change in entity.changed.items():
                    event[field] = change

                if entity._atomic_changes and fields_removed:
                    # TODO: track the fields that were removed
                    pass

                source_params = {
                    'out_v': source,
                    'in_v': event,
                    'edge_entity': TriggedSourceEvent,
                }
                source_edge = self.mapper.connect(**source_params)
                event_edge = self.mapper.connect(out_v=entity, in_v=event,
                                                 edge_entity=SourceEventEntry)

                self.mapper.save(source_edge, bind_return=True)
                self.mapper.save(event_edge, bind_return=True)

        return self

    def get_event_history(self, entity, range_start=None, range_end=None):
        # TODO: fill this query out
        pass
