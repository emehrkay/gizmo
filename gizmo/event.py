from gizmo.entity import Vertex, Edge
from gizmo.mapper import _GenericMapper


class SourcedEvent(Vertex):
    _allowed_undefined = True


class SourcedEventMapper(_GenericMapper):
    model = SourcedEvent


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

    The source must be defined before saving the model.

    When using this mixin there are a few things that need to be
    considered:
    """

    def save(self, model, bind_return=True, callback=None, source=None,
             *args, **kwargs):
        """
        Method used to save the original model and to add the
        source -> event and
        model -> event
        relationships
        """
        super(EventSourceMixin, self).save(model=model, bind_return=bind_return,
                                      callback=callback, *args, **kwargs)
        self.mapper._enqueue_mapper(self)

        if source is not None:
            fields_changed = len(model.changed) > 0
            fields_removed = len(model.removed) > 0

            # only create the source event if there were actual changes
            if fields_changed or fields_removed:
                self.event = event = \
                    self.mapper.create_model(model_class=SourcedEvent,
                                             data_type=model.data_type)

                for field, change in model.changed.items():
                    event[field] = change

                if model._atomic_changes and fields_removed:
                    # TODO: track the fields that were removed
                    pass

                source_params = {
                    'out_v': source,
                    'in_v': event,
                    'edge_model': TriggedSourceEvent,
                }
                source_edge = self.mapper.connect(**source_params)
                event_edge = self.mapper.connect(out_v=model, in_v=event,
                                                 edge_model=SourceEventEntry)

                self.mapper.save(source_edge, bind_return=True)
                self.mapper.save(event_edge, bind_return=True)

        return self

    def get_event_history(self, model, range_start=None, range_end=None):
        # TODO: fill this query out
        pass
