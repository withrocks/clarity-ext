from clarity_ext.domain import Container


class ContainerRepository:
    """
    Used to fetch `Container` domain objects. Fetches from a cache before
    creating from a rest resource.
    """

    def __init__(self):
        self.cache = dict()

    def get_container(self, container_resource):
        if container_resource.id in self.cache:
            return self.cache[container_resource.id]
        else:
            ret = Container.create_from_rest_resource(container_resource)
            self.cache[container_resource.id] = ret
            return ret

