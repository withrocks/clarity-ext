from collections import namedtuple
from clarity_ext.utils import lazyprop
from clarity_ext.domain.common import DomainObjectMixin


class Well(DomainObjectMixin):
    """
    Encapsulates a location in a container.

    This could for example be a well in a plate, but could also be the single location in a tube.

    # TODO: Rename class to Location?
    """

    def __init__(self, position, container, artifact=None):
        self.position = position
        self.container = container
        self.artifact = artifact

    @property
    def is_empty(self):
        return self.artifact is None

    def get_key(self):
        return "{}:{}".format(self.position.row, self.position.col)

    def __repr__(self):
        if self.container:
            container_name = self.container.name
        else:
            container_name = '<no container>'
        return "{}({}{})".format(container_name, self.position.row_letter, self.position.col)

    @property
    def index_down_first(self):
        # The position is 1-indexed
        return (self.position.col - 1) * self.container.size.height + self.position.row


class ContainerPosition(namedtuple("ContainerPosition", ["row", "col"])):
    """Defines the position of the plate, (zero based)"""
    def __repr__(self):
        return "{}:{}".format(self.row_letter, self.col)

    @staticmethod
    def create(repr):
        """
        Creates a PlatePosition from different representations. Supported formats:
            "<row as A-Z>:<col as int>"
            "<row as int>:<col as int>"
            (<row>, <col>)
        """
        if isinstance(repr, basestring):
            row, col = repr.split(":")
            if row.isalpha():
                row = ord(row.upper()) - 64
            else:
                row = int(row)
            col = int(col)
        else:
            row, col = repr
        return ContainerPosition(row=row, col=col)

    @property
    def row_letter(self):
        """Returns the letter representation for the row index, e.g. 3 => C"""
        return chr(65 + self.row - 1)


class PlateSize(namedtuple("PlateSize", ["height", "width"])):
    """Defines the size of a plate"""
    pass


class Container(DomainObjectMixin):
    """Encapsulates a Container"""

    DOWN_FIRST = 1
    RIGHT_FIRST = 2

    CONTAINER_TYPE_96_WELLS_PLATE = 100
    CONTAINER_TYPE_STRIP_TUBE = 200
    CONTAINER_TYPE_TUBE = 300
    CONTAINER_TYPE_PATTERNED_FLOW_CELL = 400

    def __init__(self, mapping=None, container_type=None, size=None):
        """
        :param mapping: A dictionary-like object containing mapping from well
        position to content. It can be non-complete.
        :param container_type: One of the CONTAINER_TYPE_* constants
        :param size: The size of the container. Object should support height and width
        :return:
        """
        self.mapping = mapping
        self.container_type = container_type
        self.id = None
        self.name = None

        if size:
            self.size = size
        else:
            # TODO: Require the size to be sent in instead
            if self.container_type == self.CONTAINER_TYPE_96_WELLS_PLATE:
                self.size = PlateSize(height=8, width=12)
            elif self.container_type == self.CONTAINER_TYPE_STRIP_TUBE:
                self.size = PlateSize(height=8, width=1)
            elif self.container_type == self.CONTAINER_TYPE_TUBE:
                self.size = PlateSize(height=1, width=1)
            else:
                raise ValueError("Unknown plate type '{}'".format(self.container_type))

    def __repr__(self):
        return "<Container id={}>".format(self.id)

    @classmethod
    def create_from_rest_resource(cls, resource, api_artifacts=[]):
        """
        Creates a container based on a resource from the REST API.
        """
        size = PlateSize(width=resource.type.x_dimension[
                         "size"], height=resource.type.y_dimension["size"])
        if resource.type.name == "96 well plate":
            container_type = Container.CONTAINER_TYPE_96_WELLS_PLATE
        elif resource.type.name == "Tube":
            container_type = Container.CONTAINER_TYPE_TUBE
        elif resource.type.name == "Strip Tube":
            container_type = Container.CONTAINER_TYPE_STRIP_TUBE
        elif resource.type.name == "Patterned Flow Cell":
            container_type = Container.CONTAINER_TYPE_PATTERNED_FLOW_CELL
        else:
            raise NotImplementedError(
                "Resource type '{}' is not supported".format(resource.type.name))
        ret = Container(container_type=container_type, size=size)
        ret.id = resource.id
        ret.name = resource.name
        ret.size = size
        for artifact in api_artifacts:
            ret.set_well(artifact.location[1], artifact)
        return ret

    @lazyprop
    def wells(self):
        ret = dict()
        for row, col in self._traverse():
            key = "{}:{}".format(row, col)
            content = self.mapping[key] if self.mapping and key in self.mapping else None
            pos = ContainerPosition(row=row, col=col)
            ret[(row, col)] = Well(pos, content)
            ret[(row, col)].container = self
        return ret

    def _traverse(self, order=DOWN_FIRST):
        """Traverses the container, visiting wells in a certain order, yielding keys as (row,col) tuples, 1-indexed"""
        if not self.size:
            raise ValueError("Not able to traverse the container without a plate size")

        rows = range(1, self.size.height + 1)
        cols = range(1, self.size.width + 1)
        if order == self.RIGHT_FIRST:
            return ((row, col) for row in rows for col in cols)
        else:
            return ((row, col) for col in cols for row in rows)

    # Lists the wells in a certain order:
    def enumerate_wells(self, order=DOWN_FIRST):
        for key in self._traverse(order):
            yield self.wells[key]

    def list_wells(self, order=DOWN_FIRST):
        return list(self.enumerate_wells(order))

    def set_well(self, well_pos, artifact_name=None, artifact_id=None, artifact=None):

        if well_pos not in self.wells:
            raise KeyError(
                "Well id {} is not available in this container (type={})".format(well_pos, self))

        self.wells[well_pos].artifact_name = artifact_name
        self.wells[well_pos].artifact_id = artifact_id
        # TODO: Accept only an artifact
        self.wells[well_pos].artifact = artifact


    def __repr__(self):
        return "{}".format(self.id)


