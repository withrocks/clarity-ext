import re
from clarity_ext.domain.common import DomainObjectMixin
from clarity_ext.utils import lazyprop
from clarity_ext.domain.common import AssignLogger
from clarity_ext.unit_conversion import UnitConversion


class Udf(DomainObjectMixin):
    """
    Represents an entity having udfs
    """

    def __init__(self, api_resource=None, id=None, entity_specific_udf_map=None):
        self.id = id
        if entity_specific_udf_map:
            self.udf_map = entity_specific_udf_map
        else:
            self.udf_map = dict()
        self.assigner = AssignLogger(self)
        self.api_resource = api_resource
        if api_resource:
            attributes, self.udfs_to_attributes = DomainObjectWithUdfsMixin.create_automap(api_resource.udf)
            self.__dict__.update(attributes)

    @lazyprop
    def udf_backward_map(self):
        return {self.udf_map[key]: key for key in self.udf_map}

    def reset_assigner(self):
        self.assigner = AssignLogger(self)

    def set_udf(self, name, value, from_unit=None, to_unit=None):
        if from_unit:
            units = UnitConversion()
            value = units.convert(value, from_unit, to_unit)
        if name in self.udf_backward_map:
            # Assign existing instance variable
            # Log for assignment of instance variables are handled in
            # step_repository.commit()
            self.__dict__[self.udf_backward_map[name]] = value
        else:
            # There is no mapped instance variable for this udf.
            # Log the assignment right away
            self.api_resource.udf[
                name] = self.assigner.register_assign(name, value)

    def get_udf(self, name):
        return self.api_resource.udf[name]

    def updated_rest_resource(self, original_rest_resource, updated_fields):
        """
        :param original_rest_resource: The rest resource in the state as in the api cache
        :return: An updated rest resource according to changes in this instance of Analyte
        """
        _updated_rest_resource = original_rest_resource

        # Update udf values
        values_by_udf_names = {self.udf_map[key]: self.__dict__[key]
                               for key in self.udf_map if key in updated_fields}
        # Retrieve fields that are updated, only these field should be included
        # in the rest update
        for key in values_by_udf_names:
            value = values_by_udf_names[key]
            _updated_rest_resource.udf[
                key] = self.assigner.register_assign(key, value)

        return _updated_rest_resource

    def commit(self):
        self.api_resource.put()


class DomainObjectWithUdfsMixin(DomainObjectMixin):
    """
    A mixin to add to domain objects that have UDFs.

    NOTE: This will eventually replace the Udf mixin above with the following changes:
        - Only uses the `automap` feature, doesn't expect a UDF map from the caller
        - Remembers the original state of the UDFs as a basis of comparison when updating
        - Udfs can be assigned directly without using set_udf, which will lead to the value
          being updated.

    Until the mixin has entirely replaced it, both will be available.
    """
    def __init__(self, udfs=None):
        """
        :param udfs: A dictionary of user defined fields.
        """
        if udfs:
            attributes, self.udfs_to_attributes = self.create_automap(udfs)
            self.__dict__.update(attributes)

    @classmethod
    def create_automap(cls, original_udfs):
        """
        Given a dictionary of UDFs, returns a new dictionary that uses Python naming conventions instead.

        Also returns a dictionary mapping from the original UDF name back to the attribute name, which can
        be used to synchronize the two if needed.
        """
        attributes = dict()
        # A dictionary that matches from Python attributes back to the api
        # resource UDFs.
        udfs_to_attributes = dict()
        for key, value in original_udfs.items():
            attrib_name = cls.automap_name(key)
            attributes[attrib_name] = value
            udfs_to_attributes[key] = attrib_name
        return attributes, udfs_to_attributes

    @classmethod
    def automap_name(cls, original_udf_name):
        """
        Maps a UDF name from Clarity to one that matches Python naming conventions

        Example: 'Fragment Lower (bp)' => 'fragment_lower_bp'
        """
        new_name = original_udf_name.lower().replace(" ", "_")
        # Get rid of all non-alphanumeric characters
        new_name = re.sub("\W+", "", new_name)
        return "udf_{}".format(new_name)
