from clarity_ext.domain.udf import DomainObjectWithUdfMixin, UdfMapping


class Process(DomainObjectWithUdfMixin):
    """Represents a Process (step)"""

    def __init__(self, api_resource, process_id, technician, udf_map):
        super(Process, self).__init__(api_resource, process_id, udf_map)
        self.technician = technician

    @staticmethod
    def create_from_rest_resource(resource):
        udf_map = UdfMapping(resource.udf)
        ret = Process(resource, resource.id, resource.technician, udf_map)
        return ret


class ProcessType(object):
    # TODO: The process type defined in pip/genologics doesn't have all the entries defined
    # We currently need only a few of these, but it would make sense to update
    # it there instead.

    def __init__(self, process_outputs):
        self.process_outputs = process_outputs

    @staticmethod
    def create_from_resource(process_type):
        outputs = process_type.root.findall("process-output")
        process_outputs = [ProcessOutput.create_from_element(
            output) for output in outputs]
        return ProcessType(process_outputs)


class ProcessOutput(object):
    """Defines the artifact output generated in a process"""

    def __init__(self, artifact_type, output_generation_type, field_definitions):
        self.artifact_type = artifact_type
        self.output_generation_type = output_generation_type
        self.field_definitions = field_definitions

    @staticmethod
    def create_from_element(element):
        fields = [f.attrib["name"]
                  for f in element.findall("field-definition")]
        return ProcessOutput(element.find("artifact-type").text,
                             element.find("output-generation-type").text,
                             fields)

    def __repr__(self):
        return "{}/{}".format(self.artifact_type, self.output_generation_type)
