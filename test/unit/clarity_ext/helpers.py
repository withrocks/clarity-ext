from clarity_ext.domain import Container, Analyte
from mock import MagicMock


def fake_analyte(container_id, artifact_id, sample_id, artifact_name, well, udfs):
    """
    Creates a fake analyte

    TODO: Consider setting the domain objects only (not the rest resources) and
    create tests for the mapping only
    """
    container_resource_mock = MagicMock(name="Container")
    container_resource_mock.samples = None
    container_resource_mock.id = container_id
    container_resource_mock.type.name = "96 well plate"
    container_resource_mock.type.x_dimension = {"size": 12}
    container_resource_mock.type.y_dimension = {"size": 8}

    container = Container.create_from_rest_resource(resource=container_resource_mock, artifacts=[])

    artifact_resource_mock = MagicMock()
    artifact_resource_mock.id = artifact_id

    sample_resource_mock = MagicMock(name="Sample")
    sample_resource_mock.id = sample_id
    artifact_resource_mock.samples = [sample_resource_mock]
    artifact_resource_mock.name = artifact_name
    artifact_resource_mock.type = "Analyte"
    artifact_resource_mock.location = MagicMock(), well
    artifact_resource_mock.udf = udfs

    return Analyte(artifact_resource_mock, container)

