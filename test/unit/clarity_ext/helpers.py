from clarity_ext.domain import Container, Analyte, Well, PlatePosition, Sample


def fake_analyte(container_id, artifact_id, sample_id, analyte_name, well_key, **kwargs):
    """
    Creates a fake Analyte domain object

    :container_id: The ID of the Container
    :artifact_id: The ID of the Artifact
    :sample_id: The ID of the Sample
    :analyte_name: The name of the Analyte
    :well_key: The locator key for the well, e.g. "A1"
    :kwargs: Any UDF. Use the key names specified in the udf_map being used.
    """
    container = Container(container_type=Container.CONTAINER_TYPE_96_WELLS_PLATE)
    container.id = container_id
    pos = PlatePosition.create(well_key)
    well = Well(pos, container)
    sample = Sample(id=sample_id)
    analyte = Analyte(container, analyte_name, well, sample, **kwargs)
    analyte.id = artifact_id
    return analyte

