import unittest
from clarity_ext.context import ExtensionContext


class TestUdf(unittest.TestCase):
    def test_can_set_any_udf(self):
        """
        Users can set UDFs that are either already defined on the Artifact or are defined in the process type for the
        output (the feature that makes them visible in the UI).

        In addition to this, users should be able to set any other UDF on the artifact, as you can do through the
        lab toolkit or directly through the UI.
        """
        context = ExtensionContext.create("24-1998")
        # NOTE: These do not have the udf "Amount (ng)" available
        for well in context.input_container.occupied:
            well.artifact.udf_map.force("FA Total conc. (ng/ul)", 21)
            context.update(well.artifact)
        context.commit()
