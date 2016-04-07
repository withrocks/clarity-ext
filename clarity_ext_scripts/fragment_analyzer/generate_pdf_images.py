from clarity_ext.extensions import GeneralExtension
from clarity_ext.domain import Plate
from clarity_ext.pdf import PdfSplitter


class Extension(GeneralExtension):
    def execute(self):
        """
        Splits a PDF file into one PDF per sample according to this spec:
          * 10 pages skipped
          * Samples in the order A1, B1 (DOWN_FIRST), one sample per page
        """
        # The context has access to a local version of the in file (actually downloaded if needed):
        page = 10  # Start on page 10 (zero indexed)
        splitter = PdfSplitter(self.context.local_shared_file("Fragment Analyzer PDF File"))

        # Go through each well in the plate, splitting
        for well in self.context.plate.enumerate_wells(order=Plate.DOWN_FIRST):
            if well.artifact_id:
                self.logger.debug("{} is on page {}".format(well, page + 1))
                filename = "{}_{}.pdf".format(well.artifact_id, well.get_key().replace(":", "_"))
                splitter.split(page, filename)
            page += 1

        # TODO: Make this implicit
        splitter.close()

    def integration_tests(self):
        yield self.test("24-3649")

