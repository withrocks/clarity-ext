from clarity_ext.extensions import GeneralExtension
from clarity_ext.domain import Container
from clarity_ext.pdf import PdfSplitter


class Extension(GeneralExtension):
    def execute(self):
        # The pdf file has the following structure:
        #   - page 1: overview
        #   - page 2-n: page per non-empty sample in the container
        page = 1  # Start on page 10 (zero indexed)

        # NOTE: It seems that the 'required' string is required(!) Was it added to the name or does this
        # come from the system?
        splitter = PdfSplitter(self.context.local_shared_file("TapeStation Report PDF (required)"))

        for well in self.context.input_container.enumerate_wells(order=Container.DOWN_FIRST):
            if not well.is_empty:
                self.logger.debug("{} is on page {}".format(well, page + 1))
                filename = "{}_{}.pdf".format(well.artifact_id, well.get_key().replace(":", "_"))
                splitter.split(page, filename)
                page += 1

        # TODO: Make this implicit
        splitter.close()

    def integration_tests(self):
        yield self.test("24-4026")  # Tube strip

