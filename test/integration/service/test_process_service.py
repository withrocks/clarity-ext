from unittest import TestCase


class TestProcessService(TestCase):
    def test_can_query_queue_by_step_name(self):
        """Can fetch the queue for a process by the step name"""
        from clarity_ext.service.process_service import ProcessService
        process_svc = ProcessService()
        process_svc.get_queue(protocol="TestOnly-dev-protocol1", step="TestOnly - steinar - Logging")
