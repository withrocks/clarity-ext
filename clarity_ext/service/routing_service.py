import logging
from fuzzywuzzy import fuzz
from clarity_ext import utils
import copy
from genologics.entities import Artifact


class RoutingService(object):
    def __init__(self, session, commit=True):
        self.logger = logging.getLogger(__name__)
        self.session = session
        self.commit = commit


    def build_plan(self, artifact_ids, assign_workflow_name, assign_stage_name):
        plan = dict()
        errors_entry = list()
        plan["errors"] = errors_entry
        plan["reroutes"] = reroutes = list()

        artifacts = [Artifact(self.session.api, id=artifact_id) for artifact_id in artifact_ids]
        artifacts = list(self.session.api.get_batch(artifacts))

        # TODO: Move to some utility
        def matches_by_ratio(search, values):
            def order_by_fuzz_ratio(current):
                return fuzz.ratio(search, current)
            return sorted(values, key=order_by_fuzz_ratio, reverse=True)

        def get_similar(search, values):
            import itertools
            matches = matches_by_ratio(search, values)
            return itertools.islice(matches, 3)

        def get_similar_workflows(workflow_name):
            active_workflows = [workflow.name for workflow in self.session.api.get_workflows()
                                if workflow.status == "ACTIVE"]
            return get_similar(workflow_name, active_workflows)

        # Validate that we can fetch the workflow
        assign_workflow = None
        assign_stage_entry = dict()

        assign_entry = dict()  # Everything will be assigned to the same
        try:
            assign_workflow = utils.single(self.session.api.get_workflows(name=assign_workflow_name))
            logging.info("Found workflow '{}' at {}".format(assign_workflow.name, assign_workflow.uri))
        except ValueError:
            similar_workflows = " OR ".join(get_similar_workflows(assign_workflow_name))
            errors_entry.append("Workflow not found. Closest matches: {}".format(similar_workflows))

        # Try to fetch the stage
        if assign_workflow:
            try:
                assign_stage = utils.single([stage for stage in assign_workflow.stages if stage.name == assign_stage_name])
                assign_entry['uri'] = assign_stage.uri
                assign_entry['name'] = "{}/{}".format(assign_workflow.name, assign_stage.name)
                assign_entry['type'] = "stage"  # TODO: Support being able to assign workflows only
                logging.info("Found stage '{}' in the workflow".format(assign_stage.uri))
            except ValueError:
                stage_names = [stage.name for stage in assign_workflow.stages]
                similar_stages = ", ".join(get_similar(assign_stage_name, stage_names))
                errors_entry.append("Stage '{}' not found. Closest matches: {}".format(assign_stage_name, similar_stages))

        for artifact in artifacts:
            current_entry = dict()
            reroutes.append(current_entry)
            current_entry['assign'] = assign_entries = list()
            current_entry['unassign'] = unassign_entries = list()

            assign_entries.append(copy.copy(assign_entry))

            current_entry['artifact'] = {
                "name": artifact.name,
                "uri": artifact.uri
            }
            queued_stages = [stage for stage, status, name
                             in artifact.workflow_stages_and_statuses if status == "QUEUED"]
            for stage in queued_stages:
                unassign_entry = {
                    "name": stage.name,
                    "uri": stage.uri
                }
                unassign_entries.append(unassign_entry)

            if len(queued_stages) == 0:
                self.logger.info("Artifact {} is not queued in any stage".format(artifact.id))

        return plan

    @staticmethod
    def build_reroute_message(reroute_infos):
        request = list()
        request.append('<rt:routing xmlns:rt="http://genologics.com/ri/routing">')

        def uri_attribute(uri):
            return "stage-uri" if "/stages/" in uri else "workflow-uri"

        for reroute_info in reroute_infos:
            for assign in reroute_info["assign"]:
                request.append('<assign {}="{}">'.format(uri_attribute(assign["uri"]), assign["uri"]))
                request.append('  <artifact uri="' + reroute_info["artifact"]["uri"] + '"/>')
                request.append('</assign>')

            for unassign in reroute_info["unassign"]:
                request.append('<unassign {}="{}">'.format(uri_attribute(unassign["uri"]), unassign["uri"]))
                request.append('  <artifact uri="' + reroute_info.artifact["uri"] + '"/>')
                request.append('</unassign>')

        request.append('</rt:routing>')
        return "\n".join(request)

    def route(self, reroute_infos):
        route_uri = self.session.api.get_uri("route", "artifacts")
        self.logger.info("Posting reroute message to {}".format(route_uri))
        reroute_request = self.build_reroute_message(reroute_infos)
        self.logger.info(reroute_request)
        if not self.commit:
            self.logger.info("Running with commit off. The message was not posted.")
        else:
            response = self.session.api.post(route_uri, reroute_request)
            self.logger.info(response)


class RerouteInfo(object):
    """Defines a workflow stage to route to and from"""
    def __init__(self, artifact, unassign, assign):
        """
        :param artifact: The artifact to change
        :param unassign: Any number of workflows or stages to unassign, iterable or a single instance
        :param assign: Any number of workflows or stages to assign, iterable or a single instance
        """
        self.artifact = artifact
        self.assign = assign
        self.unassign = unassign

    def __repr__(self):
        return "{}: {} => {}".format(self.artifact.id, self.unassign, self.assign)

