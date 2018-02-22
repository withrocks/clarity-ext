from __future__ import print_function
import requests_cache
import re


class ReportingService(object):
    def __init__(self, session, use_cache):
        self.session = session
        if use_cache:
            # TODO: The cache is being ignored
            cache_name = "reporting-svc-cache"
            requests_cache.configure(cache_name)

    def create_project_report(self, ignore_udf, ignore_project):
        """Creates a project report, where every project is listed in a row and each UDF is a column.

        The report is written to stdout.
        """
        projects = self.session.api.get_projects()
        used_udfs = set()
        for project in projects:
            for k, v in project.udf.items():
                used_udfs.add(k)

        ignore_udf_patterns = [re.compile(p) for p in ignore_udf]
        ignore_project_patterns = [re.compile(p) for p in ignore_project]

        unique_headers = set()
        for udf in used_udfs:
            if any(pattern.match(udf) for pattern in ignore_udf_patterns):
                continue
            unique_headers.add(udf)
        unique_headers = list(sorted(unique_headers))

        # Print header line
        print("\t".join(["Project", "Opened", "Closed"] + list(unique_headers)))

        def keep_project(project):
            if any(pattern.match(project.name) for pattern in ignore_project_patterns):
                return False
            return True

        def massage_value(obj):
            if not obj:
                return ""
            else:
                return u"{}".format(obj)

        # Print values
        for project in filter(keep_project, projects):
            row = [project.name, project.open_date, project.close_date]
            for header in unique_headers:
                if header in project.udf:
                    value = project.udf[header]
                    if isinstance(value, basestring):
                        value = value.replace("\t", r"\t")
                        value = value.replace("\n", r"\n")
                    row.append(value)
                else:
                    row.append(None)
            print(u"\t".join(map(massage_value, row)))

