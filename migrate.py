#!/bin/env python

import argparse
import logging
import os
import re
import subprocess
import traceback


_PAGURE_GLOBALS = {}


def parse_args():
    """ Parse command line arguments for this script. """
    parser = argparse.ArgumentParser(
        description="Migrate Pagure projects to repoSpanner")
    parser.add_argument(
        "--verbose",
        help="Increase logging verbosity",
        action="count", default=0)
    parser.add_argument(
        "--quiet",
        help="Decreases logging verbosity",
        action="count", default=0)
    parser.add_argument(
        "--failfast", "-x",
        help="Abort the migration if a single project fails",
        action="store_true", default=False)
    parser.add_argument(
        "--create",
        help="Create the repositories in repoSpanner",
        action="store_true", default=False)
    parser.add_argument(
        "--prime",
        help="Prime the Pagure cache in this pseudo directory")
    parser.add_argument(
        "--reconfigure",
        help="Reconfigure Pagure to use this repo from repoSpanner",
        action="store_true", default=False)
    parser.add_argument(
        "--region",
        help="repoSpanner region to migrate projects to")
    parser.add_argument(
        "--pagure-config",
        help="Pagure configuration file location",
        default="/etc/pagure/pagure.cfg")
    parser.add_argument(
        "project-match",
        help="Regular expression for which projects to migrate")
    return parser.parse_args()


def get_pagure_config(args):
    """ Load the Pagure configuration and return pagure_config. """
    if "config" not in _PAGURE_GLOBALS:
        os.environ["PAGURE_CONFIG"] = args.pagure_config
        from pagure.config import config as pagure_config
        _PAGURE_GLOBALS["config"] = pagure_config
    return _PAGURE_GLOBALS["config"]


def get_pagure_session(args):
    """ Create a Pagure database session. """
    if "session" not in _PAGURE_GLOBALS:
        config = get_pagure_config(args)
        from pagure.lib import create_session
        _PAGURE_GLOBALS["session"] = create_session(config["DB_URL"])
    return _PAGURE_GLOBALS["session"]


def get_pagure_project():
    from pagure.lib.model import Project
    return Project


def runcmd(workdir, cmd):
    logging.debug("Running %s in workdir %s", cmd, workdir)
    subprocess.check_call(
        cmd,
        cwd=workdir,
    )


def create_repos_in_repospanner(args, project):
    logging.info("Creating repositories")
    from pagure.lib.git import create_project_repos
    create_project_repos(project, args.region, None, True)


def run_git_push(args, project):
    logging.info("Pushing repositories")

    from pagure.lib import REPOTYPES

    for repotype in REPOTYPES:
        logging.info(
            "Pushing repotype %s for project %s", repotype, project.fullname)

        currentdir = project.repopath(repotype)
        repourl, regioninfo = project.repospanner_repo_info(
            repotype, args.region)

        logging.debug(
            "Pushing %s to %s (info %s)", currentdir, repourl, regioninfo)

        cmd = [
            "git",
            "-c", "http.sslcainfo=%s" % regioninfo["ca"],
            "-c", "http.sslcert=%s" % regioninfo["push_cert"]["cert"],
            "-c", "http.sslkey=%s" % regioninfo["push_cert"]["key"],
            "push",
            repourl,
            "--mirror",
        ]
        runcmd(currentdir, cmd)


def prime_cache(args, project):
    pass


def reconfigure(args, project):
    logging.info(
        "Marking project %s as moved to repoSpanner", project.fullname)
    project.repospanner_region = args.region
    get_pagure_session(args).add(project)


def run_one_project(args, project):
    """ Run the requested operations for a single project. """
    logging.info("Handling project %s", project.fullname)
    if args.create:
        create_repos_in_repospanner(args, project)
    run_git_push(args, project)
    if args.prime:
        prime_cache(args, project)
    if args.reconfigure:
        reconfigure(args, project)


def match_and_run(args):
    """ Retrieves all projects, migrating matching ones. """
    matcher = re.compile(args.project_match)
    session = get_pagure_session(args)
    Project = get_pagure_project()

    query = session.query(Project).filter(Project.repospanner_region==None)

    for project in query:
        if not matcher.match(project.fullname):
            logging.info(
                "Skipping project %s due to no match", project.fullname)
            continue
        try:
            run_one_project(args, project)
        except Exception:
            traceback.print_exc()
            if args.failfast:
                raise SystemExit("Project failed with failfast")

    logging.info("Committing database transactions")
    get_pagure_session(args).commit()
    logging.info("Done")


def main():
    args = parse_args()
    verbosity = args.verbose - args.quiet

    if verbosity == 0:
        level = logging.INFO
    elif verbosity == 1:
        level = logging.DEBUG
    elif verbosity > 1:
        level = logging.NOTSET
    elif verbosity == -1:
        level = logging.WARNING
    elif verbosity == -2:
        level = logging.ERROR
    elif verbosity <= -2:
        level = logging.CRITICAL

    logging.basicConfig(level=level)

    config = get_pagure_config(args)
    if args.region not in config["REPOSPANNER_REGIONS"]:
        raise SystemExit("Invalid repoSpanner region name")
    match_and_run(args)


if __name__ == '__main__':
    main()
