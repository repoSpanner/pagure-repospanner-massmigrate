#!/bin/env python

import argparse
import logging
import os
import re
import subprocess
import time
import traceback

import pygit2


_PAGURE_GLOBALS = {}


def parse_args():
    """ Parse command line arguments for this script. """
    parser = argparse.ArgumentParser(
        description="Migrate Pagure projects to repoSpanner")
    parser.add_argument(
        "--verbose", "-v",
        help="Increase logging verbosity",
        action="count", default=0)
    parser.add_argument(
        "--quiet", "-q",
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
        help="Prime the Pagure cache in this pseudo directory",
        action="store_true", default=False)
    parser.add_argument(
        "--reconfigure",
        help="Reconfigure Pagure to use this repo from repoSpanner",
        action="store_true", default=False)
    parser.add_argument(
        "--pagure-config",
        help="Pagure configuration file location",
        default="/etc/pagure/pagure.cfg")
    parser.add_argument(
        "region",
        help="repoSpanner region to migrate projects to")
    parser.add_argument(
        "project_match",
        help="Regular expression for which projects to migrate")
    return parser.parse_args()


def get_pagure_config(args=None):
    """ Load the Pagure configuration and return pagure_config. """
    if "config" not in _PAGURE_GLOBALS:
        if not args:
            raise Exception("Pagure config needs args")
        os.environ["PAGURE_CONFIG"] = args.pagure_config
        from pagure.config import config as pagure_config
        _PAGURE_GLOBALS["config"] = pagure_config
    return _PAGURE_GLOBALS["config"]


def get_pagure_session():
    """ Create a Pagure database session.

    This is done here, with the late import, so that pagure.config doesn't get
    imported before it's configured. """
    if "session" not in _PAGURE_GLOBALS:
        config = get_pagure_config()
        from pagure.lib import create_session
        _PAGURE_GLOBALS["session"] = create_session(config["DB_URL"])
    return _PAGURE_GLOBALS["session"]


def get_pagure_project():
    """ Get the Pagure Project class.

    This is done here, with the late import, so that pagure.config doesn't get
    imported before it's configured. """
    from pagure.lib.model import Project
    return Project


def pagure_get_session_and_project(reponame, user, namespace):
    """ Get and return a new sqlalchemy session and a Pagure project. """
    from pagure.lib import _get_project, create_session
    config = get_pagure_config()
    session = create_session(config["DB_URL"])
    project = _get_project(session, reponame, user, namespace)
    if project is None:
        raise ValueError("Project %s, %s, %s could not be found" % (reponame, user, namespace))
    return session, project


def runcmd(workdir, cmd, env=None, mayfail=False):
    """ Execute a command as a subprocess. """
    logging.debug("Running %s in workdir %s", cmd, workdir)
    func = subprocess.check_call
    if mayfail:
        func = subprocess.call
    if env:
        newenv = os.environ.copy()
        newenv.update(env)
        env = newenv
    func(
        cmd,
        cwd=workdir,
        env=env,
    )


def create_repos_in_repospanner(args, project):
    """ Create the repositories in repoSpanner. """
    logging.info("Creating repositories")
    from pagure.lib.git import create_project_repos
    create_project_repos(project, args.region, None, True)


def run_git_push(args, project):
    """ Set up and run git pushes. """
    try:
        # Temporarily mark this so we can use the helper functions
        project.repospanner_region = args.region
        # Clone
        _run_git_push(args, project)
    finally:
        # Make sure to reset this: reconfiguring comes later
        project.repospanner_region = None


def _run_git_push(args, project):
    """ Push the current repositories out to repoSpanner. """
    logging.info("Pushing repositories")

    from pagure.lib import REPOTYPES

    for repotype in REPOTYPES:
        logging.info(
            "Pushing repotype %s for project %s", repotype, project.fullname)

        currentdir = project.repopath(repotype)
        if currentdir is None:
            logging.info("Repotype not in use, skipping")
            continue

        repo = pygit2.Repository(currentdir)

        pagure_config = get_pagure_config()
        repourl, regioninfo = project.repospanner_repo_info(repotype)

        pushargs = [
            "--extra", "username", "releng",
            "--extra", "repotype", repotype,
            "--extra", "project_name", project.name,
            "--extra", "project_user", project.user.username if project.is_fork else "",
            "--extra", "project_namespace", project.namespace or "",
        ]

        environ = os.environ.copy()
        environ.update(
            {
                "USER": "pagure",
                "REPOBRIDGE_CONFIG": ":environment:",
                "REPOBRIDGE_BASEURL": regioninfo["url"],
                "REPOBRIDGE_CA": regioninfo["ca"],
                "REPOBRIDGE_CERT": regioninfo["push_cert"]["cert"],
                "REPOBRIDGE_KEY": regioninfo["push_cert"]["key"],
            }
        )
        logging.debug(
            "Pushing %s to %s (info %s)", currentdir, repourl, regioninfo)

        command = [
            "git",
            "-c",
            "protocol.ext.allow=always",
            "push",
            "ext::%s %s %s"
            % (
                pagure_config["REPOBRIDGE_BINARY"],
                " ".join(pushargs),
                project._repospanner_repo_name(repotype),
            ),
        ]
        command.extend([
            ref.shorthand
            for ref in repo.listall_reference_objects()
        ])
        subprocess.check_call(
            command, env=environ, cwd=currentdir,
        )


def repospanner_clone(project, repotype, set_config, target):
    """ Create a clone of a repoSpanner repo to filesystem.

    """
    pagure_config = get_pagure_config()
    repourl, regioninfo = project.repospanner_repo_info(repotype)

    command = [
        "git",
        "-c",
        "protocol.ext.allow=always",
        "clone",
        "ext::%s %s"
        % (
            pagure_config["REPOBRIDGE_BINARY"],
            project._repospanner_repo_name(repotype),
        ),
        target,
    ]
    environ = os.environ.copy()
    environ.update(
        {
            "USER": "pagure",
            "REPOBRIDGE_CONFIG": ":environment:",
            "REPOBRIDGE_BASEURL": regioninfo["url"],
            "REPOBRIDGE_CA": regioninfo["ca"],
            "REPOBRIDGE_CERT": regioninfo["push_cert"]["cert"],
            "REPOBRIDGE_KEY": regioninfo["push_cert"]["key"],
        }
    )
    with open(os.devnull, "w") as devnull:
        subprocess.check_call(
            command, stdout=devnull, stderr=subprocess.STDOUT, env=environ
        )

    repo = pygit2.Repository(target)
    if set_config:
        repo.config["repospanner.url"] = repourl
        repo.config["repospanner.cert"] = regioninfo["push_cert"]["cert"]
        repo.config["repospanner.key"] = regioninfo["push_cert"]["key"]
        repo.config["repospanner.cacert"] = regioninfo["ca"]
        repo.config["repospanner.enabled"] = True
    return repo


def prime_cache(args, project):
    """ Build or update the Pagure pseudo cache. """
    logging.info("Priming cache for %s", project.fullname)

    from pagure.lib import REPOTYPES
    pseudopath = get_pagure_config(args)["REPOSPANNER_PSEUDO_FOLDER"]

    for repotype in REPOTYPES:
        logging.info("Pulling repotype %s", repotype)
        currentdir = project.repopath(repotype)
        if currentdir is None:
            logging.info("Repotype not in use, skipping")
            continue
        cachedir = os.path.join(pseudopath, repotype, project.path)
        _, regioninfo = project.repospanner_repo_info(
            repotype, args.region)

        if os.path.exists(cachedir):
            env = {
                "USER": "pagure",
                "REPOBRIDGE_CONFIG": ":environment:",
                "REPOBRIDGE_BASEURL": regioninfo["url"],
                "REPOBRIDGE_CA": regioninfo["ca"],
                "REPOBRIDGE_CERT": regioninfo["push_cert"]["cert"],
                "REPOBRIDGE_KEY": regioninfo["push_cert"]["key"],
            }
            # This may fail if the repo is empty, and that is fine.
            # It will be populated when it gets filled by libgit2.
            runcmd(cachedir, ["git", "pull"], env, True)
        else:
            try:
                # Temporarily mark this so we can use the helper functions
                project.repospanner_region = args.region
                # Clone
                repospanner_clone(project, repotype, True, cachedir)
            finally:
                # Make sure to reset this: reconfiguring comes later
                project.repospanner_region = None


def reconfigure(args, session, project):
    """ Configure the project in the Pagure database to mark it's migrated. """
    logging.info(
        "Marking project %s as moved to repoSpanner", project.fullname)
    project.repospanner_region = args.region
    session.add(project)
    session.commit()


def run_one_project(args, reponame, user, namespace):
    """ Run the requested operations for a single project. """
    session, project = pagure_get_session_and_project(reponame, user, namespace)

    logging.info("Handling project %s", project.fullname)
    try:
        times = {}
        total_start = time.time()
        if args.create:
            create_start = time.time()
            create_repos_in_repospanner(args, project)
            times['create'] = time.time() - create_start
        push_start = time.time()
        run_git_push(args, project)
        times['push'] = time.time() - push_start
        if args.prime:
            prime_start = time.time()
            prime_cache(args, project)
            times['prime'] = time.time() - prime_start
        if args.reconfigure:
            reconf_start = time.time()
            reconfigure(args, session, project)
            times['reconfigure'] = time.time() - reconf_start
        times['total'] = time.time() - total_start
        time_msg = ", ".join(["%s: %f seconds" % (key, times[key])
                            for key in times])
        logging.info("Project %s done. Timing: %s", project.fullname, time_msg)
    finally:
        session.remove()


def match_and_run(args):
    """ Retrieves all projects, migrating matching ones. """
    matcher = re.compile(args.project_match)
    session = get_pagure_session()
    Project = get_pagure_project()

    query = session.query(Project).filter(Project.repospanner_region==None)

    logging.info("Starting processing")
    start = time.time()
    for project in query:
        if not matcher.match(project.fullname):
            logging.debug(
                "Skipping project %s due to no match", project.fullname)
            continue
        try:
            run_one_project(
                args,
                project.name,
                project.user if project.is_fork else None,
                project.namespace,
            )
        except Exception:
            traceback.print_exc()
            if args.failfast:
                raise SystemExit("Project failed with failfast")

    logging.info("Total time: %f", time.time() - start)


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
