#!/bin/env python

import logging
import os
import re
import shutil
import subprocess
import sys
import traceback


os.environ["PAGURE_CONFIG"] = "/etc/pagure/pagure.cfg"


from pagure.config import config as pagure_config
from pagure.lib import create_session, REPOTYPES
from pagure.lib.model import Project


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


def prime_cache(project):
    """ Build or update the Pagure pseudo cache. """
    logging.info("Priming cache for %s", project.fullname)

    pseudopath = pagure_config["REPOSPANNER_PSEUDO_FOLDER"]

    for repotype in REPOTYPES:
        logging.info("Pulling repotype %s", repotype)
        currentdir = project.repopath(repotype)
        if currentdir is None:
            logging.info("Repotype not in use, skipping")
            continue
        cachedir = os.path.join(pseudopath, repotype, project.path)

        # Clone
        tempdir = cachedir + '.cacheprime'
        project._repospanner_clone(repotype, True, tempdir)

        if os.path.exists(cachedir):
            if os.path.exists(cachedir + ".old"):
                raise Exception("Error: old cachedir already existed: %s.old" % cachedir)
            os.rename(cachedir, cachedir + ".old")
        os.rename(tempdir, cachedir)
        shutil.rmtree(cachedir + ".old")


def main():
    if len(sys.argv) != 2:
        raise SystemExit("Usage: %s <project-match>" % sys.argv[0])

    logging.basicConfig(level=logging.INFO)

    matcher = re.compile(sys.argv[1])

    session = create_session(pagure_config["DB_URL"])

    query = session.query(Project).filter(Project.repospanner_region!=None)

    logging.info("Starting processing")
    for project in query:
        if not matcher.match(project.fullname):
            logging.debug(
                "Skipping project %s due to no match", project.fullname)
            continue
        try:
            prime_cache(project)
        except Exception:
            traceback.print_exc()


if __name__ == '__main__':
    main()
