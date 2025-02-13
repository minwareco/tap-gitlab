#!/usr/bin/env python

from setuptools import setup
import os

UTILS_VERSION = "22f493552c4eb46b2b5a6d98d7acacd9fb7edf68"

setup(name='tap-gitlab',
      version='0.9.15',
      description='Singer.io tap for extracting data from the GitLab API',
      author='Meltano Team && Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_gitlab'],
      install_requires=[
          'singer-python==6.0.1',
          'requests==2.32.0',
          'strict-rfc3339==0.7',
          'backoff==2.2.1',
          'psutil==5.8.0',
          'minware-singer-utils@git+https://{}@github.com/minwareco/minware-singer-utils.git{}'.format(
              os.environ.get("GITHUB_TOKEN", ""),
              UTILS_VERSION
          )
      ],
      entry_points='''
          [console_scripts]
          tap-gitlab=tap_gitlab:main
      ''',
      packages=['tap_gitlab'],
      package_data = {
          'tap_gitlab/schemas': [
            "branches.json",
            "commits.json",
            "issues.json",
            "milestones.json",
            "projects.json",
            "users.json",
            "groups.json",
            "merge_requests.json",
            "project_members.json",
            "group_members.json",
            "project_labels.json",
            "group_labels.json",
            "tags.json",
            "releases.json",
            "vulnerabilities.json",
            "project_variables.json",
            "group_variables.json"
          ],
      },
      include_package_data=True,
)
