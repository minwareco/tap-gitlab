#!/usr/bin/env python

from setuptools import setup
import os

UTILS_VERSION = "756eaa55ae05c44a58124a26e839698c2f5b78cc"

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
          
          'minware_singer_utils@git+https://{}github.com/minwareco/minware-singer-utils.git@{}'.format(
              "{}@".format(os.environ.get("GITHUB_TOKEN")) if os.environ.get("GITHUB_TOKEN") else "",
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
      tests_require=[
        "pytest",
        "mock",
    ],
    setup_requires=["pytest-runner"],
)
