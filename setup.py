#!/usr/bin/env python

from setuptools import setup
import os

setup(name='tap-gitlab',
      version='0.9.15',
      description='Singer.io tap for extracting data from the GitLab API',
      author='Meltano Team && Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_gitlab'],
      install_requires=[
          'singer-python==5.9.1',
          'requests==2.20.0',
          'strict-rfc3339==0.7',
          'backoff==1.8.0',
          'psutil==5.8.0',
          'gitlocal@git+https://{}@github.com/minwareco/gitlocal.git@cffdf75345db9024b1026cb2b2ea31e35d135ce0'.format(os.environ.get("GITHUB_TOKEN", ""))
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
          ],
      },
      include_package_data=True,
)
