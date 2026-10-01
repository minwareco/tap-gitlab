import unittest
from unittest.mock import MagicMock
import sys

mock_singer = MagicMock()
mock_singer.utils = MagicMock()
mock_singer.metadata = MagicMock()
mock_singer.catalog = MagicMock()
mock_singer.schema = MagicMock()
mock_singer.transform = MagicMock()

sys.modules['singer'] = mock_singer
sys.modules['singer.utils'] = mock_singer.utils
sys.modules['singer.metadata'] = mock_singer.metadata
sys.modules['singer.catalog'] = mock_singer.catalog
sys.modules['singer.schema'] = mock_singer.schema
sys.modules['singer.transform'] = mock_singer.transform

sys.modules['pytz'] = MagicMock()
sys.modules['strict_rfc3339'] = MagicMock()
sys.modules['backoff'] = MagicMock()
sys.modules['psutil'] = MagicMock()
sys.modules['minware_singer_utils'] = MagicMock()

from tap_gitlab import is_gitlab_com_host


class TestIsGitlabComHost(unittest.TestCase):
    def test_gitlab_com_and_subdomains(self):
        self.assertTrue(is_gitlab_com_host('gitlab.com'))
        self.assertTrue(is_gitlab_com_host('GitLab.com'))
        self.assertTrue(is_gitlab_com_host('foo.gitlab.com'))

    def test_lookalike_and_self_hosted(self):
        self.assertFalse(is_gitlab_com_host('notgitlab.com'))
        self.assertFalse(is_gitlab_com_host('gitlab.com.example.org'))
        self.assertFalse(is_gitlab_com_host('gitlab.example.com'))

    def test_host_with_port(self):
        self.assertTrue(is_gitlab_com_host('gitlab.com:443'))
        self.assertFalse(is_gitlab_com_host('gitlab.example.com:8443'))

    def test_missing_hostname(self):
        self.assertFalse(is_gitlab_com_host(None))
        self.assertFalse(is_gitlab_com_host(''))


if __name__ == '__main__':
    unittest.main()
