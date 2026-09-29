import unittest
from unittest.mock import MagicMock, patch
import sys

# Create comprehensive mocks for all singer dependencies, following the pattern
# used by the other tap_gitlab unit tests in this directory.
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

# Mock other dependencies
sys.modules['pytz'] = MagicMock()
sys.modules['strict_rfc3339'] = MagicMock()
sys.modules['backoff'] = MagicMock()
sys.modules['psutil'] = MagicMock()
sys.modules['minware_singer_utils'] = MagicMock()

# Import the function we're testing
from tap_gitlab import sync_branches


class TestSyncBranches(unittest.TestCase):

    def test_skips_branch_missing_commit_key(self):
        """
        GitLab occasionally returns a branch row with no 'commit' field at all
        (MW-12670). This used to raise KeyError: 'commit' and crash the whole
        ingest job. It should now log a warning and skip head-tracking for that
        one branch while still processing the rest.
        """
        rows = [
            {'name': 'main', 'commit': {'id': 'sha-main'}},
            {'name': 'broken-branch'},  # No 'commit' key at all
            {'name': 'feature', 'commit': {'id': 'sha-feature'}},
        ]

        with patch('tap_gitlab.gen_request', return_value=iter(rows)), \
             patch('tap_gitlab.CATALOG') as mock_catalog, \
             patch('tap_gitlab.LOGGER') as mock_logger:

            heads = sync_branches({'id': 123}, headsOnly=True)

        self.assertEqual(heads, {
            'refs/heads/main': 'sha-main',
            'refs/heads/feature': 'sha-feature',
        })
        self.assertNotIn('refs/heads/broken-branch', heads)
        mock_logger.warning.assert_called_once()
        warning_message = mock_logger.warning.call_args[0][0]
        self.assertIn('broken-branch', warning_message)
        self.assertIn('123', warning_message)

    def test_skips_branch_with_null_commit(self):
        """Same as above, but the 'commit' key is present with a None value."""
        rows = [
            {'name': 'main', 'commit': {'id': 'sha-main'}},
            {'name': 'broken-branch', 'commit': None},
        ]

        with patch('tap_gitlab.gen_request', return_value=iter(rows)), \
             patch('tap_gitlab.CATALOG'), \
             patch('tap_gitlab.LOGGER') as mock_logger:

            heads = sync_branches({'id': 123}, headsOnly=True)

        self.assertEqual(heads, {'refs/heads/main': 'sha-main'})
        mock_logger.warning.assert_called_once()

    def test_all_valid_branches_no_warning(self):
        rows = [
            {'name': 'main', 'commit': {'id': 'sha-main'}},
            {'name': 'feature', 'commit': {'id': 'sha-feature'}},
        ]

        with patch('tap_gitlab.gen_request', return_value=iter(rows)), \
             patch('tap_gitlab.CATALOG'), \
             patch('tap_gitlab.LOGGER') as mock_logger:

            heads = sync_branches({'id': 123}, headsOnly=True)

        self.assertEqual(heads, {
            'refs/heads/main': 'sha-main',
            'refs/heads/feature': 'sha-feature',
        })
        mock_logger.warning.assert_not_called()


if __name__ == '__main__':
    unittest.main()
