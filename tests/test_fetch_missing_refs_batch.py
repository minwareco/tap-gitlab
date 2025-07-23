import unittest
from unittest.mock import MagicMock, patch
import sys
import logging

# Create comprehensive mocks for all singer dependencies
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
from tap_gitlab import fetch_missing_refs_batch


class ConfigurableGitLocal:
    """Test double for GitLocal that can be configured to succeed or fail"""
    
    def __init__(self, should_fail=False, exception_msg="Test failure"):
        self.should_fail = should_fail
        self.exception_msg = exception_msg
        self.fetch_calls = []
        
    def fetchMultipleCommits(self, repo_path, shas, source):
        """Track calls and optionally fail"""
        self.fetch_calls.append({
            'repo_path': repo_path,
            'shas': shas,
            'source': source
        })
        
        if self.should_fail:
            raise Exception(self.exception_msg)


class TestFetchMissingRefsBatch(unittest.TestCase):
    
    def setUp(self):
        """Set up test logging to verify log messages"""
        self.log_messages = []
        
        # Patch the LOGGER to capture log messages
        self.logger_patch = patch('tap_gitlab.LOGGER')
        self.mock_logger = self.logger_patch.start()
        
        # Capture info and warning calls
        self.mock_logger.info.side_effect = lambda msg, *args: self.log_messages.append(('INFO', msg % args if args else msg))
        self.mock_logger.warning.side_effect = lambda msg, *args: self.log_messages.append(('WARNING', msg % args if args else msg))
        
    def tearDown(self):
        """Clean up patches"""
        self.logger_patch.stop()
        
    def test_empty_input_returns_empty_list(self):
        """Test that empty input returns empty list without calling git"""
        gitLocal = ConfigurableGitLocal()
        
        result = fetch_missing_refs_batch(gitLocal, 'test/repo', [])
        
        self.assertEqual(result, [])
        self.assertEqual(len(gitLocal.fetch_calls), 0)
        self.assertEqual(len(self.log_messages), 0)
        
    def test_successful_batch_fetch(self):
        """Test successful batch fetch returns input refs"""
        gitLocal = ConfigurableGitLocal(should_fail=False)
        missing_refs = [
            {'ref': 'refs/heads/feature1', 'sha': 'abc123'},
            {'ref': 'refs/heads/feature2', 'sha': 'def456'},
            {'ref': 'refs/heads/feature3', 'sha': 'ghi789'}
        ]
        
        result = fetch_missing_refs_batch(gitLocal, 'test/repo', missing_refs)
        
        # Should return the same refs
        self.assertEqual(result, missing_refs)
        
        # Should have made one fetch call
        self.assertEqual(len(gitLocal.fetch_calls), 1)
        call = gitLocal.fetch_calls[0]
        self.assertEqual(call['repo_path'], 'test/repo')
        self.assertEqual(call['shas'], ['abc123', 'def456', 'ghi789'])
        self.assertEqual(call['source'], 'gitlab')
        
        # Check log messages
        self.assertEqual(len(self.log_messages), 2)
        self.assertEqual(self.log_messages[0][0], 'INFO')
        self.assertIn('Attempting to batch fetch 3 missing refs', self.log_messages[0][1])
        self.assertEqual(self.log_messages[1][0], 'INFO')
        self.assertIn('Successfully batch fetched 3 refs', self.log_messages[1][1])
        
    def test_failed_batch_fetch_returns_empty(self):
        """Test failed batch fetch returns empty list and logs warning"""
        gitLocal = ConfigurableGitLocal(should_fail=True, exception_msg="Network error")
        missing_refs = [
            {'ref': 'refs/heads/feature1', 'sha': 'abc123'}
        ]
        
        result = fetch_missing_refs_batch(gitLocal, 'test/repo', missing_refs)
        
        # Should return empty list on failure
        self.assertEqual(result, [])
        
        # Should have attempted the fetch
        self.assertEqual(len(gitLocal.fetch_calls), 1)
        
        # Check log messages
        self.assertEqual(len(self.log_messages), 2)
        self.assertEqual(self.log_messages[0][0], 'INFO')
        self.assertIn('Attempting to batch fetch 1 missing refs', self.log_messages[0][1])
        self.assertEqual(self.log_messages[1][0], 'WARNING')
        self.assertIn('Batch fetch failed with exception', self.log_messages[1][1])
        self.assertIn('Network error', self.log_messages[1][1])
        
    def test_extracts_shas_correctly(self):
        """Test that SHAs are extracted correctly from ref info"""
        gitLocal = ConfigurableGitLocal()
        missing_refs = [
            {'ref': 'refs/heads/main', 'sha': '1234567890abcdef'},
            {'ref': 'refs/tags/v1.0', 'sha': 'fedcba0987654321'},
            {'ref': 'refs/merge-requests/42/head', 'sha': 'aabbccddeeff0011'}
        ]
        
        fetch_missing_refs_batch(gitLocal, 'gitlab/project', missing_refs)
        
        # Verify correct SHAs were passed
        self.assertEqual(len(gitLocal.fetch_calls), 1)
        self.assertEqual(gitLocal.fetch_calls[0]['shas'], [
            '1234567890abcdef',
            'fedcba0987654321', 
            'aabbccddeeff0011'
        ])
        
    def test_handles_large_batch(self):
        """Test handling of large number of missing refs"""
        gitLocal = ConfigurableGitLocal()
        
        # Create 1000 missing refs
        missing_refs = [
            {'ref': f'refs/heads/branch{i}', 'sha': f'sha{i:04d}'}
            for i in range(1000)
        ]
        
        result = fetch_missing_refs_batch(gitLocal, 'test/repo', missing_refs)
        
        # Should handle large batch successfully
        self.assertEqual(len(result), 1000)
        self.assertEqual(len(gitLocal.fetch_calls[0]['shas']), 1000)
        
        # Check logging mentions the large number
        self.assertIn('1000 missing refs', self.log_messages[0][1])
        
    def test_preserves_ref_order(self):
        """Test that refs are returned in the same order as input"""
        gitLocal = ConfigurableGitLocal()
        missing_refs = [
            {'ref': 'refs/heads/z-branch', 'sha': 'zzz'},
            {'ref': 'refs/heads/a-branch', 'sha': 'aaa'},
            {'ref': 'refs/heads/m-branch', 'sha': 'mmm'}
        ]
        
        result = fetch_missing_refs_batch(gitLocal, 'test/repo', missing_refs)
        
        # Order should be preserved
        self.assertEqual(result[0]['ref'], 'refs/heads/z-branch')
        self.assertEqual(result[1]['ref'], 'refs/heads/a-branch')
        self.assertEqual(result[2]['ref'], 'refs/heads/m-branch')


if __name__ == '__main__':
    unittest.main()