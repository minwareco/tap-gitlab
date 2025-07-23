import unittest
from unittest.mock import MagicMock, patch, call
import sys

# Create comprehensive mocks for all singer imports
mock_singer = MagicMock()
mock_singer.utils.now = MagicMock(return_value="2024-01-01T00:00:00Z")
mock_singer.write_state = MagicMock()
mock_singer.write_record = MagicMock()
mock_singer.write_schema = MagicMock()
mock_singer.metadata = MagicMock()
mock_singer.metadata.to_map = MagicMock(return_value={})
mock_singer.Transformer = MagicMock
mock_singer.transform = MagicMock(side_effect=lambda x, *args: x)  # Pass through

sys.modules['singer'] = mock_singer
sys.modules['singer.utils'] = mock_singer.utils
sys.modules['singer.metadata'] = mock_singer.metadata
sys.modules['singer.transform'] = mock_singer.transform

# Mock other dependencies
sys.modules['pytz'] = MagicMock()
sys.modules['strict_rfc3339'] = MagicMock()
sys.modules['backoff'] = MagicMock()
sys.modules['psutil'] = MagicMock()
sys.modules['minware_singer_utils'] = MagicMock()

# Mock other dependencies that might be imported
import asyncio
sys.modules['asyncio'] = asyncio

# Mock git-related modules
sys.modules['dateutil'] = MagicMock()
sys.modules['dateutil.parser'] = MagicMock()

# Import after all mocking is complete
from tap_gitlab import sync_commit_files, process_head_commits, fetch_missing_refs_batch


class IntegrationGitLocal:
    """GitLocal mock that simulates missing refs and batch fetch"""
    
    def __init__(self):
        self.local_commits = {
            'exists1': True,
            'exists2': True,
            'missing1': False,
            'missing2': False,
            'missing3': False
        }
        self.batch_fetch_called = False
        self.single_fetch_calls = []
        
        # Commit data for when commits are fetched
        self.commit_data = {
            'exists1': {'sha': 'exists1', 'parents': []},
            'exists2': {'sha': 'exists2', 'parents': [{'sha': 'exists1'}]},
            'missing1': {'sha': 'missing1', 'parents': []},
            'missing2': {'sha': 'missing2', 'parents': []},
            'missing3': {'sha': 'missing3', 'parents': [{'sha': 'missing2'}]}
        }
        
    def hasLocalCommit(self, repo_path, sha, source):
        """Check if commit exists locally"""
        return self.local_commits.get(sha, False)
        
    def fetchMultipleCommits(self, repo_path, shas, source):
        """Simulate batch fetch - make commits available"""
        self.batch_fetch_called = True
        # After batch fetch, all commits become available
        for sha in shas:
            self.local_commits[sha] = True
            
    def getCommitsFromHeadPyGit(self, repo_path, head_sha, limit, offset, skipAtCommits):
        """Return commit data"""
        if head_sha in self.commit_data:
            commit = self.commit_data[head_sha]
            if head_sha not in skipAtCommits:
                return [commit]
        return []


class TestSyncCommitFilesIntegration(unittest.TestCase):
    
    def setUp(self):
        """Set up test environment"""
        # Mock the catalog and state
        self.mock_catalog = MagicMock()
        self.mock_stream = MagicMock()
        self.mock_stream.is_selected.return_value = True
        self.mock_stream.metadata = []
        self.mock_catalog.get_stream.return_value = self.mock_stream
        
        # Patch CATALOG and STATE at module level
        self.catalog_patch = patch('tap_gitlab.CATALOG', self.mock_catalog)
        self.state_patch = patch('tap_gitlab.STATE', {})
        self.resources_patch = patch('tap_gitlab.RESOURCES', {
            'refs': {'schema': {}}
        })
        
        self.catalog_patch.start()
        self.state_patch.start()
        self.resources_patch.start()
        
        # Patch logger
        self.logger_patch = patch('tap_gitlab.LOGGER')
        self.mock_logger = self.logger_patch.start()
        self.log_messages = []
        
        def capture_log(level):
            def log_func(msg, *args):
                self.log_messages.append((level, msg % args if args else msg))
            return log_func
            
        self.mock_logger.info.side_effect = capture_log('INFO')
        self.mock_logger.warning.side_effect = capture_log('WARNING')
        self.mock_logger.debug.side_effect = capture_log('DEBUG')
        
        # Patch async functions
        self.async_patch = patch('tap_gitlab.getChangedfilesForCommits', 
                                return_value=[])
        self.async_patch.start()
        
    def tearDown(self):
        """Clean up patches"""
        self.catalog_patch.stop()
        self.state_patch.stop()
        self.resources_patch.stop()
        self.logger_patch.stop()
        self.async_patch.stop()
        
    def test_batch_fetch_integration(self):
        """Test the full flow with missing refs that trigger batch fetch"""
        project = {
            'id': '123',
            'path_with_namespace': 'test/project'
        }
        
        # Heads include both existing and missing commits
        heads = [
            {'ref': 'refs/heads/main', 'sha': 'exists1'},
            {'ref': 'refs/heads/feature1', 'sha': 'exists2'},
            {'ref': 'refs/heads/feature2', 'sha': 'missing1'},
            {'ref': 'refs/heads/feature3', 'sha': 'missing2'},
            {'ref': 'refs/heads/feature4', 'sha': 'missing3'}
        ]
        
        gitLocal = IntegrationGitLocal()
        
        # Run sync_commit_files
        sync_commit_files(project, heads, gitLocal, commits_only=False)
        
        # Verify batch fetch was called
        self.assertTrue(gitLocal.batch_fetch_called)
        
        # Check log messages for the flow
        log_msgs = [msg[1] for msg in self.log_messages]
        
        # Should log missing refs
        missing_ref_logs = [msg for msg in log_msgs if 'MISSING REF/COMMIT' in msg]
        self.assertEqual(len(missing_ref_logs), 3)
        
        # Should log batch fetch attempt
        batch_attempt_logs = [msg for msg in log_msgs if 'Found 3 missing refs' in msg]
        self.assertEqual(len(batch_attempt_logs), 1)
        
        # Should log successful batch fetch
        batch_success_logs = [msg for msg in log_msgs if 'Batch fetch succeeded' in msg]
        self.assertEqual(len(batch_success_logs), 1)
        
        # Should process the batch-fetched refs
        processing_logs = [msg for msg in log_msgs if 'Processing batch-fetched ref' in msg]
        self.assertEqual(len(processing_logs), 3)
        
    def test_batch_fetch_failure_handling(self):
        """Test handling when batch fetch fails"""
        project = {
            'id': '456',
            'path_with_namespace': 'test/project2'
        }
        
        heads = [
            {'ref': 'refs/heads/broken', 'sha': 'missing1'}
        ]
        
        # Create a GitLocal that will fail batch fetch
        gitLocal = IntegrationGitLocal()
        
        # Make fetchMultipleCommits raise an exception
        def failing_fetch(*args):
            gitLocal.batch_fetch_called = True
            raise Exception("Network timeout")
            
        gitLocal.fetchMultipleCommits = failing_fetch
        
        # Run sync_commit_files
        sync_commit_files(project, heads, gitLocal, commits_only=False)
        
        # Verify batch fetch was attempted
        self.assertTrue(gitLocal.batch_fetch_called)
        
        # Check for failure log
        log_msgs = [msg[1] for msg in self.log_messages]
        failure_logs = [msg for msg in log_msgs if 'Batch fetch failed for 1 refs' in msg]
        self.assertEqual(len(failure_logs), 1)
        
    def test_no_missing_refs_skips_batch_fetch(self):
        """Test that batch fetch is not called when all refs exist"""
        project = {
            'id': '789',
            'path_with_namespace': 'test/project3'
        }
        
        # All commits exist locally
        heads = [
            {'ref': 'refs/heads/main', 'sha': 'exists1'},
            {'ref': 'refs/heads/develop', 'sha': 'exists2'}
        ]
        
        gitLocal = IntegrationGitLocal()
        
        # Run sync_commit_files
        sync_commit_files(project, heads, gitLocal, commits_only=False)
        
        # Batch fetch should not be called
        self.assertFalse(gitLocal.batch_fetch_called)
        
        # Should not have any missing ref logs
        log_msgs = [msg[1] for msg in self.log_messages]
        missing_ref_logs = [msg for msg in log_msgs if 'missing refs' in msg.lower()]
        self.assertEqual(len(missing_ref_logs), 0)


if __name__ == '__main__':
    unittest.main()