import unittest
from unittest.mock import MagicMock
import sys

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
from tap_gitlab import process_head_commits


class FakeGitLocal:
    """Fake GitLocal that returns predetermined commit data for testing"""
    
    def __init__(self, commit_graph):
        """
        commit_graph: dict mapping sha to commit data
        Each commit should have 'sha' and 'parents' list
        """
        self.commit_graph = commit_graph
        self.calls = []  # Track calls for verification
        
    def getCommitsFromHeadPyGit(self, repo_path, head_sha, limit, offset, skipAtCommits):
        """Return commits based on our test graph with proper pagination simulation"""
        self.calls.append({
            'repo_path': repo_path,
            'head_sha': head_sha,
            'limit': limit,
            'offset': offset,
            'skipAtCommits': skipAtCommits
        })
        
        # Find all commits reachable from head_sha in chronological order
        all_reachable = []
        to_visit = [head_sha]
        visited = set()
        
        # Build full reachable commit list first
        while to_visit:
            sha = to_visit.pop(0)
            if sha in visited or sha not in self.commit_graph:
                continue
                
            visited.add(sha)
            commit = self.commit_graph[sha]
            all_reachable.append(commit)
            
            # Add parents to visit (they come after current commit chronologically)
            for parent in commit.get('parents', []):
                if parent['sha'] not in visited:
                    to_visit.append(parent['sha'])
        
        # Filter out already fetched commits
        filtered_commits = [c for c in all_reachable if c['sha'] not in skipAtCommits]
        
        # Apply pagination
        start_idx = offset
        end_idx = offset + limit
        return filtered_commits[start_idx:end_idx]


class TestProcessHeadCommits(unittest.TestCase):
    
    def test_single_commit_no_parents(self):
        """Test processing a single commit with no parents"""
        commit_graph = {
            'abc123': {
                'sha': 'abc123',
                'parents': []
            }
        }
        
        gitLocal = FakeGitLocal(commit_graph)
        fetchedCommits = {}
        commitQ = []
        
        result = process_head_commits(
            gitLocal, 'test/repo', 'refs/heads/main', 'abc123',
            fetchedCommits, commitQ, 10
        )
        
        # Should have processed one commit
        self.assertEqual(len(commitQ), 1)
        self.assertEqual(commitQ[0]['sha'], 'abc123')
        self.assertEqual(result, {'abc123': 1})
        
    def test_linear_history(self):
        """Test processing linear commit history"""
        commit_graph = {
            'commit3': {
                'sha': 'commit3',
                'parents': [{'sha': 'commit2'}]
            },
            'commit2': {
                'sha': 'commit2',
                'parents': [{'sha': 'commit1'}]
            },
            'commit1': {
                'sha': 'commit1',
                'parents': []
            }
        }
        
        gitLocal = FakeGitLocal(commit_graph)
        fetchedCommits = {}
        commitQ = []
        
        result = process_head_commits(
            gitLocal, 'test/repo', 'refs/heads/main', 'commit3',
            fetchedCommits, commitQ, 10
        )
        
        # Should have processed all three commits
        self.assertEqual(len(commitQ), 3)
        self.assertEqual(set(c['sha'] for c in commitQ), {'commit1', 'commit2', 'commit3'})
        self.assertEqual(len(result), 3)
        
    def test_skip_already_fetched(self):
        """Test that already fetched commits are skipped"""
        commit_graph = {
            'new2': {
                'sha': 'new2',
                'parents': [{'sha': 'new1'}]
            },
            'new1': {
                'sha': 'new1',
                'parents': [{'sha': 'old1'}]
            },
            'old1': {
                'sha': 'old1',
                'parents': []
            }
        }
        
        gitLocal = FakeGitLocal(commit_graph)
        fetchedCommits = {'old1': 1}  # Already fetched
        commitQ = []
        
        result = process_head_commits(
            gitLocal, 'test/repo', 'refs/heads/main', 'new2',
            fetchedCommits, commitQ, 10
        )
        
        # Should only process the new commits
        self.assertEqual(len(commitQ), 2)
        self.assertEqual(set(c['sha'] for c in commitQ), {'new1', 'new2'})
        self.assertNotIn('old1', result)
        
    def test_pagination(self):
        """Test that pagination works correctly"""
        # Create a longer chain of commits
        commit_graph = {}
        for i in range(25):
            sha = f'commit{i:02d}'
            parent_sha = f'commit{i-1:02d}' if i > 0 else None
            commit_graph[sha] = {
                'sha': sha,
                'parents': [{'sha': parent_sha}] if parent_sha else []
            }
        
        gitLocal = FakeGitLocal(commit_graph)
        fetchedCommits = {}
        commitQ = []
        
        # Use small page size to test pagination
        result = process_head_commits(
            gitLocal, 'test/repo', 'refs/heads/main', 'commit24',
            fetchedCommits, commitQ, 5  # Small page size
        )
        
        # Should have processed all 25 commits despite pagination
        self.assertEqual(len(commitQ), 25)
        self.assertEqual(len(result), 25)
        
        # Verify multiple pages were fetched
        self.assertGreater(len(gitLocal.calls), 1)
        
    def test_missing_parent_error(self):
        """Test that missing parents raise an exception"""
        commit_graph = {
            'orphan': {
                'sha': 'orphan',
                'parents': [{'sha': 'missing'}]  # Parent doesn't exist
            }
        }
        
        gitLocal = FakeGitLocal(commit_graph)
        fetchedCommits = {}
        commitQ = []
        
        with self.assertRaises(Exception) as ctx:
            process_head_commits(
                gitLocal, 'test/repo', 'refs/heads/main', 'orphan',
                fetchedCommits, commitQ, 10
            )
        
        self.assertIn('Some commit parents never found', str(ctx.exception))
        self.assertIn('missing', str(ctx.exception))
        
    def test_merge_commit(self):
        """Test processing merge commits with multiple parents"""
        commit_graph = {
            'merge': {
                'sha': 'merge',
                'parents': [{'sha': 'parent1'}, {'sha': 'parent2'}]
            },
            'parent1': {
                'sha': 'parent1',
                'parents': [{'sha': 'base'}]
            },
            'parent2': {
                'sha': 'parent2',
                'parents': [{'sha': 'base'}]
            },
            'base': {
                'sha': 'base',
                'parents': []
            }
        }
        
        gitLocal = FakeGitLocal(commit_graph)
        fetchedCommits = {}
        commitQ = []
        
        result = process_head_commits(
            gitLocal, 'test/repo', 'refs/heads/main', 'merge',
            fetchedCommits, commitQ, 10
        )
        
        # Should process all commits including both parents
        self.assertEqual(len(commitQ), 4)
        self.assertEqual(set(c['sha'] for c in commitQ), {'merge', 'parent1', 'parent2', 'base'})
        
    def test_dont_process_commit_twice(self):
        """Test that commits appearing in newlyFetchedCommits aren't processed again"""
        # Create a diamond pattern where 'base' could be reached twice
        commit_graph = {
            'merge': {
                'sha': 'merge',
                'parents': [{'sha': 'left'}, {'sha': 'right'}]
            },
            'left': {
                'sha': 'left',
                'parents': [{'sha': 'base'}]
            },
            'right': {
                'sha': 'right',
                'parents': [{'sha': 'base'}]
            },
            'base': {
                'sha': 'base',
                'parents': []
            }
        }
        
        gitLocal = FakeGitLocal(commit_graph)
        fetchedCommits = {}
        commitQ = []
        
        result = process_head_commits(
            gitLocal, 'test/repo', 'refs/heads/main', 'merge',
            fetchedCommits, commitQ, 10
        )
        
        # 'base' should only appear once in commitQ
        base_count = sum(1 for c in commitQ if c['sha'] == 'base')
        self.assertEqual(base_count, 1)
        

if __name__ == '__main__':
    unittest.main()