import unittest
from unittest.mock import patch, MagicMock
import requests
import json
import sys

# Create mock singer modules
mock_singer = MagicMock()
mock_singer.Transformer = MagicMock
mock_singer.utils = MagicMock()
mock_singer.metadata = MagicMock()
mock_singer.metrics = MagicMock()
# Create a logger instance for get_logger to return
mock_logger = MagicMock()
mock_singer.get_logger = MagicMock(return_value=mock_logger)

# Create mock stream class
class MockStream:
    def is_selected(self):
        return False  # Return False to skip stream processing

# Create Catalog mock with get_selected_streams method
class MockCatalog:
    def get_selected_streams(self, state):
        return []  # Return empty list since we don't need any streams for this test
    def get_stream(self, name):
        return MockStream()  # Return a mock stream object instead of empty list
mock_catalog = MagicMock()
mock_catalog.Catalog = MockCatalog
mock_catalog.CatalogEntry = MagicMock

# Create Schema mock with from_dict method
class MockSchema:
    @staticmethod
    def from_dict(data):
        return data
mock_schema = MagicMock()
mock_schema.Schema = MockSchema

# Create mock minware_singer_utils module with required classes
mock_minware = MagicMock()
mock_minware.GitLocal = MagicMock
# Create SecureLogger class that can be instantiated
class MockSecureLogger:
    def __init__(self, logger):
        self.logger = logger
        # Pass through all common logging methods to the mock logger
        self.critical = logger.critical
        self.error = logger.error
        self.warning = logger.warning
        self.info = logger.info
        self.debug = logger.debug
    def addToken(self, token):
        pass  # We don't need to do anything with the token in our test
mock_minware.SecureLogger = MockSecureLogger

# Mock all modules before any imports
sys.modules['minware_singer_utils'] = mock_minware
sys.modules['singer'] = mock_singer
sys.modules['singer.catalog'] = mock_catalog
sys.modules['singer.schema'] = mock_schema
sys.modules['singer.metrics'] = MagicMock()
sys.modules['singer.metadata'] = MagicMock()
sys.modules['singer.utils'] = MagicMock()
sys.modules['pytz'] = MagicMock()
sys.modules['backoff'] = MagicMock()
sys.modules['strict_rfc3339'] = MagicMock()
sys.modules['dateutil.parser'] = MagicMock()
sys.modules['dateutil.tz'] = MagicMock()
sys.modules['psutil'] = MagicMock()

# Now import tap_gitlab
from tap_gitlab import main_impl

class TestErrorLogging(unittest.TestCase):
    def test_logs_error_details_on_exception(self):
        # Setup mock response
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = '{"error": "Internal Server Error"}'
        
        # Configure request to raise an exception
        with patch('tap_gitlab.request') as mock_request, \
             patch('tap_gitlab.latest_response', mock_response), \
             patch('tap_gitlab.latest_request', {'url': 'https://gitlab.com/api/v4/test'}), \
             patch('tap_gitlab.do_sync') as mock_do_sync:
            
            # Make do_sync raise the exception instead of request
            mock_do_sync.side_effect = requests.exceptions.RequestException("Test error")

            # Configure minimal arguments to trigger sync
            test_args = MagicMock()
            test_args.config = {
                'private_token': 'test-token',
                'projects': 'test-project',
                'api_url': 'https://gitlab.com',
                'start_date': '2021-01-01',
                'groups': '',
                'ultimate_license': False,
                'fetch_merge_request_commits': False,
                'fetch_merge_request_notes': False,
                'fetch_pipelines_extended': False,
                'fetch_group_variables': False,
                'fetch_project_variables': False
            }
            test_args.state = None
            test_args.discover = False
            test_args.catalog = MockCatalog()  # Use our MockCatalog instance

            with patch('tap_gitlab.utils.parse_args', return_value=test_args):
                # Run main which should catch and handle the error
                from tap_gitlab import main
                with self.assertRaises(SystemExit):
                    main()

            # Verify error logging calls on the mock_logger we set up at the start
            mock_logger.critical.assert_any_call('Latest Request URL: https://gitlab.com/api/v4/test')
            mock_logger.critical.assert_any_call('Response Code: 500')
            mock_logger.critical.assert_any_call('Response Data:')
            mock_logger.critical.assert_any_call('{"error": "Internal Server Error"}')

if __name__ == '__main__':
    unittest.main() 