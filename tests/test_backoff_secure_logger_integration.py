import sys
import os
import unittest
from unittest.mock import patch, Mock, MagicMock, call
import logging
import io
import requests
import backoff

# Import from the git-installed package
from minware_singer_utils.logging import SecureLogger

# Import the LOGGER from tap_gitlab
sys.path.insert(0, '.')
from tap_gitlab import LOGGER


class TestBackoffSecureLoggerIntegration(unittest.TestCase):
    """Test that validates backoff library specifically uses SecureLogger.log() and masking works"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Capture the original logger configuration
        self.original_handlers = LOGGER.logger.handlers.copy()
        self.original_level = LOGGER.logger.level
        
        # Create a string buffer to capture log output
        self.log_buffer = io.StringIO()
        
        # Clear existing handlers and add our test handler
        LOGGER.logger.handlers.clear()
        handler = logging.StreamHandler(self.log_buffer)
        handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
        LOGGER.logger.addHandler(handler)
        LOGGER.logger.setLevel(logging.INFO)
        
    def tearDown(self):
        """Clean up test fixtures"""
        # Restore original logger configuration
        LOGGER.logger.handlers.clear()
        LOGGER.logger.handlers.extend(self.original_handlers)
        LOGGER.logger.setLevel(self.original_level)
    
    def test_backoff_calls_secure_logger_log_method_with_masking(self):
        """Test that backoff specifically calls SecureLogger.log() and sensitive data gets masked"""
        
        # Add sensitive token to SecureLogger
        LOGGER.addToken('sensitive-api-key-xyz789')
        
        # Create a spy on the log method to verify backoff calls it
        original_log = LOGGER.log
        log_calls = []
        
        def spy_log(level, message, *args):
            log_calls.append((level, message, args))
            return original_log(level, message, *args)
        
        LOGGER.log = spy_log
        
        try:
            # Create a function that will fail and trigger backoff retries
            call_count = 0
            
            @backoff.on_exception(
                backoff.expo,
                requests.exceptions.RequestException,
                max_tries=3,
                factor=0.1,  # Fast retries for testing
                logger=LOGGER  # This is the critical part - backoff uses our SecureLogger
            )
            def failing_function():
                nonlocal call_count
                call_count += 1
                
                # Raise exception with sensitive data that should be masked
                raise requests.exceptions.RequestException(
                    f"API request failed with key sensitive-api-key-xyz789 on attempt {call_count}"
                )
            
            # Execute and expect failure after retries
            with self.assertRaises(requests.exceptions.RequestException):
                failing_function()
            
            # Verify backoff made multiple attempts
            self.assertEqual(call_count, 3, "Should have made 3 attempts")
            
            # Verify backoff called the log method
            self.assertGreater(len(log_calls), 0, "Backoff should have called LOGGER.log()")
            
            # Check that at least one log call was from backoff (contains "Backing off")
            backoff_log_calls = [call for call in log_calls if 'Backing off' in str(call[1])]
            self.assertGreater(len(backoff_log_calls), 0, "Should have backoff retry log messages")
            
            # Get the actual logged output to verify masking
            log_output = self.log_buffer.getvalue()
            
            # Verify backoff generated retry logs
            self.assertIn('Backing off', log_output, "Should contain backoff retry messages")
            
            # CRITICAL TEST: Verify sensitive data was masked in backoff logs
            self.assertNotIn('sensitive-api-key-xyz789', log_output, 
                           "Sensitive API key should NOT appear in any logs")
            
            # Verify masking occurred (should contain <TOKEN>)
            self.assertIn('<TOKEN>', log_output, 
                         "Logs should contain masked <TOKEN> placeholder")
            
            print(f"\\nDEBUG - Log calls made by backoff: {len(log_calls)}")
            print(f"DEBUG - Log output:\\n{log_output}")
            
        finally:
            # Restore original log method
            LOGGER.log = original_log
    
    def test_backoff_decorators_match_tap_gitlab_configuration(self):
        """Verify the test uses the same backoff configuration as tap_gitlab.request()"""
        import inspect
        from tap_gitlab import request
        
        # Get the source code of the actual request function
        source = inspect.getsource(request)
        
        # Verify the actual function has the expected backoff decorators
        self.assertIn('@backoff.on_predicate', source, 
                     "request() should have @backoff.on_predicate decorator")
        self.assertIn('@backoff.on_exception', source, 
                     "request() should have @backoff.on_exception decorator")
        self.assertIn('logger=LOGGER', source, 
                     "backoff decorators should use logger=LOGGER parameter")
        
        # Verify it's using the same exception types
        self.assertIn('requests.exceptions.RequestException', source,
                     "Should handle RequestException like our test")


if __name__ == '__main__':
    unittest.main() 