import unittest
import time
from unittest.mock import Mock, patch, MagicMock, call
from metranova.consumers.base import BaseConsumer, TimedIntervalConsumer


class TestBaseConsumer(unittest.TestCase):
    """Test the BaseConsumer class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_pipeline = Mock()
        self.consumer = BaseConsumer(self.mock_pipeline)
    
    def test_initialization(self):
        """Test BaseConsumer initialization."""
        self.assertEqual(self.consumer.pipeline, self.mock_pipeline)
        self.assertIsNotNone(self.consumer.logger)
    
    def test_pre_consume_messages_default(self):
        """Test pre_consume_messages default implementation."""
        result = self.consumer.pre_consume_messages()
        self.assertIsNone(result)
    
    def test_post_consume_messages_default(self):
        """Test post_consume_messages default implementation."""
        result = self.consumer.post_consume_messages()
        self.assertTrue(result)
    
    def test_consume_messages_not_implemented(self):
        """Test that consume_messages raises NotImplementedError."""
        with self.assertRaises(NotImplementedError) as context:
            self.consumer.consume_messages()
        self.assertIn("Subclasses must implement consume_messages method", str(context.exception))
    
    def test_close_default(self):
        """Test close default implementation."""
        result = self.consumer.close()
        self.assertIsNone(result)
    
    def test_consume_no_datasource(self):
        """Test consume when datasource is None."""
        self.consumer.datasource = None
        self.consumer.consume_messages = Mock()
        
        # Should still run since datasource check only happens if datasource exists
        # Let's make it exit immediately
        self.consumer.post_consume_messages = Mock(return_value=False)
        
        self.consumer.consume()
        
        self.consumer.consume_messages.assert_called_once()
    
    def test_consume_datasource_no_client(self):
        """Test consume when datasource client is not initialized."""
        self.consumer.datasource = Mock()
        self.consumer.datasource.client = None
        self.consumer.consume_messages = Mock()
        
        self.consumer.consume()
        
        # Should log error and return without calling consume_messages
        self.consumer.consume_messages.assert_not_called()
    
    def test_consume_successful_loop(self):
        """Test consume runs successfully and exits after post_consume returns False."""
        self.consumer.datasource = Mock()
        self.consumer.datasource.client = Mock()
        self.consumer.consume_messages = Mock()
        self.consumer.pre_consume_messages = Mock()
        self.consumer.post_consume_messages = Mock(side_effect=[True, True, False])
        self.consumer.close = Mock()
        
        self.consumer.consume()
        
        # Should call consume_messages 3 times
        self.assertEqual(self.consumer.consume_messages.call_count, 3)
        self.assertEqual(self.consumer.pre_consume_messages.call_count, 3)
        self.assertEqual(self.consumer.post_consume_messages.call_count, 3)
        self.consumer.close.assert_called_once()
    
    def test_consume_keyboard_interrupt(self):
        """Test consume handles KeyboardInterrupt gracefully."""
        self.consumer.datasource = Mock()
        self.consumer.datasource.client = Mock()
        self.consumer.consume_messages = Mock(side_effect=KeyboardInterrupt())
        self.consumer.close = Mock()
        
        self.consumer.consume()
        
        self.consumer.consume_messages.assert_called_once()
        self.consumer.close.assert_called_once()
    
    def test_consume_calls_pre_and_post_hooks(self):
        """Test that consume calls pre and post hooks."""
        self.consumer.datasource = Mock()
        self.consumer.datasource.client = Mock()
        self.consumer.consume_messages = Mock()
        self.consumer.pre_consume_messages = Mock()
        self.consumer.post_consume_messages = Mock(return_value=False)
        self.consumer.close = Mock()
        
        self.consumer.consume()
        
        self.consumer.pre_consume_messages.assert_called_once()
        self.consumer.post_consume_messages.assert_called_once()
    
    def test_consume_infinite_loop_with_true_post_consume(self):
        """Test consume continues looping while post_consume returns True."""
        self.consumer.datasource = Mock()
        self.consumer.datasource.client = Mock()
        call_count = [0]
        
        def consume_messages_side_effect():
            call_count[0] += 1
            if call_count[0] >= 5:
                raise KeyboardInterrupt()
        
        self.consumer.consume_messages = Mock(side_effect=consume_messages_side_effect)
        self.consumer.post_consume_messages = Mock(return_value=True)
        self.consumer.close = Mock()
        
        self.consumer.consume()
        
        self.assertEqual(self.consumer.consume_messages.call_count, 5)


class TestTimedIntervalConsumer(unittest.TestCase):
    """Test the TimedIntervalConsumer class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_pipeline = Mock()
        self.mock_pipeline.cachers = {}  # Default to empty dict
        self.consumer = TimedIntervalConsumer(self.mock_pipeline)
    
    def test_initialization(self):
        """Test TimedIntervalConsumer initialization."""
        self.assertEqual(self.consumer.pipeline, self.mock_pipeline)
        self.assertIsNotNone(self.consumer.logger)
        self.assertEqual(self.consumer.update_interval, -1)
        self.assertTrue(self.consumer.auto_prime_cachers)
    
    def test_initialization_inherits_from_base_consumer(self):
        """Test that TimedIntervalConsumer inherits from BaseConsumer."""
        self.assertIsInstance(self.consumer, BaseConsumer)
    
    def test_pre_consume_messages_no_cachers(self):
        """Test pre_consume_messages when pipeline has no cachers."""
        self.mock_pipeline.cachers = None
        
        self.consumer.pre_consume_messages()
        
        # Should not raise exception
    
    def test_pre_consume_messages_empty_cachers(self):
        """Test pre_consume_messages with empty cachers dict."""
        self.mock_pipeline.cachers = {}
        
        self.consumer.pre_consume_messages()
        
        # Should not raise exception
    
    def test_pre_consume_messages_primes_cachers(self):
        """Test pre_consume_messages primes all cachers."""
        mock_cacher1 = Mock()
        mock_cacher2 = Mock()
        self.mock_pipeline.cachers = {
            'cacher1': mock_cacher1,
            'cacher2': mock_cacher2
        }
        
        self.consumer.pre_consume_messages()
        
        mock_cacher1.prime.assert_called_once()
        mock_cacher2.prime.assert_called_once()
    
    def test_pre_consume_messages_auto_prime_disabled(self):
        """Test pre_consume_messages when auto_prime_cachers is False."""
        mock_cacher = Mock()
        self.mock_pipeline.cachers = {'cacher': mock_cacher}
        self.consumer.auto_prime_cachers = False
        
        self.consumer.pre_consume_messages()
        
        mock_cacher.prime.assert_not_called()
    
    def test_post_consume_messages_no_interval(self):
        """Test post_consume_messages with no update interval (run once)."""
        self.consumer.update_interval = -1
        
        result = self.consumer.post_consume_messages()
        
        self.assertFalse(result)
    
    def test_post_consume_messages_zero_interval(self):
        """Test post_consume_messages with zero interval."""
        self.consumer.update_interval = 0
        
        result = self.consumer.post_consume_messages()
        
        self.assertFalse(result)
    
    @patch('metranova.consumers.base.time.sleep')
    def test_post_consume_messages_with_interval(self, mock_sleep):
        """Test post_consume_messages sleeps and returns True with positive interval."""
        self.consumer.update_interval = 10
        
        result = self.consumer.post_consume_messages()
        
        mock_sleep.assert_called_once_with(10)
        self.assertTrue(result)
    
    @patch('metranova.consumers.base.time.sleep')
    def test_post_consume_messages_different_intervals(self, mock_sleep):
        """Test post_consume_messages with different interval values."""
        test_intervals = [1, 5, 30, 60, 300]
        
        for interval in test_intervals:
            mock_sleep.reset_mock()
            self.consumer.update_interval = interval
            
            result = self.consumer.post_consume_messages()
            
            mock_sleep.assert_called_once_with(interval)
            self.assertTrue(result)
    
    def test_consume_runs_once_with_negative_interval(self):
        """Test that consume runs only once with negative interval."""
        self.consumer.datasource = Mock()
        self.consumer.datasource.client = Mock()
        self.consumer.consume_messages = Mock()
        self.consumer.update_interval = -1
        self.consumer.close = Mock()
        
        self.consumer.consume()
        
        # Should call consume_messages only once
        self.consumer.consume_messages.assert_called_once()
        self.consumer.close.assert_called_once()
    
    @patch('metranova.consumers.base.time.sleep')
    def test_consume_loops_with_positive_interval(self, mock_sleep):
        """Test consume continues looping with positive interval until interrupted."""
        self.consumer.datasource = Mock()
        self.consumer.datasource.client = Mock()
        self.consumer.update_interval = 5
        self.consumer.close = Mock()
        
        call_count = [0]
        def consume_messages_side_effect():
            call_count[0] += 1
            if call_count[0] >= 3:
                raise KeyboardInterrupt()
        
        self.consumer.consume_messages = Mock(side_effect=consume_messages_side_effect)
        
        self.consumer.consume()
        
        # Should call consume_messages 3 times
        self.assertEqual(self.consumer.consume_messages.call_count, 3)
        # Should sleep 2 times (after first and second call, not after third due to interrupt)
        self.assertEqual(mock_sleep.call_count, 2)
        mock_sleep.assert_called_with(5)
    
    @patch('metranova.consumers.base.time.sleep')
    def test_consume_with_cachers(self, mock_sleep):
        """Test consume primes cachers before each consume_messages call."""
        mock_cacher = Mock()
        self.mock_pipeline.cachers = {'test_cacher': mock_cacher}
        
        self.consumer.datasource = Mock()
        self.consumer.datasource.client = Mock()
        self.consumer.update_interval = 1
        self.consumer.close = Mock()
        
        call_count = [0]
        def consume_messages_side_effect():
            call_count[0] += 1
            if call_count[0] >= 2:
                raise KeyboardInterrupt()
        
        self.consumer.consume_messages = Mock(side_effect=consume_messages_side_effect)
        
        self.consumer.consume()
        
        # Should prime cacher 2 times (before each consume_messages)
        self.assertEqual(mock_cacher.prime.call_count, 2)
    
    def test_custom_update_interval(self):
        """Test setting custom update interval."""
        self.consumer.update_interval = 120
        self.assertEqual(self.consumer.update_interval, 120)
    
    def test_custom_auto_prime_setting(self):
        """Test setting auto_prime_cachers to False."""
        self.consumer.auto_prime_cachers = False
        self.assertFalse(self.consumer.auto_prime_cachers)


if __name__ == '__main__':
    unittest.main()
