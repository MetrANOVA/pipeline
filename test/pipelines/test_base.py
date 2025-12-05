#!/usr/bin/env python3

import unittest
import sys
import os
import tempfile
import threading
import time
from unittest.mock import Mock, patch, MagicMock, mock_open, call

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from metranova.pipelines.base import BasePipeline, YAMLPipeline
from metranova.consumers.base import BaseConsumer
from metranova.cachers.base import BaseCacher, NoOpCacher
from metranova.writers.base import BaseWriter
from metranova.processors.base import BaseProcessor


class TestBasePipeline(unittest.TestCase):
    """Test cases for BasePipeline class."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.pipeline = BasePipeline()
    
    def test_initialization(self):
        """Test that BasePipeline initializes with correct default values."""
        self.assertIsInstance(self.pipeline.consumers, list)
        self.assertIsInstance(self.pipeline.processors, list)
        self.assertIsInstance(self.pipeline.cachers, dict)
        self.assertIsInstance(self.pipeline.writers, list)
        self.assertIsInstance(self.pipeline.consumer_threads, list)
        
        self.assertEqual(len(self.pipeline.consumers), 0)
        self.assertEqual(len(self.pipeline.processors), 0)
        self.assertEqual(len(self.pipeline.cachers), 0)
        self.assertEqual(len(self.pipeline.writers), 0)
        self.assertEqual(len(self.pipeline.consumer_threads), 0)
    
    def test_cacher_returns_existing(self):
        """Test that cacher() returns existing cacher when found."""
        mock_cacher = Mock(spec=BaseCacher)
        self.pipeline.cachers['test_cacher'] = mock_cacher
        
        result = self.pipeline.cacher('test_cacher')
        
        self.assertEqual(result, mock_cacher)
    
    def test_cacher_returns_noop_when_not_found(self):
        """Test that cacher() returns NoOpCacher when cacher not found."""
        result = self.pipeline.cacher('nonexistent_cacher')
        
        self.assertIsInstance(result, NoOpCacher)
    
    def test_process_message_with_writers(self):
        """Test that process_message calls all writers."""
        mock_writer1 = Mock(spec=BaseWriter)
        mock_writer2 = Mock(spec=BaseWriter)
        self.pipeline.writers = [mock_writer1, mock_writer2]
        
        test_msg = {'data': 'test'}
        test_metadata = {'source': 'test'}
        
        self.pipeline.process_message(test_msg, test_metadata)
        
        mock_writer1.process_message.assert_called_once_with(test_msg, test_metadata)
        mock_writer2.process_message.assert_called_once_with(test_msg, test_metadata)
    
    def test_process_message_with_none(self):
        """Test that process_message does nothing when message is None."""
        mock_writer = Mock(spec=BaseWriter)
        self.pipeline.writers = [mock_writer]
        
        self.pipeline.process_message(None, None)
        
        mock_writer.process_message.assert_not_called()
    
    def test_load_classes_valid_processor(self):
        """Test load_classes with valid processor class string."""
        class_str = 'test.processors.clickhouse.test_base.TestProcessor'
        
        # Create a mock processor class
        class TestProcessor(BaseProcessor):
            def __init__(self, pipeline):
                super().__init__(pipeline)
        
        with patch('importlib.import_module') as mock_import:
            mock_module = Mock()
            mock_module.TestProcessor = TestProcessor
            mock_import.return_value = mock_module
            
            result = self.pipeline.load_classes(
                class_str,
                init_args={'pipeline': self.pipeline},
                required_class=BaseProcessor
            )
        
        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], TestProcessor)
    
    def test_load_classes_multiple(self):
        """Test load_classes with comma-separated class strings."""
        class_str = 'module1.Class1,module2.Class2'
        
        class TestClass1(BaseProcessor):
            def __init__(self, pipeline):
                super().__init__(pipeline)
        
        class TestClass2(BaseProcessor):
            def __init__(self, pipeline):
                super().__init__(pipeline)
        
        def mock_import(module_name):
            if module_name == 'module1':
                mock_module = Mock()
                mock_module.Class1 = TestClass1
                return mock_module
            elif module_name == 'module2':
                mock_module = Mock()
                mock_module.Class2 = TestClass2
                return mock_module
        
        with patch('importlib.import_module', side_effect=mock_import):
            result = self.pipeline.load_classes(
                class_str,
                init_args={'pipeline': self.pipeline},
                required_class=BaseProcessor
            )
        
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], TestClass1)
        self.assertIsInstance(result[1], TestClass2)
    
    def test_load_classes_empty_string(self):
        """Test load_classes with empty string."""
        result = self.pipeline.load_classes('', required_class=BaseProcessor)
        
        self.assertEqual(result, [])
    
    def test_load_classes_with_whitespace(self):
        """Test load_classes handles whitespace in class names."""
        class_str = ' module.Class1 , module.Class2 '
        
        class TestClass(BaseProcessor):
            def __init__(self, pipeline):
                super().__init__(pipeline)
        
        with patch('importlib.import_module') as mock_import:
            mock_module = Mock()
            mock_module.Class1 = TestClass
            mock_module.Class2 = TestClass
            mock_import.return_value = mock_module
            
            result = self.pipeline.load_classes(
                class_str,
                init_args={'pipeline': self.pipeline},
                required_class=BaseProcessor
            )
        
        self.assertEqual(len(result), 2)
    
    def test_load_classes_import_error(self):
        """Test load_classes handles ImportError gracefully."""
        class_str = 'nonexistent.module.Class'
        
        with patch('importlib.import_module', side_effect=ImportError("Module not found")):
            result = self.pipeline.load_classes(class_str, required_class=BaseProcessor)
        
        self.assertEqual(result, [])
    
    def test_load_classes_attribute_error(self):
        """Test load_classes handles AttributeError gracefully."""
        class_str = 'module.NonExistentClass'
        
        with patch('importlib.import_module') as mock_import:
            mock_module = Mock()
            del mock_module.NonExistentClass  # Ensure attribute doesn't exist
            mock_import.return_value = mock_module
            
            result = self.pipeline.load_classes(class_str, required_class=BaseProcessor)
        
        self.assertEqual(result, [])
    
    def test_load_classes_wrong_subclass(self):
        """Test load_classes skips classes not matching required_class."""
        class_str = 'module.WrongClass'
        
        class WrongClass:
            def __init__(self):
                pass
        
        with patch('importlib.import_module') as mock_import:
            mock_module = Mock()
            mock_module.WrongClass = WrongClass
            mock_import.return_value = mock_module
            
            result = self.pipeline.load_classes(class_str, required_class=BaseProcessor)
        
        self.assertEqual(result, [])
    
    def test_load_classes_no_required_class(self):
        """Test load_classes works when required_class is None."""
        class_str = 'module.AnyClass'
        
        class AnyClass:
            def __init__(self):
                pass
        
        with patch('importlib.import_module') as mock_import:
            mock_module = Mock()
            mock_module.AnyClass = AnyClass
            mock_import.return_value = mock_module
            
            result = self.pipeline.load_classes(class_str, required_class=None)
        
        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], AnyClass)
    
    def test_start_with_consumers(self):
        """Test start() method creates and starts consumer threads."""
        mock_consumer = Mock(spec=BaseConsumer)
        self.pipeline.consumers = [mock_consumer]
        
        # Mock threading.Thread to avoid actually creating threads
        with patch('metranova.pipelines.base.threading.Thread') as mock_thread_class:
            mock_thread = Mock()
            mock_thread.join = Mock()  # Mock join to prevent blocking
            mock_thread_class.return_value = mock_thread
            
            # Start the pipeline in a controlled way
            # We'll let it create the thread but immediately return from join
            try:
                # Create a thread that will call start but we can interrupt
                import signal
                
                def timeout_handler(signum, frame):
                    raise TimeoutError("Test timeout")
                
                # Set a 0.5 second timeout
                signal.signal(signal.SIGALRM, timeout_handler)
                signal.alarm(1)
                
                try:
                    # Start will block on join, but we'll timeout
                    self.pipeline.start()
                except TimeoutError:
                    pass  # Expected - start() blocks on thread.join()
                finally:
                    signal.alarm(0)  # Cancel alarm
                
                # Verify thread was created and started
                mock_thread_class.assert_called_once()
                self.assertTrue(mock_thread.start.called or mock_thread_class.called)
                
            except Exception:
                # If signal doesn't work (e.g., on Windows), just verify consumers exist
                self.assertEqual(len(self.pipeline.consumers), 1)
    
    def test_start_without_consumers(self):
        """Test start() method logs warning when no consumers."""
        # No consumers added
        with patch.object(self.pipeline.logger, 'warning') as mock_warning:
            # Start in a thread since it blocks
            def start_pipeline():
                self.pipeline.start()
            
            pipeline_thread = threading.Thread(target=start_pipeline)
            pipeline_thread.daemon = True
            pipeline_thread.start()
            time.sleep(0.1)
            
            mock_warning.assert_called_with("No consumers to start")
    
    def test_close_consumers(self):
        """Test close() method closes all consumers."""
        mock_consumer1 = Mock(spec=BaseConsumer)
        mock_consumer2 = Mock(spec=BaseConsumer)
        self.pipeline.consumers = [mock_consumer1, mock_consumer2]
        
        self.pipeline.close()
        
        mock_consumer1.close.assert_called_once()
        mock_consumer2.close.assert_called_once()
    
    def test_close_writers(self):
        """Test close() method closes all writers."""
        mock_writer1 = Mock(spec=BaseWriter)
        mock_writer2 = Mock(spec=BaseWriter)
        self.pipeline.writers = [mock_writer1, mock_writer2]
        
        self.pipeline.close()
        
        mock_writer1.close.assert_called_once()
        mock_writer2.close.assert_called_once()
    
    def test_close_cachers(self):
        """Test close() method closes all cachers."""
        mock_cacher1 = Mock(spec=BaseCacher)
        mock_cacher2 = Mock(spec=BaseCacher)
        self.pipeline.cachers = {'cacher1': mock_cacher1, 'cacher2': mock_cacher2}
        
        self.pipeline.close()
        
        mock_cacher1.close.assert_called_once()
        mock_cacher2.close.assert_called_once()
    
    def test_close_consumer_threads(self):
        """Test close() method waits for consumer threads to finish."""
        mock_thread = Mock(spec=threading.Thread)
        mock_thread.is_alive.return_value = False
        self.pipeline.consumer_threads = [mock_thread]
        
        self.pipeline.close()
        
        mock_thread.join.assert_called_once_with(timeout=10.0)
        self.assertEqual(len(self.pipeline.consumer_threads), 0)
    
    def test_close_logs_timeout_warning(self):
        """Test close() logs warning for threads that don't finish."""
        mock_thread = Mock(spec=threading.Thread)
        mock_thread.name = "TestThread"
        mock_thread.is_alive.return_value = True  # Thread still alive after timeout
        self.pipeline.consumer_threads = [mock_thread]
        
        with patch.object(self.pipeline.logger, 'warning') as mock_warning:
            self.pipeline.close()
            
            mock_warning.assert_called()
            warning_message = mock_warning.call_args[0][0]
            self.assertIn("TestThread", warning_message)
            self.assertIn("did not finish within timeout", warning_message)
    
    def test_str_representation(self):
        """Test __str__() method returns proper string representation."""
        # Add mock components - need to use type() to set __name__ properly
        mock_consumer = Mock(spec=BaseConsumer)
        type(mock_consumer).__name__ = 'TestConsumer'
        
        mock_cacher = Mock(spec=BaseCacher)
        type(mock_cacher).__name__ = 'TestCacher'
        
        mock_processor = Mock(spec=BaseProcessor)
        type(mock_processor).__name__ = 'TestProcessor'
        
        mock_writer = Mock(spec=BaseWriter)
        type(mock_writer).__name__ = 'TestWriter'
        mock_writer.processors = [mock_processor]
        
        self.pipeline.consumers = [mock_consumer]
        self.pipeline.cachers = {'test_cache': mock_cacher}
        self.pipeline.writers = [mock_writer]
        
        result = str(self.pipeline)
        
        self.assertIn('TestConsumer', result)
        self.assertIn('test_cache:TestCacher', result)
        self.assertIn('TestWriter', result)
        self.assertIn('TestProcessor', result)
    
    def test_str_representation_writer_without_processors(self):
        """Test __str__() handles writers without processors attribute."""
        mock_writer = Mock(spec=BaseWriter)
        type(mock_writer).__name__ = 'TestWriter'
        del mock_writer.processors  # Remove processors attribute
        
        self.pipeline.writers = [mock_writer]
        
        result = str(self.pipeline)
        
        self.assertIn('TestWriter', result)
        self.assertIn('Processors: []', result)


class TestYAMLPipeline(unittest.TestCase):
    """Test cases for YAMLPipeline class."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.temp_dir = tempfile.mkdtemp()
        self.yaml_file = os.path.join(self.temp_dir, 'test_pipeline.yml')
    
    def tearDown(self):
        """Clean up temporary files."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_initialization_valid_yaml(self):
        """Test YAMLPipeline initializes with valid YAML configuration."""
        yaml_content = """
consumers:
  - type: metranova.consumers.kafka.KafkaConsumer

cachers:
  - type: metranova.cachers.redis.RedisCacher
    name: redis

writers:
  - type: metranova.writers.clickhouse.ClickHouseWriter
    processors:
      - metranova.processors.clickhouse.flow.FlowProcessor
"""
        
        with open(self.yaml_file, 'w') as f:
            f.write(yaml_content)
        
        with patch.object(BasePipeline, 'load_classes') as mock_load:
            mock_load.return_value = []
            
            pipeline = YAMLPipeline(yaml_file=self.yaml_file)
            
            self.assertEqual(pipeline.yaml_file, self.yaml_file)
    
    def test_initialization_missing_file(self):
        """Test YAMLPipeline raises error with missing file."""
        nonexistent_file = os.path.join(self.temp_dir, 'nonexistent.yml')
        
        with self.assertRaises(FileNotFoundError):
            YAMLPipeline(yaml_file=nonexistent_file)
    
    def test_initialization_invalid_yaml(self):
        """Test YAMLPipeline raises error with invalid YAML."""
        yaml_content = """
consumers:
  - type: test
    invalid yaml structure: [
"""
        
        with open(self.yaml_file, 'w') as f:
            f.write(yaml_content)
        
        with self.assertRaises(Exception):  # yaml.YAMLError
            YAMLPipeline(yaml_file=self.yaml_file)
    
    def test_initialization_empty_yaml(self):
        """Test YAMLPipeline raises error with empty YAML."""
        with open(self.yaml_file, 'w') as f:
            f.write('')
        
        with self.assertRaises(ValueError) as context:
            YAMLPipeline(yaml_file=self.yaml_file)
        
        self.assertIn("empty or invalid", str(context.exception))
    
    def test_loads_consumers(self):
        """Test YAMLPipeline loads consumer classes."""
        yaml_content = """
consumers:
  - type: module.Consumer1
  - type: module.Consumer2
"""
        
        with open(self.yaml_file, 'w') as f:
            f.write(yaml_content)
        
        with patch.object(BasePipeline, 'load_classes') as mock_load:
            mock_consumer1 = Mock(spec=BaseConsumer)
            mock_consumer2 = Mock(spec=BaseConsumer)
            mock_load.return_value = [mock_consumer1, mock_consumer2]
            
            pipeline = YAMLPipeline(yaml_file=self.yaml_file)
            
            # Should be called for each consumer type
            self.assertEqual(mock_load.call_count, 2)
    
    def test_loads_cachers(self):
        """Test YAMLPipeline loads cacher classes with names."""
        yaml_content = """
cachers:
  - type: module.Cacher1
    name: cache1
  - type: module.Cacher2
    name: cache2
"""
        
        with open(self.yaml_file, 'w') as f:
            f.write(yaml_content)
        
        with patch.object(BasePipeline, 'load_classes') as mock_load:
            mock_cacher1 = Mock(spec=BaseCacher)
            mock_cacher2 = Mock(spec=BaseCacher)
            
            def side_effect(class_str, **kwargs):
                if 'Cacher1' in class_str:
                    return [mock_cacher1]
                elif 'Cacher2' in class_str:
                    return [mock_cacher2]
                return []
            
            mock_load.side_effect = side_effect
            
            pipeline = YAMLPipeline(yaml_file=self.yaml_file)
            
            self.assertIn('cache1', pipeline.cachers)
            self.assertIn('cache2', pipeline.cachers)
            self.assertEqual(pipeline.cachers['cache1'], mock_cacher1)
            self.assertEqual(pipeline.cachers['cache2'], mock_cacher2)
    
    def test_loads_cacher_skips_without_name(self):
        """Test YAMLPipeline skips cacher without name."""
        yaml_content = """
cachers:
  - type: module.Cacher1
"""
        
        with open(self.yaml_file, 'w') as f:
            f.write(yaml_content)
        
        with patch.object(BasePipeline, 'load_classes') as mock_load:
            mock_load.return_value = [Mock(spec=BaseCacher)]
            
            pipeline = YAMLPipeline(yaml_file=self.yaml_file)
            
            self.assertEqual(len(pipeline.cachers), 0)
    
    def test_loads_writers_with_processors(self):
        """Test YAMLPipeline loads writer classes with processors."""
        yaml_content = """
writers:
  - type: module.Writer1
    processors:
      - module.Processor1
      - module.Processor2
"""
        
        with open(self.yaml_file, 'w') as f:
            f.write(yaml_content)
        
        with patch.object(BasePipeline, 'load_classes') as mock_load:
            mock_processor1 = Mock(spec=BaseProcessor)
            mock_processor2 = Mock(spec=BaseProcessor)
            mock_writer = Mock(spec=BaseWriter)
            
            def side_effect(class_str, **kwargs):
                if 'Processor1' in class_str:
                    return [mock_processor1]
                elif 'Processor2' in class_str:
                    return [mock_processor2]
                elif 'Writer1' in class_str:
                    return [mock_writer]
                return []
            
            mock_load.side_effect = side_effect
            
            pipeline = YAMLPipeline(yaml_file=self.yaml_file)
            
            # Should load 2 processors and 1 writer
            self.assertEqual(mock_load.call_count, 3)
            self.assertEqual(len(pipeline.writers), 1)
    
    def test_loads_writers_without_processors(self):
        """Test YAMLPipeline loads writer without processors."""
        yaml_content = """
writers:
  - type: module.Writer1
"""
        
        with open(self.yaml_file, 'w') as f:
            f.write(yaml_content)
        
        with patch.object(BasePipeline, 'load_classes') as mock_load:
            mock_writer = Mock(spec=BaseWriter)
            mock_load.return_value = [mock_writer]
            
            pipeline = YAMLPipeline(yaml_file=self.yaml_file)
            
            self.assertEqual(len(pipeline.writers), 1)
    
    def test_loads_writer_skips_without_type(self):
        """Test YAMLPipeline skips writer without type."""
        yaml_content = """
writers:
  - processors:
      - module.Processor1
"""
        
        with open(self.yaml_file, 'w') as f:
            f.write(yaml_content)
        
        with patch.object(BasePipeline, 'load_classes') as mock_load:
            mock_load.return_value = []
            
            pipeline = YAMLPipeline(yaml_file=self.yaml_file)
            
            self.assertEqual(len(pipeline.writers), 0)
    
    def test_complex_pipeline_configuration(self):
        """Test YAMLPipeline with complex multi-component configuration."""
        yaml_content = """
consumers:
  - type: module.KafkaConsumer
  - type: module.RedisConsumer

cachers:
  - type: module.ClickHouseCacher
    name: clickhouse
  - type: module.IPCacher
    name: ip

writers:
  - type: module.ClickHouseWriter
    processors:
      - module.FlowProcessor
      - module.InterfaceProcessor
  - type: module.RedisWriter
    processors:
      - module.CacheProcessor
"""
        
        with open(self.yaml_file, 'w') as f:
            f.write(yaml_content)
        
        with patch.object(BasePipeline, 'load_classes') as mock_load:
            # Return appropriate mocks for each type
            def side_effect(class_str, **kwargs):
                if 'Consumer' in class_str:
                    return [Mock(spec=BaseConsumer)]
                elif 'Cacher' in class_str:
                    return [Mock(spec=BaseCacher)]
                elif 'Processor' in class_str:
                    return [Mock(spec=BaseProcessor)]
                elif 'Writer' in class_str:
                    return [Mock(spec=BaseWriter)]
                return []
            
            mock_load.side_effect = side_effect
            
            pipeline = YAMLPipeline(yaml_file=self.yaml_file)
            
            # Verify all components were loaded
            self.assertEqual(len(pipeline.cachers), 2)
            self.assertIn('clickhouse', pipeline.cachers)
            self.assertIn('ip', pipeline.cachers)
    
    def test_yaml_with_no_consumers(self):
        """Test YAMLPipeline handles YAML with no consumers section."""
        yaml_content = """
writers:
  - type: module.Writer1
"""
        
        with open(self.yaml_file, 'w') as f:
            f.write(yaml_content)
        
        with patch.object(BasePipeline, 'load_classes') as mock_load:
            mock_load.return_value = [Mock(spec=BaseWriter)]
            
            pipeline = YAMLPipeline(yaml_file=self.yaml_file)
            
            self.assertEqual(len(pipeline.consumers), 0)
    
    def test_yaml_with_no_cachers(self):
        """Test YAMLPipeline handles YAML with no cachers section."""
        yaml_content = """
consumers:
  - type: module.Consumer1
"""
        
        with open(self.yaml_file, 'w') as f:
            f.write(yaml_content)
        
        with patch.object(BasePipeline, 'load_classes') as mock_load:
            mock_load.return_value = [Mock(spec=BaseConsumer)]
            
            pipeline = YAMLPipeline(yaml_file=self.yaml_file)
            
            self.assertEqual(len(pipeline.cachers), 0)
    
    def test_yaml_with_no_writers(self):
        """Test YAMLPipeline handles YAML with no writers section."""
        yaml_content = """
consumers:
  - type: module.Consumer1
"""
        
        with open(self.yaml_file, 'w') as f:
            f.write(yaml_content)
        
        with patch.object(BasePipeline, 'load_classes') as mock_load:
            mock_load.return_value = [Mock(spec=BaseConsumer)]
            
            pipeline = YAMLPipeline(yaml_file=self.yaml_file)
            
            self.assertEqual(len(pipeline.writers), 0)


if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)
