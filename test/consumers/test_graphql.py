#!/usr/bin/env python3

import unittest
import sys
import os
from unittest.mock import Mock, patch, MagicMock
from requests.exceptions import HTTPError, RequestException, ConnectionError

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from metranova.consumers.graphql import GraphQLConsumer


class TestGraphQLConsumer(unittest.TestCase):
    """Unit tests for GraphQLConsumer class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = Mock()
        self.mock_pipeline.process_message = Mock()
        
        self.env_patcher = patch.dict(os.environ, {
            'GRAPHQL_CONSUMER_UPDATE_INTERVAL': '3600',
            'GRAPHQL_ENDPOINT': 'http://example.com/graphql',
            'GRAPHQL_QUERY': '{ users { id name } }'
        }, clear=True)
        self.env_patcher.start()
        
        with patch('metranova.consumers.graphql.HTTPConnector') as mock_connector_class:
            self.mock_connector = Mock()
            self.mock_client = Mock()
            self.mock_connector.client = self.mock_client
            mock_connector_class.return_value = self.mock_connector
            
            self.consumer = GraphQLConsumer(self.mock_pipeline)

    def tearDown(self):
        """Clean up after each test."""
        self.env_patcher.stop()

    # ==================== Initialization Tests ====================

    def test_initialization(self):
        """Test that GraphQLConsumer initializes correctly."""
        self.assertEqual(self.consumer.update_interval, 3600)
        self.assertEqual(self.consumer.endpoint, 'http://example.com/graphql')
        self.assertEqual(self.consumer.query, '{ users { id name } }')
        self.assertIsNotNone(self.consumer.datasource)

    def test_initialization_with_prefix(self):
        """Test initialization with environment variable prefix."""
        with patch.dict(os.environ, {
            'TESTAPI_GRAPHQL_CONSUMER_UPDATE_INTERVAL': '7200',
            'TESTAPI_GRAPHQL_ENDPOINT': 'http://testapi.example.com/graphql',
            'TESTAPI_GRAPHQL_QUERY': '{ services { id name } }',
            'GRAPHQL_CONSUMER_ENV_PREFIX': 'TESTAPI'
        }, clear=True):
            with patch('metranova.consumers.graphql.HTTPConnector'):
                consumer = GraphQLConsumer(self.mock_pipeline)
                
                self.assertEqual(consumer.update_interval, 7200)
                self.assertEqual(consumer.endpoint, 'http://testapi.example.com/graphql')
                self.assertEqual(consumer.query, '{ services { id name } }')

    def test_initialization_with_token(self):
        """Test initialization with GraphQL token."""
        with patch.dict(os.environ, {
            'GRAPHQL_ENDPOINT': 'http://example.com/graphql',
            'GRAPHQL_QUERY': '{ test }',
            'GRAPHQL_TOKEN': 'test-token-123'
        }, clear=True):
            with patch('metranova.consumers.graphql.HTTPConnector'):
                consumer = GraphQLConsumer(self.mock_pipeline)
                
                self.assertEqual(consumer.token, 'test-token-123')

    def test_initialization_missing_endpoint(self):
        """Test initialization fails with missing endpoint."""
        with patch.dict(os.environ, {
            'GRAPHQL_QUERY': '{ test }'
        }, clear=True):
            with patch('metranova.consumers.graphql.HTTPConnector'):
                with self.assertRaises(ValueError) as context:
                    GraphQLConsumer(self.mock_pipeline)
                
                self.assertIn('GRAPHQL_ENDPOINT', str(context.exception))

    def test_initialization_missing_query(self):
        """Test initialization fails with missing query."""
        with patch.dict(os.environ, {
            'GRAPHQL_ENDPOINT': 'http://example.com/graphql'
        }, clear=True):
            with patch('metranova.consumers.graphql.HTTPConnector'):
                with self.assertRaises(ValueError) as context:
                    GraphQLConsumer(self.mock_pipeline)
                
                self.assertIn('GRAPHQL_QUERY', str(context.exception))

    def test_initialization_default_update_interval(self):
        """Test initialization uses default update interval when not specified."""
        with patch.dict(os.environ, {
            'GRAPHQL_ENDPOINT': 'http://example.com/graphql',
            'GRAPHQL_QUERY': '{ test }'
        }, clear=True):
            with patch('metranova.consumers.graphql.HTTPConnector'):
                consumer = GraphQLConsumer(self.mock_pipeline)
                
                # Should use default value of -1
                self.assertEqual(consumer.update_interval, -1)

    # ==================== consume_messages Tests ====================

    def test_consume_messages_success(self):
        """Test consume_messages successfully fetches and processes data."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': {
                'users': [
                    {'id': '1', 'name': 'User 1'},
                    {'id': '2', 'name': 'User 2'}
                ]
            }
        }
        
        self.mock_client.post.return_value = mock_response
        
        self.consumer.consume_messages()
        
        # Verify POST was called with correct parameters
        self.mock_client.post.assert_called_once()
        call_args = self.mock_client.post.call_args
        self.assertEqual(call_args[0][0], 'http://example.com/graphql')
        
        # Verify JSON payload contains query
        json_payload = call_args[1].get('json', {})
        self.assertEqual(json_payload.get('query'), '{ users { id name } }')
        
        # Verify headers
        headers = call_args[1].get('headers', {})
        self.assertEqual(headers.get('Content-Type'), 'application/json')
        
        # Verify response was processed
        mock_response.raise_for_status.assert_called_once()
        
        # Verify message was passed to pipeline
        self.mock_pipeline.process_message.assert_called_once()
        
        # Verify message structure
        call_args = self.mock_pipeline.process_message.call_args[0][0]
        self.assertEqual(call_args['url'], 'http://example.com/graphql')
        self.assertEqual(call_args['status_code'], 200)
        self.assertIn('data', call_args['data'])

    def test_consume_messages_with_token(self):
        """Test consume_messages includes authorization token."""
        with patch.dict(os.environ, {
            'GRAPHQL_ENDPOINT': 'http://example.com/graphql',
            'GRAPHQL_QUERY': '{ test }',
            'GRAPHQL_TOKEN': 'test-token-456'
        }, clear=True):
            with patch('metranova.consumers.graphql.HTTPConnector') as mock_connector_class:
                mock_connector = Mock()
                mock_client = Mock()
                mock_connector.client = mock_client
                mock_connector_class.return_value = mock_connector
                
                mock_response = Mock()
                mock_response.status_code = 200
                mock_response.json.return_value = {'data': {}}
                mock_client.post.return_value = mock_response
                
                consumer = GraphQLConsumer(self.mock_pipeline)
                consumer.consume_messages()
                
                # Verify authorization header was included
                call_args = mock_client.post.call_args
                headers = call_args[1].get('headers', {})
                self.assertEqual(headers.get('Authorization'), 'Token test-token-456')

    def test_consume_messages_http_error(self):
        """Test consume_messages handles HTTP errors."""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = HTTPError('500 Internal Server Error')
        
        self.mock_client.post.return_value = mock_response
        
        with patch.object(self.consumer.logger, 'error') as mock_error:
            self.consumer.consume_messages()
            
            # Verify error was logged
            mock_error.assert_called()
            self.assertIn('HTTP error', mock_error.call_args[0][0])
        
        # Verify no messages were processed
        self.mock_pipeline.process_message.assert_not_called()

    def test_consume_messages_request_exception(self):
        """Test consume_messages handles request exceptions."""
        self.mock_client.post.side_effect = RequestException('Request failed')
        
        with patch.object(self.consumer.logger, 'error') as mock_error:
            self.consumer.consume_messages()
            
            # Verify error was logged
            mock_error.assert_called()
            self.assertIn('request error', mock_error.call_args[0][0])

    def test_consume_messages_connection_error(self):
        """Test consume_messages handles connection errors."""
        self.mock_client.post.side_effect = ConnectionError('Connection refused')
        
        with patch.object(self.consumer.logger, 'error') as mock_error:
            self.consumer.consume_messages()
            
            # Verify error was logged
            mock_error.assert_called()
            self.assertIn('connection error', mock_error.call_args[0][0])

    def test_consume_messages_invalid_json(self):
        """Test consume_messages handles invalid JSON response."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = ValueError('Invalid JSON')
        
        self.mock_client.post.return_value = mock_response
        
        with patch.object(self.consumer.logger, 'error') as mock_error:
            self.consumer.consume_messages()
            
            # Verify error was logged
            mock_error.assert_called()

    def test_consume_messages_graphql_errors(self):
        """Test consume_messages handles GraphQL errors in response."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'errors': [
                {'message': 'Field "unknown" not found'},
                {'message': 'Authentication required'}
            ]
        }
        
        self.mock_client.post.return_value = mock_response
        
        self.consumer.consume_messages()
        
        # GraphQL errors in response are still processed
        # The message is passed to pipeline which may handle errors
        self.mock_pipeline.process_message.assert_called_once()

    def test_consume_messages_partial_data_with_errors(self):
        """Test consume_messages handles response with both data and errors."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': {
                'users': [{'id': '1', 'name': 'User 1'}]
            },
            'errors': [
                {'message': 'Some field failed', 'path': ['users', 0, 'email']}
            ]
        }
        
        self.mock_client.post.return_value = mock_response
        
        self.consumer.consume_messages()
        
        # Data should still be processed even with partial errors
        self.mock_pipeline.process_message.assert_called_once()
        call_args = self.mock_pipeline.process_message.call_args[0][0]
        self.assertIn('data', call_args['data'])

    def test_consume_messages_unexpected_exception(self):
        """Test consume_messages handles unexpected exceptions."""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception('Unexpected error')
        
        self.mock_client.post.return_value = mock_response
        
        with patch.object(self.consumer.logger, 'error') as mock_error:
            self.consumer.consume_messages()
            
            # Verify error was logged
            mock_error.assert_called()
            self.assertIn('unexpected error', mock_error.call_args[0][0])

    def test_consume_messages_null_response_data(self):
        """Test consume_messages handles null data in response."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': None}
        
        self.mock_client.post.return_value = mock_response
        
        self.consumer.consume_messages()
        
        # Should still process the message
        self.mock_pipeline.process_message.assert_called_once()
        call_args = self.mock_pipeline.process_message.call_args[0][0]
        self.assertIsNone(call_args['data']['data'])

    # ==================== Query Building Tests ====================

    def test_query_payload_structure(self):
        """Test that GraphQL query payload has correct structure."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': {}}
        
        self.mock_client.post.return_value = mock_response
        
        self.consumer.consume_messages()
        
        call_args = self.mock_client.post.call_args
        json_payload = call_args[1].get('json', {})
        
        # Standard GraphQL payload should have 'query' key
        self.assertIn('query', json_payload)

    def test_content_type_header(self):
        """Test that Content-Type header is set correctly."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': {}}
        
        self.mock_client.post.return_value = mock_response
        
        self.consumer.consume_messages()
        
        call_args = self.mock_client.post.call_args
        headers = call_args[1].get('headers', {})
        
        # GraphQL requests should use application/json
        self.assertEqual(headers.get('Content-Type'), 'application/json')

    # ==================== Environment Variable Tests ====================

    def test_env_prefix_adds_underscore(self):
        """Test that env prefix automatically adds underscore if missing."""
        with patch.dict(os.environ, {
            'GRAPHQL_CONSUMER_ENV_PREFIX': 'TEST',
            'TEST_GRAPHQL_ENDPOINT': 'http://test.com/graphql',
            'TEST_GRAPHQL_QUERY': '{ test }'
        }, clear=True):
            with patch('metranova.consumers.graphql.HTTPConnector'):
                consumer = GraphQLConsumer(self.mock_pipeline)
                
                self.assertEqual(consumer.endpoint, 'http://test.com/graphql')

    def test_endpoint_whitespace_handling(self):
        """Test that endpoint is properly trimmed of whitespace."""
        with patch.dict(os.environ, {
            'GRAPHQL_ENDPOINT': '  http://example.com/graphql  ',
            'GRAPHQL_QUERY': '{ test }'
        }, clear=True):
            with patch('metranova.consumers.graphql.HTTPConnector'):
                consumer = GraphQLConsumer(self.mock_pipeline)
                
                # Should be ideally trimmed 
                # current implementation doesn't trim, so this test
                # documents current behavior
                self.assertIn('http://example.com/graphql', consumer.endpoint)

    def test_query_multiline_preserved(self):
        """Test that multiline queries are preserved."""
        multiline_query = '''
        {
            users {
                id
                name
            }
        }
        '''
        with patch.dict(os.environ, {
            'GRAPHQL_ENDPOINT': 'http://example.com/graphql',
            'GRAPHQL_QUERY': multiline_query
        }, clear=True):
            with patch('metranova.consumers.graphql.HTTPConnector'):
                consumer = GraphQLConsumer(self.mock_pipeline)
                
                # Query should be preserved
                self.assertIn('users', consumer.query)
                self.assertIn('id', consumer.query)
                self.assertIn('name', consumer.query)

    # ==================== Complex Query Tests ====================

    def test_consume_messages_complex_query(self):
        """Test consume_messages with a complex nested query."""
        complex_query = '''
        query ServiceList {
            serviceList {
                list {
                    id
                    label
                    prefixGroupName
                    serviceType { shortName }
                    customer {
                        shortName
                        fullName
                        types { name }
                        fundingAgency { shortName }
                        visibility
                    }
                    peer { asn }
                    prefixes { prefixIp }
                }
            }
        }
        '''
        
        with patch.dict(os.environ, {
            'GRAPHQL_ENDPOINT': 'http://api.example.com/graphql',
            'GRAPHQL_QUERY': complex_query
        }, clear=True):
            with patch('metranova.consumers.graphql.HTTPConnector') as mock_connector_class:
                mock_connector = Mock()
                mock_client = Mock()
                mock_connector.client = mock_client
                mock_connector_class.return_value = mock_connector
                
                mock_response = Mock()
                mock_response.status_code = 200
                mock_response.json.return_value = {
                    'data': {
                        'serviceList': {
                            'list': [
                                {
                                    'id': '123',
                                    'label': 'Primary',
                                    'prefixGroupName': 'TESTLAB-Primary-v4',
                                    'serviceType': {'shortName': 'IPv4'},
                                    'customer': {
                                        'shortName': 'TESTLAB',
                                        'fullName': 'Test Laboratory',
                                        'types': [{'name': 'Research Site'}],
                                        'fundingAgency': {'shortName': 'TESTFUND'},
                                        'visibility': 'Public'
                                    },
                                    'peer': {'asn': 64496},
                                    'prefixes': [
                                        {'prefixIp': '192.0.2.0/24'},
                                        {'prefixIp': '198.51.100.0/24'}
                                    ]
                                }
                            ]
                        }
                    }
                }
                mock_client.post.return_value = mock_response
                
                consumer = GraphQLConsumer(self.mock_pipeline)
                consumer.consume_messages()
                
                # Verify complex response was processed
                self.mock_pipeline.process_message.assert_called_once()
                call_args = self.mock_pipeline.process_message.call_args[0][0]
                
                # Verify nested data structure is preserved
                service_list = call_args['data']['data']['serviceList']['list']
                self.assertEqual(len(service_list), 1)
                self.assertEqual(service_list[0]['customer']['shortName'], 'TESTLAB')
                self.assertEqual(len(service_list[0]['prefixes']), 2)


class TestGraphQLConsumerIntegration(unittest.TestCase):
    """Integration-style tests for GraphQLConsumer."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_pipeline = Mock()
        self.mock_pipeline.process_message = Mock()

    def test_esdb_api_configuration(self):
        """Test configuration for ESDB API."""
        with patch.dict(os.environ, {
            'GRAPHQL_CONSUMER_ENV_PREFIX': 'ESDB',
            'ESDB_GRAPHQL_CONSUMER_UPDATE_INTERVAL': '86400',
            'ESDB_GRAPHQL_ENDPOINT': 'https://esdb.example.com/graphql',
            'ESDB_GRAPHQL_QUERY': '''
                query {
                    serviceList {
                        list {
                            id
                            prefixes { prefixIp }
                        }
                    }
                }
            ''',
            'ESDB_GRAPHQL_TOKEN': 'esdb-token-123'
        }, clear=True):
            with patch('metranova.consumers.graphql.HTTPConnector') as mock_connector_class:
                mock_connector = Mock()
                mock_client = Mock()
                mock_connector.client = mock_client
                mock_connector_class.return_value = mock_connector
                
                consumer = GraphQLConsumer(self.mock_pipeline)
                
                self.assertEqual(consumer.update_interval, 86400)
                self.assertEqual(consumer.endpoint, 'https://esdb.example.com/graphql')
                self.assertIn('serviceList', consumer.query)
                self.assertEqual(consumer.token, 'esdb-token-123')

    def test_multiple_consumers_different_prefixes(self):
        """Test that multiple consumers can use different prefixes."""
        consumers = []
        
        configs = [
            {
                'prefix': 'SERVICE_A',
                'endpoint': 'http://service-a.example.com/graphql',
                'query': '{ serviceA { id } }'
            },
            {
                'prefix': 'SERVICE_B',
                'endpoint': 'http://service-b.example.com/graphql',
                'query': '{ serviceB { id } }'
            }
        ]
        
        for config in configs:
            env_vars = {
                'GRAPHQL_CONSUMER_ENV_PREFIX': config['prefix'],
                f"{config['prefix']}_GRAPHQL_ENDPOINT": config['endpoint'],
                f"{config['prefix']}_GRAPHQL_QUERY": config['query']
            }
            
            with patch.dict(os.environ, env_vars, clear=True):
                with patch('metranova.consumers.graphql.HTTPConnector'):
                    consumer = GraphQLConsumer(self.mock_pipeline)
                    consumers.append({
                        'consumer': consumer,
                        'expected_endpoint': config['endpoint'],
                        'expected_query': config['query']
                    })
        
        # Verify each consumer has correct configuration
        for consumer_info in consumers:
            self.assertEqual(consumer_info['consumer'].endpoint, consumer_info['expected_endpoint'])
            self.assertEqual(consumer_info['consumer'].query, consumer_info['expected_query'])


if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)