"""
Cassandra client for the Messenger application.
This provides a connection to the Cassandra database.
"""
import os
import uuid
from typing import List, Dict, Any, Optional, Tuple, Union
from datetime import datetime
import logging
import time

from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, dict_factory

logger = logging.getLogger(__name__)

class CassandraClient:
    """Singleton Cassandra client for the application."""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(CassandraClient, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize the Cassandra connection."""
        if self._initialized:
            return
        
        self.host = os.getenv("CASSANDRA_HOST", "localhost")
        self.port = int(os.getenv("CASSANDRA_PORT", "9042"))
        self.keyspace = os.getenv("CASSANDRA_KEYSPACE", "messenger")
        
        self.cluster = None
        self.session = None
        self._initialized = True
        try:
            self.connect()
        except Exception as e:
            logger.warning(f"Initial connection failed: {str(e)}")
            try:
                self.connect_without_keyspace()
            except Exception as e:
                logger.error(f"Failed to connect without keyspace: {str(e)}")
    
    def connect_without_keyspace(self) -> None:
        try:
            self.cluster = Cluster([self.host])
            self.session = self.cluster.connect()
            self.session.row_factory = dict_factory
            logger.info(f"Connected to Cassandra at {self.host}:{self.port} without keyspace")
            self.session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {self.keyspace} 
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 3}}
            """)
            self.session.set_keyspace(self.keyspace)
            logger.info(f"Using keyspace: {self.keyspace}")
        except Exception as e:
            logger.error(f"Failed to connect without keyspace: {str(e)}")
            raise
    
    def connect(self) -> None:
        """Connect to the Cassandra cluster."""
        retry_count = 0
        max_retries = 5
        
        while retry_count < max_retries:
            try:
                self.cluster = Cluster([self.host])
                self.session = self.cluster.connect(self.keyspace)
                self.session.row_factory = dict_factory
                logger.info(f"Connected to Cassandra at {self.host}:{self.port}, keyspace: {self.keyspace}")
                return
            except Exception as e:
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = 2 ** retry_count 
                    logger.warning(f"Connection attempt {retry_count} failed: {str(e)}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to connect to Cassandra after {max_retries} attempts: {str(e)}")
                    raise
    
    def close(self) -> None:
        """Close the Cassandra connection."""
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Cassandra connection closed")
    
    def execute(self, query: str, params: Union[Tuple, Dict, None] = None) -> List[Dict[str, Any]]:
        """
        Execute a CQL query.
        
        Args:
            query: The CQL query string
            params: The parameters for the query
            
        Returns:
            List of rows as dictionaries
        """
        if not self.session:
            self.connect()
        
        try:
            statement = SimpleStatement(query)
            if params is None:
                result = self.session.execute(statement)
            else:
                result = self.session.execute(statement, params)
            return list(result)
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise
    
    def execute_async(self, query: str, params: Union[Tuple, Dict, None] = None):
        """
        Execute a CQL query asynchronously.
        
        Args:
            query: The CQL query string
            params: The parameters for the query
            
        Returns:
            Async result object
        """
        if not self.session:
            self.connect()
        
        try:
            statement = SimpleStatement(query)
            if params is None:
                return self.session.execute_async(statement)
            else:
                return self.session.execute_async(statement, params)
        except Exception as e:
            logger.error(f"Async query execution failed: {str(e)}")
            raise
    
    def get_session(self) -> Session:
        """Get the Cassandra session."""
        if not self.session:
            self.connect()
        return self.session

# Create a global instance
cassandra_client = CassandraClient() 