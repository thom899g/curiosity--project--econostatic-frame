"""
Firebase Initialization Module for Econostatic Frame
Handles secure connection to Firebase services with comprehensive error handling
"""
import os
import json
import logging
from typing import Dict, Any, Optional
from pathlib import Path

import firebase_admin
from firebase_admin import credentials, firestore, db
from firebase_admin.exceptions import FirebaseError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FirebaseManager:
    """Manages Firebase connections and provides access to services"""
    
    # Singleton instance
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(FirebaseManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self.app = None
            self.firestore_client = None
            self.realtime_db = None
            self._initialized = True
    
    def initialize(self, service_account_path: str = "serviceAccountKey.json") -> bool:
        """
        Initialize Firebase connection with robust error handling
        
        Args:
            service_account_path: Path to service account key file
            
        Returns:
            bool: True if initialization successful, False otherwise
        """
        try:
            # Validate service account file exists
            if not os.path.exists(service_account_path):
                logger.error(f"Service account file not found: {service_account_path}")
                return False
            
            # Load and validate service account
            with open(service_account_path, 'r') as f:
                service_account = json.load(f)
            
            required_fields = ['type', 'project_id', 'private_key_id', 
                              'private_key', 'client_email']
            for field in required_fields:
                if field not in service_account:
                    logger.error(f"Missing required field in service account: {field}")
                    return False
            
            # Initialize Firebase app if not already initialized
            if not firebase_admin._apps:
                cred = credentials.Certificate(service_account_path)
                self.app = firebase_admin.initialize_app(cred, {
                    'databaseURL': f'https://{service_account["project_id"]}.firebaseio.com'
                })
                logger.info(f"Firebase initialized for project: {service_account['project_id']}")
            else:
                self.app = firebase_admin.get_app()
                logger.info("Using existing Firebase app")
            
            # Initialize clients
            self.firestore_client = firestore.client()
            self.realtime_db = db.reference()
            
            # Test connection
            self._test_connections()
            
            logger.info("Firebase initialization successful")
            return True
            
        except FileNotFoundError as e:
            logger.error(f"Service account file not found: {e}")
            return False
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in service account: {e}")
            return False
        except ValueError as e:
            logger.error(f"Invalid service account configuration: {e}")
            return False
        except FirebaseError as e:
            logger.error(f"Firebase initialization error: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during Firebase initialization: {e}")
            return False
    
    def _test_connections(self) -> None:
        """Test connections to Firebase services"""
        try:
            # Test Firestore connection
            test_ref = self.firestore_client.collection('connection_test').document('test')
            test_ref.set({'timestamp': firestore.SERVER_TIMESTAMP})
            test_ref.delete()
            logger.debug("Firestore connection test passed")
            
            # Test Realtime Database connection
            test_ref = self.realtime_db.child('connection_test')
            test_ref.set({'timestamp': {'.sv': 'timestamp'}})
            test_ref.delete()
            logger.debug("Realtime Database connection test passed")
            
        except Exception as e:
            logger.warning(f"Connection test warning: {e}")
    
    def get_firestore(self) -> firestore.Client:
        """Get Firestore client with validation"""
        if not self.firestore_client:
            raise RuntimeError("Firebase not initialized. Call initialize() first.")
        return self.firestore_client
    
    def get_realtime_db(self) -> db.Reference:
        """Get Realtime Database reference with validation"""
        if not self.realtime_db:
            raise RuntimeError("Firebase not initialized. Call initialize() first.")
        return self.realtime_db
    
    def cleanup(self) -> None:
        """Clean up Firebase connections"""
        try:
            if self.app:
                firebase_admin.delete_app(self.app)
                logger.info("Firebase app cleaned up")
        except Exception as e:
            logger.error(f"Error during Firebase cleanup: {e}")

# Global instance for easy access
firebase_manager = FirebaseManager()