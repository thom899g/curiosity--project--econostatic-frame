"""
Mythos Engineering Module for Econostatic Frame
Manages the North Star narrative and version-controlled mythos documents
"""
import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path

from firebase_init import firebase_manager, FirebaseError

logger = logging.getLogger(__name__)

class MythosEngine:
    """Manages the North Star narrative and mythos versioning"""
    
    def __init__(self):
        self.firestore = None
        self.collection_name = "mythos"
        self.current_version = None
        
    def initialize(self) -> bool:
        """Initialize mythos engine with Firebase connection"""
        try:
            self.firestore = firebase_manager.get_firestore()
            
            # Create mythos collection if it doesn't exist
            self._ensure_collection_exists()
            
            # Load latest version
            self._load_latest_version()
            
            logger.info("Mythos engine initialized")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize mythos engine: {e}")
            return False
    
    def _ensure_collection_exists(self) -> None:
        """Ensure mythos collection exists with initial structure"""
        try:
            # Try to get collection count to check existence
            docs = self.firestore.collection(self.collection_name).limit(1).get()
            
            # If empty, create initial structure
            if len(list(docs)) == 0:
                initial_doc = {
                    "type": "collection_metadata",
                    "created_at": firestore.SERVER_TIMESTAMP,
                    "total_versions": 0,
                    "active_version": None
                }
                self.firestore.collection(self.collection_name).document("_metadata").set(initial_doc)
                logger.info("Created initial mythos collection structure")
                
        except FirebaseError as e:
            logger.error(f"Error ensuring collection exists: {e}")
            raise
    
    def _load_latest_version(self) -> Optional[str]:
        """Load the latest active version number"""
        try:
            metadata_ref = self.firestore.collection(self.collection_name).document("_metadata")
            metadata = metadata_ref.get()
            
            if metadata.exists:
                self.current_version = metadata.to_dict().get("active_version")
                return self.current_version
            return None
            
        except FirebaseError as e:
            logger.error(f"Error loading latest version: {e}")
            return None
    
    def create_version(self, mythos_data: Dict[str, Any], 
                      author: str = "system") -> Optional[str]:
        """
        Create a new version of the mythos
        
        Args:
            mythos_data: Complete mythos document
            author: Creator identifier
            
        Returns:
            str: Version ID if successful, None otherwise
        """
        try:
            # Validate mythos data structure
            if not self._validate_mythos_data(mythos_data):
                logger.error("Invalid mythos data structure")
                return None
            
            # Generate version ID (timestamp-based)
            version_id = f"v{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
            
            # Prepare document
            mythos_doc = {
                **mythos_data,
                "version": version_id,
                "author": author,
                "created_at": firestore.SERVER_TIMESTAMP,
                "status": "active"
            }
            
            # Store in Firestore
            doc_ref = self.firestore.collection(self.collection_name).document(version_id)
            doc_ref.set(mythos_doc)
            
            # Update metadata
            self._update_metadata(version_id)
            
            logger.info(f"Created mythos version: {version_id}")
            self.current_version = version_id
            return version_id
            
        except Exception as e:
            logger.error(f"Error creating mythos version: {e}")
            return None
    
    def _validate_mythos_data(self, data: Dict[str, Any]) -> bool:
        """Validate mythos data structure"""
        required_sections = [
            "north_star_statement",
            "heros_journey",
            "enemy_definition", 
            "transcendent_goal",
            "core_principles"
        ]
        
        try:
            for section in required_sections:
                if section not in data:
                    logger.error(f"Missing required section: {section}")
                    return False
            
            # Validate types
            if not isinstance(data["north_star_statement"], str):
                logger.error("north_star_statement must be a string")
                return False
                
            if not isinstance(data["heros_journey"], dict):
                logger.error("heros_journey must be a dict")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Validation error: {e}")
            return False
    
    def _update_metadata(self, new_version: str) -> None:
        """Update collection metadata"""
        try:
            metadata_ref = self.firestore.collection(self.collection_name).document("_metadata")
            
            # Get current metadata
            metadata = metadata_ref.get()
            current_data = metadata.to_dict() if metadata.exists else {}
            
            # Update metadata
            update_data = {
                "active_version": new_version,
                "updated_at": firestore.SERVER_TIMESTAMP,
                "total_versions": current_data.get("total_versions", 0) + 1
            }
            
            metadata_ref.set(update_data, merge=True)
            
        except FirebaseError as e:
            logger.error(f"Error updating metadata: {e}")
            raise
    
    def get_version(self, version_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Get specific version of mythos
        
        Args:
            version_id: Version to retrieve (defaults to active version)
            
        Returns:
            Dict containing mythos data or None if not found
        """
        try:
            if version_id is None:
                version_id = self.current_version
                if version_id is None:
                    logger.error("No active version available")
                    return None
            
            doc_ref = self.firestore.collection(self.collection_name).document(version_id)
            doc = doc_ref.get()
            
            if doc.exists:
                return doc.to_dict()
            else:
                logger.warning(f"Version not found: {version_id}")
                return None
                
        except FirebaseError as e:
            logger.error(f"Error getting version: {e}")
            return None
    
    def list_versions(self) -> List[Dict[str, Any]]:
        """List all mythos versions"""
        try:
            versions = []
            docs = self.firestore.collection(self.collection_name).get()
            
            for doc in docs:
                data = doc.to_dict()
                if doc.id != "_metadata":  # Skip metadata document
                    versions.append({
                        "id": doc.id,
                        "version": data.get("version"),
                        "author": data.get("author"),
                        "created_at": data.get("created_at"),
                        "status": data.get("status")
                    })
            
            return sorted(versions, key=lambda x: x.get("created_at", ""), reverse=True)
            
        except FirebaseError as e:
            logger.error(f"Error listing versions: {e}")
            return []

def create_primal_mythos() -> Dict[str, Any]:
    """Create the primal mythos document for Econostatic Frame"""
    
    return {
        "north_star_statement": """
        To architect fertile soil for spontaneous economic organisms that evolve 
        through subsidiarity, withstand all agent types, and transform human 
        ambition into executable physics without central control.
        """,
        
        "heros_journey": {
            "ordinary_world": "Participant operates in traditional zero-sum economy",
            "call_to_adventure": "Discovers Protocell with transparent economic physics",
            "refusal_of_call": "Skepticism about non-financializable Agency Proof",
            "meeting_mentor": "Core Simulator demonstrates resilience to parasitic agents",
            "crossing_threshold": "Mints first Agency Proof via verifiable contribution",
            "tests_allies_enemies": "Navigates Protocell dynamics, encounters parasitic agents",
            "approach": "Contributes to Sovereignty Milestone calculation",
            "ordeal": "Economic attack on Protocell tests system resilience",
            "reward": "Receives Capital Units from successful defense",
            "the_road_back": "Protocell integrates innovation into Core Frame",
            "resurrection": "Becomes Protocell founder with charter minting rights",
            "return_with_elixir": "Leads new Protocell with evolved economic model"
        },
        
        "enemy_definition": {
            "stagnation": "Economic models that cannot evolve without central planners",
            "parasitism": "Agents who extract value without contributing to the commons",
            "apathy": "Failure to participate despite clear value propositions",
            "complexity_bottlenecks": "Governance models that require human voting",
            "financialization_loops": "Turning status signals into speculative assets"
        },
        
        "transcendent_goal": """
        To create an economic frame so robust that it becomes the default substrate
        for human coordination, where ambition automatically finds its optimal
        expression through recursive Protocell experimentation.
        """,
        
        "core_principles": [
            "Subsidiarity: Evolution occurs at the edges, not the center",
            "Physics, Not Politics: Economic rules are computational, not democratic",
            "Status Over Speculation: Agency Proof is non-transferable",
            "Irrationality Vault: Value destruction for status is a core feature",
            "Anti-Fragility: Systems strengthen through stress, not avoid it"
        ],
        
        "archetypes": {
            "rational_agents": "Maximize utility following clear incentives",
            "parasitic_agents": "Seek to extract value without contribution", 
            "apathetic_agents": "Observe but don't participate without excessive incentive",
            "irrational_agents": "Burn Capital Units for unique Artifacts with no financial value"
        }
    }

# Example usage
if __name__ == "__main__":
    # Initialize Firebase
    if not firebase_manager.initialize():
        print("Failed to initialize Firebase")
        exit(1)
    
    # Create mythos engine
    mythos_engine = MythosEngine()
    if mythos_engine.initialize():
        # Create primal mythos
        primal_mythos = create_primal_mythos()
        version_id = mythos_engine.create_version(