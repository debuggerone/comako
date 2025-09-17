"""
APERAK Response Generator

This module generates EDIFACT APERAK (Application Error and Acknowledgment) 
messages in response to received EDI messages. APERAK is used to acknowledge
receipt and processing status of EDI messages.
"""

import uuid
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Literal
import logging

logger = logging.getLogger(__name__)


class APERAKGenerator:
    """
    Generates APERAK (Application Error and Acknowledgment) messages.
    
    APERAK is used in EDI communication to:
    - Acknowledge receipt of messages
    - Report processing status (accepted/rejected)
    - Provide error details for rejected messages
    """
    
    def __init__(self, sender_id: str = "COMAKO", recipient_id: str = None):
        """
        Initialize APERAK generator.
        
        Args:
            sender_id: Identifier for the sender (our system)
            recipient_id: Default recipient identifier
        """
        self.sender_id = sender_id
        self.default_recipient_id = recipient_id
        
        # APERAK response codes
        self.response_codes = {
            "accepted": "29",      # Message accepted
            "rejected": "27",      # Message rejected
            "partially_accepted": "28",  # Message partially accepted
            "received": "26"       # Message received (acknowledgment only)
        }
        
        # Error codes for common issues
        self.error_codes = {
            "syntax_error": "2",
            "segment_missing": "12",
            "segment_invalid": "13",
            "data_element_missing": "15",
            "data_element_invalid": "16",
            "segment_sequence_error": "17",
            "duplicate_message": "18",
            "message_type_not_supported": "19"
        }
    
    def generate_aperak(
        self,
        original_message: Dict[str, Any],
        status: Literal["accepted", "rejected", "partially_accepted", "received"],
        errors: Optional[List[Dict[str, Any]]] = None,
        recipient_id: Optional[str] = None
    ) -> str:
        """
        Generate APERAK response message.
        
        Args:
            original_message: The original EDI message being acknowledged
            status: Processing status of the original message
            errors: List of errors (if any) with the original message
            recipient_id: Override recipient ID
            
        Returns:
            APERAK message as EDIFACT string
        """
        try:
            # Extract information from original message
            original_ref = self._extract_message_reference(original_message)
            original_type = self._extract_message_type(original_message)
            original_sender = self._extract_sender(original_message)
            
            # Use provided recipient or extract from original message
            recipient = recipient_id or original_sender or self.default_recipient_id
            
            if not recipient:
                raise ValueError("No recipient ID available for APERAK generation")
            
            # Generate APERAK components
            interchange_ref = self._generate_reference()
            message_ref = self._generate_reference()
            timestamp = self._generate_timestamp()
            
            # Build APERAK message
            aperak_segments = []
            
            # UNB - Interchange Header
            aperak_segments.append(
                f"UNB+UNOC:3+{self.sender_id}+{recipient}+{timestamp}+{interchange_ref}'"
            )
            
            # UNH - Message Header
            aperak_segments.append(
                f"UNH+{message_ref}+APERAK:D:03B:UN:EEG+1.1e'"
            )
            
            # BGM - Beginning of Message
            aperak_segments.append(
                f"BGM+916+{message_ref}+{self.response_codes[status]}'"
            )
            
            # DTM - Date/Time
            aperak_segments.append(
                f"DTM+137:{self._format_date(datetime.now(timezone.utc))}:102'"
            )
            
            # RFF - Reference to original message
            if original_ref:
                aperak_segments.append(f"RFF+ACW:{original_ref}'")
            
            # ERC - Application Error Information (if errors exist)
            if errors and status in ["rejected", "partially_accepted"]:
                for error in errors:
                    error_code = error.get("code", "16")  # Default to data element invalid
                    error_desc = error.get("description", "Unspecified error")
                    aperak_segments.append(f"ERC+{error_code}:{error_desc}'")
            
            # FTX - Free Text (additional information)
            status_text = self._get_status_text(status)
            aperak_segments.append(f"FTX+AAO+++{status_text}'")
            
            # UNT - Message Trailer
            segment_count = len(aperak_segments) + 1  # +1 for UNT itself
            aperak_segments.append(f"UNT+{segment_count}+{message_ref}'")
            
            # UNZ - Interchange Trailer
            aperak_segments.append(f"UNZ+1+{interchange_ref}'")
            
            # Join segments with newlines
            aperak_message = "\n".join(aperak_segments)
            
            logger.info(f"Generated APERAK response: {status} for message {original_ref}")
            return aperak_message
            
        except Exception as e:
            logger.error(f"Error generating APERAK: {e}")
            raise ValueError(f"APERAK generation failed: {e}")
    
    def generate_acceptance_aperak(
        self,
        original_message: Dict[str, Any],
        recipient_id: Optional[str] = None
    ) -> str:
        """
        Generate APERAK for accepted message.
        
        Args:
            original_message: The original EDI message
            recipient_id: Override recipient ID
            
        Returns:
            APERAK acceptance message
        """
        return self.generate_aperak(
            original_message=original_message,
            status="accepted",
            recipient_id=recipient_id
        )
    
    def generate_rejection_aperak(
        self,
        original_message: Dict[str, Any],
        errors: List[Dict[str, Any]],
        recipient_id: Optional[str] = None
    ) -> str:
        """
        Generate APERAK for rejected message.
        
        Args:
            original_message: The original EDI message
            errors: List of errors that caused rejection
            recipient_id: Override recipient ID
            
        Returns:
            APERAK rejection message
        """
        return self.generate_aperak(
            original_message=original_message,
            status="rejected",
            errors=errors,
            recipient_id=recipient_id
        )
    
    def generate_acknowledgment_aperak(
        self,
        original_message: Dict[str, Any],
        recipient_id: Optional[str] = None
    ) -> str:
        """
        Generate APERAK for message receipt acknowledgment.
        
        Args:
            original_message: The original EDI message
            recipient_id: Override recipient ID
            
        Returns:
            APERAK acknowledgment message
        """
        return self.generate_aperak(
            original_message=original_message,
            status="received",
            recipient_id=recipient_id
        )
    
    def _extract_message_reference(self, message: Dict[str, Any]) -> Optional[str]:
        """Extract message reference from original message."""
        # Try different possible locations for message reference
        if isinstance(message, dict):
            # Check UNH segment
            if "UNH" in message:
                unh = message["UNH"]
                if isinstance(unh, list) and len(unh) > 0:
                    return unh[0]
            
            # Check message_header
            if "message_header" in message:
                header = message["message_header"]
                if isinstance(header, dict) and "reference_number" in header:
                    return header["reference_number"]
            
            # Check segments for UNH
            if "segments" in message:
                for segment in message["segments"]:
                    if segment.get("segment_type") == "UNH":
                        data = segment.get("data", {})
                        if "message_header" in data:
                            return data["message_header"].get("reference_number")
        
        return None
    
    def _extract_message_type(self, message: Dict[str, Any]) -> Optional[str]:
        """Extract message type from original message."""
        if isinstance(message, dict):
            # Direct message_type field
            if "message_type" in message:
                return message["message_type"]
            
            # Check UNH segment
            if "UNH" in message:
                unh = message["UNH"]
                if isinstance(unh, list) and len(unh) > 1:
                    return unh[1]
        
        return None
    
    def _extract_sender(self, message: Dict[str, Any]) -> Optional[str]:
        """Extract sender from original message."""
        if isinstance(message, dict):
            # Check UNB segment
            if "UNB" in message:
                unb = message["UNB"]
                if isinstance(unb, list) and len(unb) > 1:
                    return unb[1]
            
            # Check interchange_header
            if "interchange_header" in message:
                header = message["interchange_header"]
                if isinstance(header, dict) and "sender" in header:
                    return header["sender"]
        
        return None
    
    def _generate_reference(self) -> str:
        """Generate unique reference number."""
        return str(uuid.uuid4()).replace("-", "")[:12].upper()
    
    def _generate_timestamp(self) -> str:
        """Generate timestamp in EDIFACT format."""
        now = datetime.now(timezone.utc)
        return f"{now.strftime('%y%m%d')}:{now.strftime('%H%M')}+00"
    
    def _format_date(self, dt: datetime) -> str:
        """Format date for EDIFACT DTM segment."""
        return dt.strftime('%Y%m%d')
    
    def _get_status_text(self, status: str) -> str:
        """Get human-readable status text."""
        status_texts = {
            "accepted": "Message processed successfully",
            "rejected": "Message rejected due to errors",
            "partially_accepted": "Message partially processed",
            "received": "Message received and queued for processing"
        }
        return status_texts.get(status, "Unknown status")


class APERAKValidator:
    """
    Validates APERAK messages for correctness.
    """
    
    @staticmethod
    def validate_aperak_structure(aperak_message: str) -> bool:
        """
        Validate basic APERAK message structure.
        
        Args:
            aperak_message: APERAK message string
            
        Returns:
            True if structure is valid, False otherwise
        """
        try:
            lines = aperak_message.strip().split('\n')
            
            # Check minimum required segments
            required_segments = ['UNB', 'UNH', 'BGM', 'UNT', 'UNZ']
            found_segments = []
            
            for line in lines:
                if line.startswith(tuple(required_segments)):
                    segment_type = line[:3]
                    if segment_type not in found_segments:
                        found_segments.append(segment_type)
            
            # Check all required segments are present
            return all(seg in found_segments for seg in required_segments)
            
        except Exception:
            return False
    
    @staticmethod
    def validate_aperak_response_code(aperak_message: str) -> bool:
        """
        Validate APERAK response code in BGM segment.
        
        Args:
            aperak_message: APERAK message string
            
        Returns:
            True if response code is valid, False otherwise
        """
        try:
            lines = aperak_message.strip().split('\n')
            valid_codes = ["26", "27", "28", "29"]
            
            for line in lines:
                if line.startswith('BGM'):
                    # Extract response code from BGM segment
                    parts = line.split('+')
                    if len(parts) >= 4:
                        response_code = parts[3].rstrip("'")
                        return response_code in valid_codes
            
            return False
            
        except Exception:
            return False


def generate_aperak_for_message(
    original_message: Dict[str, Any],
    status: Literal["accepted", "rejected", "partially_accepted", "received"],
    errors: Optional[List[Dict[str, Any]]] = None,
    sender_id: str = "COMAKO"
) -> str:
    """
    Convenience function to generate APERAK response.
    
    Args:
        original_message: The original EDI message
        status: Processing status
        errors: List of errors (if any)
        sender_id: Sender identifier
        
    Returns:
        APERAK message string
    """
    generator = APERAKGenerator(sender_id=sender_id)
    return generator.generate_aperak(
        original_message=original_message,
        status=status,
        errors=errors
    )


def create_error_list(error_descriptions: List[str]) -> List[Dict[str, Any]]:
    """
    Create error list for APERAK generation.
    
    Args:
        error_descriptions: List of error description strings
        
    Returns:
        List of error dictionaries
    """
    errors = []
    for desc in error_descriptions:
        errors.append({
            "code": "16",  # Default to data element invalid
            "description": desc
        })
    return errors


def validate_aperak_message(aperak_message: str) -> Dict[str, bool]:
    """
    Validate APERAK message comprehensively.
    
    Args:
        aperak_message: APERAK message string
        
    Returns:
        Dictionary with validation results
    """
    return {
        "structure_valid": APERAKValidator.validate_aperak_structure(aperak_message),
        "response_code_valid": APERAKValidator.validate_aperak_response_code(aperak_message)
    }
