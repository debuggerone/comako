"""
AS4 Integration Module for SAP IS-U Compatibility

This module provides AS4 (Applicability Statement 4) functionality for secure
EDI message exchange with SAP IS-U systems, implementing OASIS ebMS 3.0 standards.

AS4 is the modern standard for B2B message exchange, providing:
- Web Services-based architecture (SOAP 1.2)
- Enhanced security and reliability
- Message-level security with WS-Security
- Reliable messaging with receipts and retries
- Mandatory for certified software solutions
"""

import os
import asyncio
import logging
import xml.etree.ElementTree as ET
from typing import Dict, Any, Optional, List, Union
from datetime import datetime, timezone
import hashlib
import base64
import uuid
from pathlib import Path
import tempfile
import json

# Note: In a real implementation, you would use proper SOAP/WS-Security libraries
# For this demo, we'll simulate AS4 functionality with proper structure
logger = logging.getLogger(__name__)


class AS4Error(Exception):
    """Custom exception for AS4 operations."""
    pass


class AS4Security:
    """
    AS4 Security handler for WS-Security implementation.
    
    Handles digital signatures, encryption, and security tokens
    according to OASIS ebMS 3.0 specification.
    """
    
    def __init__(self, certificate_path: str, private_key_path: str):
        """
        Initialize AS4 security.
        
        Args:
            certificate_path: Path to X.509 certificate
            private_key_path: Path to private key
        """
        self.certificate_path = certificate_path
        self.private_key_path = private_key_path
        self._cert_data = None
        self._key_data = None
    
    def load_certificates(self) -> bool:
        """Load security certificates."""
        try:
            # In real implementation, use cryptography library
            if os.path.exists(self.certificate_path) and os.path.exists(self.private_key_path):
                with open(self.certificate_path, 'rb') as f:
                    self._cert_data = f.read()
                with open(self.private_key_path, 'rb') as f:
                    self._key_data = f.read()
                logger.info("AS4 certificates loaded successfully")
                return True
            else:
                # Create dummy certificate data for demo
                self._cert_data = b"-----BEGIN CERTIFICATE-----\nAS4_CERT_DATA\n-----END CERTIFICATE-----"
                self._key_data = b"-----BEGIN PRIVATE KEY-----\nAS4_KEY_DATA\n-----END PRIVATE KEY-----"
                logger.warning("Using dummy AS4 certificate data for demo")
                return True
        except Exception as e:
            logger.error(f"Failed to load AS4 certificates: {e}")
            return False
    
    def create_security_header(self, message_id: str) -> str:
        """
        Create WS-Security header for AS4 message.
        
        Args:
            message_id: Unique message identifier
            
        Returns:
            XML security header
        """
        timestamp = datetime.now(timezone.utc).isoformat()
        
        security_header = f"""
        <wsse:Security xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"
                       xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
            <wsu:Timestamp wsu:Id="TS-{message_id}">
                <wsu:Created>{timestamp}</wsu:Created>
                <wsu:Expires>{timestamp}</wsu:Expires>
            </wsu:Timestamp>
            <wsse:BinarySecurityToken 
                ValueType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-x509-token-profile-1.0#X509v3"
                EncodingType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary"
                wsu:Id="X509-{message_id}">
                {base64.b64encode(self._cert_data or b'DUMMY_CERT').decode('utf-8')}
            </wsse:BinarySecurityToken>
            <ds:Signature xmlns:ds="http://www.w3.org/2000/09/xmldsig#">
                <ds:SignedInfo>
                    <ds:CanonicalizationMethod Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#"/>
                    <ds:SignatureMethod Algorithm="http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"/>
                    <ds:Reference URI="#Body-{message_id}">
                        <ds:DigestMethod Algorithm="http://www.w3.org/2001/04/xmlenc#sha256"/>
                        <ds:DigestValue>DUMMY_DIGEST_VALUE</ds:DigestValue>
                    </ds:Reference>
                </ds:SignedInfo>
                <ds:SignatureValue>DUMMY_SIGNATURE_VALUE</ds:SignatureValue>
                <ds:KeyInfo>
                    <wsse:SecurityTokenReference>
                        <wsse:Reference URI="#X509-{message_id}"/>
                    </wsse:SecurityTokenReference>
                </ds:KeyInfo>
            </ds:Signature>
        </wsse:Security>
        """
        return security_header.strip()
    
    def verify_signature(self, soap_message: str) -> bool:
        """
        Verify digital signature of incoming AS4 message.
        
        Args:
            soap_message: SOAP message with signature
            
        Returns:
            True if signature is valid
        """
        # In real implementation, verify XML signature
        logger.debug("Verifying AS4 message signature")
        return True  # Simplified for demo


class AS4Message:
    """
    AS4 Message container implementing OASIS ebMS 3.0 specification.
    
    Provides structured message handling with proper SOAP envelope,
    ebMS headers, and security features.
    """
    
    def __init__(
        self,
        message_id: str,
        conversation_id: str,
        from_party_id: str,
        from_party_type: str,
        to_party_id: str,
        to_party_type: str,
        service: str,
        action: str,
        payload: bytes,
        content_type: str = "application/xml"
    ):
        """
        Initialize AS4 message.
        
        Args:
            message_id: Unique message identifier
            conversation_id: Conversation identifier for message correlation
            from_party_id: Sender party identifier
            from_party_type: Sender party type
            to_party_id: Recipient party identifier  
            to_party_type: Recipient party type
            service: Service identifier
            action: Action to be performed
            payload: Message payload
            content_type: MIME content type
        """
        self.message_id = message_id
        self.conversation_id = conversation_id
        self.from_party_id = from_party_id
        self.from_party_type = from_party_type
        self.to_party_id = to_party_id
        self.to_party_type = to_party_type
        self.service = service
        self.action = action
        self.payload = payload
        self.content_type = content_type
        self.timestamp = datetime.now(timezone.utc)
        self.ref_to_message_id = None
        self.message_properties = {}
    
    def add_message_property(self, name: str, value: str, type_attr: str = "string") -> None:
        """Add message property."""
        self.message_properties[name] = {
            "value": value,
            "type": type_attr
        }
    
    def create_soap_envelope(self, security_header: str = "") -> str:
        """
        Create SOAP 1.2 envelope with ebMS 3.0 headers.
        
        Args:
            security_header: WS-Security header
            
        Returns:
            Complete SOAP envelope as XML string
        """
        # Create message properties XML
        properties_xml = ""
        for name, prop in self.message_properties.items():
            properties_xml += f"""
                <eb:Property name="{name}" type="{prop['type']}">{prop['value']}</eb:Property>
            """
        
        # Create payload reference
        payload_cid = f"payload-{self.message_id}@comako.energy"
        payload_b64 = base64.b64encode(self.payload).decode('utf-8')
        
        soap_envelope = f"""<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope"
               xmlns:eb="http://docs.oasis-open.org/ebxml-msg/ebms/v3.0/ns/core/200704/"
               xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
    <soap:Header>
        {security_header}
        <eb:Messaging soap:mustUnderstand="true">
            <eb:UserMessage>
                <eb:MessageInfo>
                    <eb:Timestamp>{self.timestamp.isoformat()}</eb:Timestamp>
                    <eb:MessageId>{self.message_id}</eb:MessageId>
                    <eb:ConversationId>{self.conversation_id}</eb:ConversationId>
                </eb:MessageInfo>
                <eb:PartyInfo>
                    <eb:From>
                        <eb:PartyId type="{self.from_party_type}">{self.from_party_id}</eb:PartyId>
                        <eb:Role>http://docs.oasis-open.org/ebxml-msg/ebms/v3.0/ns/core/200704/initiator</eb:Role>
                    </eb:From>
                    <eb:To>
                        <eb:PartyId type="{self.to_party_type}">{self.to_party_id}</eb:PartyId>
                        <eb:Role>http://docs.oasis-open.org/ebxml-msg/ebms/v3.0/ns/core/200704/responder</eb:Role>
                    </eb:To>
                </eb:PartyInfo>
                <eb:CollaborationInfo>
                    <eb:Service>{self.service}</eb:Service>
                    <eb:Action>{self.action}</eb:Action>
                    <eb:ConversationId>{self.conversation_id}</eb:ConversationId>
                </eb:CollaborationInfo>
                <eb:MessageProperties>
                    {properties_xml}
                </eb:MessageProperties>
                <eb:PayloadInfo>
                    <eb:PartInfo href="cid:{payload_cid}">
                        <eb:PartProperties>
                            <eb:Property name="MimeType">{self.content_type}</eb:Property>
                        </eb:PartProperties>
                    </eb:PartInfo>
                </eb:PayloadInfo>
            </eb:UserMessage>
        </eb:Messaging>
    </soap:Header>
    <soap:Body wsu:Id="Body-{self.message_id}" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
        <eb:PayloadContainer>
            <eb:Payload contentId="{payload_cid}" mimeType="{self.content_type}">
                {payload_b64}
            </eb:Payload>
        </eb:PayloadContainer>
    </soap:Body>
</soap:Envelope>"""
        
        return soap_envelope
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary representation."""
        return {
            "message_id": self.message_id,
            "conversation_id": self.conversation_id,
            "from_party": {
                "id": self.from_party_id,
                "type": self.from_party_type
            },
            "to_party": {
                "id": self.to_party_id,
                "type": self.to_party_type
            },
            "service": self.service,
            "action": self.action,
            "content_type": self.content_type,
            "timestamp": self.timestamp.isoformat(),
            "payload_size": len(self.payload),
            "message_properties": self.message_properties
        }


class AS4Receipt:
    """
    AS4 Receipt message for reliable messaging.
    
    Implements ebMS 3.0 receipt functionality for message acknowledgment
    and non-repudiation.
    """
    
    def __init__(self, original_message_id: str, receipt_id: str = None):
        """
        Initialize AS4 receipt.
        
        Args:
            original_message_id: ID of the original message
            receipt_id: Receipt message ID (generated if not provided)
        """
        self.receipt_id = receipt_id or str(uuid.uuid4())
        self.original_message_id = original_message_id
        self.timestamp = datetime.now(timezone.utc)
    
    def create_receipt_soap(self, security_header: str = "") -> str:
        """
        Create SOAP receipt message.
        
        Args:
            security_header: WS-Security header
            
        Returns:
            SOAP receipt message
        """
        receipt_soap = f"""<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope"
               xmlns:eb="http://docs.oasis-open.org/ebxml-msg/ebms/v3.0/ns/core/200704/">
    <soap:Header>
        {security_header}
        <eb:Messaging soap:mustUnderstand="true">
            <eb:SignalMessage>
                <eb:MessageInfo>
                    <eb:Timestamp>{self.timestamp.isoformat()}</eb:Timestamp>
                    <eb:MessageId>{self.receipt_id}</eb:MessageId>
                    <eb:RefToMessageId>{self.original_message_id}</eb:RefToMessageId>
                </eb:MessageInfo>
                <eb:Receipt>
                    <eb:UserMessage>
                        <eb:MessageInfo>
                            <eb:MessageId>{self.original_message_id}</eb:MessageId>
                        </eb:MessageInfo>
                    </eb:UserMessage>
                </eb:Receipt>
            </eb:SignalMessage>
        </eb:Messaging>
    </soap:Header>
    <soap:Body/>
</soap:Envelope>"""
        
        return receipt_soap


class AS4Server:
    """
    AS4 Server for receiving EDI messages from SAP IS-U.
    
    Implements OASIS ebMS 3.0 server functionality including:
    - SOAP 1.2 message processing
    - WS-Security validation
    - Reliable messaging with receipts
    - Error handling and reporting
    """
    
    def __init__(
        self,
        security: AS4Security,
        listen_port: int = 8443,
        endpoint_url: str = "/as4"
    ):
        """
        Initialize AS4 server.
        
        Args:
            security: AS4 security handler
            listen_port: HTTPS port to listen on
            endpoint_url: AS4 endpoint URL path
        """
        self.security = security
        self.listen_port = listen_port
        self.endpoint_url = endpoint_url
        self.running = False
        self.received_messages: List[AS4Message] = []
        self.sent_receipts: List[AS4Receipt] = []
    
    async def start(self) -> bool:
        """
        Start AS4 server.
        
        Returns:
            True if started successfully
        """
        try:
            if not self.security.load_certificates():
                raise AS4Error("Failed to load AS4 certificates")
            
            # In real implementation, start HTTPS server with SOAP endpoint
            self.running = True
            logger.info(f"AS4 server started on port {self.listen_port}")
            logger.info(f"AS4 endpoint: https://localhost:{self.listen_port}{self.endpoint_url}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start AS4 server: {e}")
            return False
    
    async def stop(self) -> None:
        """Stop AS4 server."""
        self.running = False
        logger.info("AS4 server stopped")
    
    async def process_soap_message(self, soap_content: str, headers: Dict[str, str]) -> str:
        """
        Process incoming SOAP message.
        
        Args:
            soap_content: SOAP message content
            headers: HTTP headers
            
        Returns:
            SOAP response (receipt or error)
        """
        try:
            # Parse SOAP envelope
            root = ET.fromstring(soap_content)
            
            # Extract ebMS headers
            message_info = self._extract_message_info(root)
            party_info = self._extract_party_info(root)
            collaboration_info = self._extract_collaboration_info(root)
            payload_info = self._extract_payload_info(root)
            
            # Verify security
            if not self.security.verify_signature(soap_content):
                return self._create_error_response("SecurityFailure", "Invalid signature")
            
            # Create AS4 message
            message = AS4Message(
                message_id=message_info["message_id"],
                conversation_id=message_info["conversation_id"],
                from_party_id=party_info["from"]["party_id"],
                from_party_type=party_info["from"]["party_type"],
                to_party_id=party_info["to"]["party_id"],
                to_party_type=party_info["to"]["party_type"],
                service=collaboration_info["service"],
                action=collaboration_info["action"],
                payload=payload_info["payload"],
                content_type=payload_info["content_type"]
            )
            
            # Store received message
            self.received_messages.append(message)
            
            # Generate receipt
            receipt = AS4Receipt(message.message_id)
            self.sent_receipts.append(receipt)
            
            # Create security header for receipt
            security_header = self.security.create_security_header(receipt.receipt_id)
            
            logger.info(f"Processed AS4 message {message.message_id} from {message.from_party_id}")
            
            return receipt.create_receipt_soap(security_header)
            
        except Exception as e:
            logger.error(f"Failed to process AS4 message: {e}")
            return self._create_error_response("ProcessingError", str(e))
    
    def _extract_message_info(self, root: ET.Element) -> Dict[str, str]:
        """Extract message info from SOAP envelope."""
        ns = {"eb": "http://docs.oasis-open.org/ebxml-msg/ebms/v3.0/ns/core/200704/"}
        
        message_info = root.find(".//eb:MessageInfo", ns)
        if message_info is None:
            raise AS4Error("Missing MessageInfo")
        
        return {
            "message_id": message_info.find("eb:MessageId", ns).text,
            "conversation_id": message_info.find("eb:ConversationId", ns).text,
            "timestamp": message_info.find("eb:Timestamp", ns).text
        }
    
    def _extract_party_info(self, root: ET.Element) -> Dict[str, Dict[str, str]]:
        """Extract party info from SOAP envelope."""
        ns = {"eb": "http://docs.oasis-open.org/ebxml-msg/ebms/v3.0/ns/core/200704/"}
        
        party_info = root.find(".//eb:PartyInfo", ns)
        if party_info is None:
            raise AS4Error("Missing PartyInfo")
        
        from_party = party_info.find("eb:From/eb:PartyId", ns)
        to_party = party_info.find("eb:To/eb:PartyId", ns)
        
        return {
            "from": {
                "party_id": from_party.text,
                "party_type": from_party.get("type", "")
            },
            "to": {
                "party_id": to_party.text,
                "party_type": to_party.get("type", "")
            }
        }
    
    def _extract_collaboration_info(self, root: ET.Element) -> Dict[str, str]:
        """Extract collaboration info from SOAP envelope."""
        ns = {"eb": "http://docs.oasis-open.org/ebxml-msg/ebms/v3.0/ns/core/200704/"}
        
        collab_info = root.find(".//eb:CollaborationInfo", ns)
        if collab_info is None:
            raise AS4Error("Missing CollaborationInfo")
        
        return {
            "service": collab_info.find("eb:Service", ns).text,
            "action": collab_info.find("eb:Action", ns).text
        }
    
    def _extract_payload_info(self, root: ET.Element) -> Dict[str, Any]:
        """Extract payload from SOAP envelope."""
        ns = {"eb": "http://docs.oasis-open.org/ebxml-msg/ebms/v3.0/ns/core/200704/"}
        
        payload_elem = root.find(".//eb:Payload", ns)
        if payload_elem is None:
            raise AS4Error("Missing Payload")
        
        # Decode base64 payload
        payload_b64 = payload_elem.text or ""
        payload = base64.b64decode(payload_b64)
        
        return {
            "payload": payload,
            "content_type": payload_elem.get("mimeType", "application/xml"),
            "content_id": payload_elem.get("contentId", "")
        }
    
    def _create_error_response(self, error_code: str, description: str) -> str:
        """Create SOAP error response."""
        error_id = str(uuid.uuid4())
        timestamp = datetime.now(timezone.utc).isoformat()
        
        error_soap = f"""<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope"
               xmlns:eb="http://docs.oasis-open.org/ebxml-msg/ebms/v3.0/ns/core/200704/">
    <soap:Header>
        <eb:Messaging soap:mustUnderstand="true">
            <eb:SignalMessage>
                <eb:MessageInfo>
                    <eb:Timestamp>{timestamp}</eb:Timestamp>
                    <eb:MessageId>{error_id}</eb:MessageId>
                </eb:MessageInfo>
                <eb:Error errorCode="{error_code}" severity="failure">
                    <eb:Description xml:lang="en">{description}</eb:Description>
                </eb:Error>
            </eb:SignalMessage>
        </eb:Messaging>
    </soap:Header>
    <soap:Body/>
</soap:Envelope>"""
        
        return error_soap
    
    def get_received_messages(self) -> List[Dict[str, Any]]:
        """Get list of received messages."""
        return [msg.to_dict() for msg in self.received_messages]


class AS4Client:
    """
    AS4 Client for sending EDI messages to SAP IS-U.
    
    Implements OASIS ebMS 3.0 client functionality including:
    - SOAP 1.2 message creation
    - WS-Security implementation
    - Reliable messaging with receipt processing
    - Error handling and retry logic
    """
    
    def __init__(
        self,
        security: AS4Security,
        from_party_id: str = "COMAKO",
        from_party_type: str = "urn:oasis:names:tc:ebcore:partyid-type:unregistered"
    ):
        """
        Initialize AS4 client.
        
        Args:
            security: AS4 security handler
            from_party_id: Sender party identifier
            from_party_type: Sender party type
        """
        self.security = security
        self.from_party_id = from_party_id
        self.from_party_type = from_party_type
        self.sent_messages: List[AS4Message] = []
        self.received_receipts: List[AS4Receipt] = []
    
    async def send_message(
        self,
        to_party_id: str,
        to_party_type: str,
        endpoint_url: str,
        service: str,
        action: str,
        payload: bytes,
        content_type: str = "application/xml",
        conversation_id: str = None,
        message_properties: Dict[str, str] = None
    ) -> Dict[str, Any]:
        """
        Send AS4 message to partner.
        
        Args:
            to_party_id: Recipient party identifier
            to_party_type: Recipient party type
            endpoint_url: Partner's AS4 endpoint URL
            service: Service identifier
            action: Action to be performed
            payload: Message payload
            content_type: MIME content type
            conversation_id: Conversation identifier
            message_properties: Additional message properties
            
        Returns:
            Send result with receipt information
        """
        try:
            # Load certificates if not already loaded
            if not self.security.load_certificates():
                raise AS4Error("Failed to load AS4 certificates")
            
            # Generate message ID and conversation ID
            message_id = f"COMAKO-AS4-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
            if not conversation_id:
                conversation_id = str(uuid.uuid4())
            
            # Create AS4 message
            message = AS4Message(
                message_id=message_id,
                conversation_id=conversation_id,
                from_party_id=self.from_party_id,
                from_party_type=self.from_party_type,
                to_party_id=to_party_id,
                to_party_type=to_party_type,
                service=service,
                action=action,
                payload=payload,
                content_type=content_type
            )
            
            # Add message properties
            if message_properties:
                for name, value in message_properties.items():
                    message.add_message_property(name, value)
            
            # Add standard properties
            message.add_message_property("originalSender", self.from_party_id)
            message.add_message_property("finalRecipient", to_party_id)
            
            # Create security header
            security_header = self.security.create_security_header(message_id)
            
            # Create SOAP envelope
            soap_envelope = message.create_soap_envelope(security_header)
            
            # Send message (simulated for demo)
            send_result = await self._send_soap_message(soap_envelope, endpoint_url)
            
            # Store sent message
            self.sent_messages.append(message)
            
            logger.info(f"Sent AS4 message {message_id} to {to_party_id}")
            
            return {
                "status": "success",
                "message_id": message_id,
                "conversation_id": conversation_id,
                "to_party_id": to_party_id,
                "endpoint_url": endpoint_url,
                "payload_size": len(payload),
                "send_result": send_result,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to send AS4 message: {e}")
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
    
    async def _send_soap_message(self, soap_envelope: str, endpoint_url: str) -> Dict[str, Any]:
        """Send SOAP message to endpoint (simulated for demo)."""
        # In real implementation, use HTTP client with SOAP/WS-Security
        await asyncio.sleep(0.1)  # Simulate network delay
        
        return {
            "http_status": 200,
            "response_headers": {
                "Content-Type": "application/soap+xml",
                "Server": "SAP IS-U AS4 Gateway/1.0"
            },
            "receipt_received": True,
            "receipt_id": f"RECEIPT-{uuid.uuid4().hex[:8]}",
            "processing_mode": "sync"
        }
    
    def get_sent_messages(self) -> List[Dict[str, Any]]:
        """Get list of sent messages."""
        return [msg.to_dict() for msg in self.sent_messages]


class AS4Manager:
    """
    AS4 Manager for complete AS4 integration with SAP IS-U.
    
    Provides high-level interface for AS4 operations including:
    - Server and client management
    - Partner configuration and management
    - Message routing and processing
    - Reliable messaging coordination
    - Error handling and monitoring
    """
    
    def __init__(
        self,
        certificate_path: str = "/tmp/as4/cert.pem",
        private_key_path: str = "/tmp/as4/key.pem",
        server_port: int = 8443
    ):
        """
        Initialize AS4 manager.
        
        Args:
            certificate_path: Path to AS4 certificate
            private_key_path: Path to private key
            server_port: AS4 server port (HTTPS)
        """
        self.certificate_path = certificate_path
        self.private_key_path = private_key_path
        self.server_port = server_port
        
        # Create certificate directories
        os.makedirs(os.path.dirname(certificate_path), exist_ok=True)
        
        # Initialize security
        self.security = AS4Security(certificate_path, private_key_path)
        
        # Initialize server and client
        self.server = AS4Server(self.security, server_port)
        self.client = AS4Client(self.security)
        
        # Partner configuration
        self.partners: Dict[str, Dict[str, Any]] = {}
        
        # Message tracking
        self.message_status: Dict[str, str] = {}
    
    def add_partner(
        self,
        party_id: str,
        party_type: str,
        name: str,
        endpoint_url: str,
        service: str,
        certificate_path: Optional[str] = None
    ) -> None:
        """
        Add AS4 trading partner.
        
        Args:
            party_id: Partner party identifier
            party_type: Partner party type
            name: Partner name
            endpoint_url: Partner AS4 endpoint URL
            service: Default service for this partner
            certificate_path: Path to partner's certificate
        """
        self.partners[party_id] = {
            "party_type": party_type,
            "name": name,
            "endpoint_url": endpoint_url,
            "service": service,
            "certificate_path": certificate_path,
            "added_at": datetime.now(timezone.utc).isoformat()
        }
        logger.info(f"Added AS4 partner: {party_id} ({name})")
    
    async def start_server(self) -> bool:
        """Start AS4 server."""
        return await self.server.start()
    
    async def stop_server(self) -> None:
        """Stop AS4 server."""
        await self.server.stop()
    
    async def send_edi_message(
        self,
        party_id: str,
        edi_content: str,
        message_type: str = "UTILMD"
    ) -> Dict[str, Any]:
        """
        Send EDI message to partner via AS4.
        
        Args:
            party_id: Partner party identifier
            edi_content: EDI message content
            message_type: EDI message type
            
        Returns:
            Send result
        """
        if party_id not in self.partners:
            return {
                "status": "error",
                "error": f"Unknown partner: {party_id}"
            }
        
        partner = self.partners[party_id]
        payload = edi_content.encode('utf-8')
        
        return await self.client.send_message(
            to_party_id=party_id,
            to_party_type=partner["party_type"],
            endpoint_url=partner["endpoint_url"],
            service=partner["service"],
            action=f"Process{message_type}",
            payload=payload,
            content_type="application/xml",
            message_properties={
                "messageType": message_type,
                "originalSender": self.client.from_party_id,
                "finalRecipient": party_id
            }
        )
    
    def get_status(self) -> Dict[str, Any]:
        """Get AS4 manager status."""
        return {
            "server_running": self.server.running,
            "server_port": self.server_port,
            "certificates_loaded": self.security._cert_data is not None,
            "partners_count": len(self.partners),
            "partners": list(self.partners.keys()),
            "messages_received": len(self.server.received_messages),
            "messages_sent": len(self.client.sent_messages),
            "receipts_sent": len(self.server.sent_receipts),
            "receipts_received": len(self.client.received_receipts)
        }


# Configuration and utility functions
def get_as4_config() -> Dict[str, Any]:
    """Get AS4 configuration from environment variables."""
    return {
        "certificate_path": os.getenv("AS4_CERT_PATH", "/tmp/as4/cert.pem"),
        "private_key_path": os.getenv("AS4_KEY_PATH", "/tmp/as4/key.pem"),
        "server_port": int(os.getenv("AS4_SERVER_PORT", "8443")),
        "from_party_id": os.getenv("AS4_FROM_PARTY_ID", "COMAKO"),
        "from_party_type": os.getenv("AS4_FROM_PARTY_TYPE", "urn:oasis:names:tc:ebcore:partyid-type:unregistered"),
        "endpoint_url": os.getenv("AS4_ENDPOINT_URL", "/as4")
    }


async def setup_as4_integration() -> AS4Manager:
    """Set up AS4 integration with default configuration."""
    config = get_as4_config()
    
    manager = AS4Manager(
        certificate_path=config["certificate_path"],
        private_key_path=config["private_key_path"],
        server_port=config["server_port"]
    )
    
    # Add default SAP IS-U partner
    manager.add_partner(
        party_id="SAPISU",
        party_type="urn:oasis:names:tc:ebcore:partyid-type:unregistered",
        name="SAP IS-U System",
        endpoint_url="https://sapisu.example.com/as4",
        service="urn:comako:services:edi",
        certificate_path="/tmp/as4/sapisu_cert.pem"
    )
    
    return manager


# Example usage and testing
async def demo_as4_operations():
    """Demonstrate AS4 operations for EDI exchange."""
    
    print("=== AS4 Integration Demo ===")
    
    # Initialize AS4 manager
    print("1. Initializing AS4 manager...")
    manager = await setup_as4_integration()
    print(f"   ✅ AS4 manager initialized")
    print(f"   Partners: {list(manager.partners.keys())}")
    
    # Start AS4 server
    print("\n2. Starting AS4 server...")
    server_started = await manager.start_server()
    print(f"   Server: {'✅ STARTED' if server_started else '❌ FAILED'}")
    
    if server_started:
        status = manager.get_status()
        print(f"   Port: {status['server_port']}")
        print(f"   Certificates: {'✅ LOADED' if status['certificates_loaded'] else '❌ NOT LOADED'}")
    
    # Test sending EDI message
    print("\n3. Testing EDI message sending...")
    sample_edi = """<?xml version="1.0" encoding="UTF-8"?>
<UTILMD xmlns="urn:edi-energy.de:schema:utilmd:v5.1">
    <UNB>
        <S001>
            <E0001>UNOC</E0001>
            <E0002>3</E0002>
        </S001>
        <S002>
            <E0004>COMAKO</E0004>
        </S002>
        <S003>
            <E0010>SAPISU</E0010>
        </S003>
        <S004>
            <E0017>250103</E0017>
            <E0019>1200</E0019>
        </S004>
        <E0020>REF001</E0020>
    </UNB>
    <UNH>
        <E0062>MSG001</E0062>
        <S009>
            <E0065>UTILMD</E0065>
            <E0052>D</E0052>
            <E0054>03B</E0054>
            <E0051>UN</E0051>
            <E0057>EEG</E0057>
        </S009>
    </UNH>
    <BGM>
        <C002>
            <E1001>E01</E1001>
        </C002>
        <E1004>DOC123</E1004>
        <E1225>9</E1225>
    </BGM>
    <DTM>
        <C507>
            <E2005>137</E2005>
            <E2380>20250103</E2380>
            <E2379>102</E2379>
        </C507>
    </DTM>
    <NAD>
        <E3035>MS</E3035>
        <C082>
            <E3039>COMAKO</E3039>
        </C082>
        <C080>
            <E3036>CoMaKo Energy Cooperative</E3036>
        </C080>
    </NAD>
    <LOC>
        <E3227>172</E3227>
        <C517>
            <E3225>MP001</E3225>
        </C517>
        <C519>
            <E3223>Test Metering Point</E3223>
        </C519>
    </LOC>
    <QTY>
        <C186>
            <E6063>220</E6063>
            <E6060>1500.5</E6060>
            <E6411>KWH</E6411>
        </C186>
    </QTY>
    <UNT>
        <E0074>7</E0074>
        <E0062>MSG001</E0062>
    </UNT>
    <UNZ>
        <E0036>1</E0036>
        <E0020>REF001</E0020>
    </UNZ>
</UTILMD>"""
    
    send_result = await manager.send_edi_message(
        party_id="SAPISU",
        edi_content=sample_edi,
        message_type="UTILMD"
    )
    
    print(f"   Send: {'✅ SUCCESS' if send_result['status'] == 'success' else '❌ FAILED'}")
    if send_result['status'] == 'success':
        print(f"   Message ID: {send_result['message_id']}")
        print(f"   Conversation ID: {send_result['conversation_id']}")
        print(f"   Payload size: {send_result['payload_size']} bytes")
    else:
        print(f"   Error: {send_result.get('error', 'Unknown error')}")
    
    # Test receiving message (simulation)
    print("\n4. Testing message reception...")
    test_soap_message = f"""<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope"
               xmlns:eb="http://docs.oasis-open.org/ebxml-msg/ebms/v3.0/ns/core/200704/">
    <soap:Header>
        <eb:Messaging soap:mustUnderstand="true">
            <eb:UserMessage>
                <eb:MessageInfo>
                    <eb:Timestamp>{datetime.now(timezone.utc).isoformat()}</eb:Timestamp>
                    <eb:MessageId>SAPISU-MSG-001</eb:MessageId>
                    <eb:ConversationId>{uuid.uuid4()}</eb:ConversationId>
                </eb:MessageInfo>
                <eb:PartyInfo>
                    <eb:From>
                        <eb:PartyId type="urn:oasis:names:tc:ebcore:partyid-type:unregistered">SAPISU</eb:PartyId>
                        <eb:Role>http://docs.oasis-open.org/ebxml-msg/ebms/v3.0/ns/core/200704/initiator</eb:Role>
                    </eb:From>
                    <eb:To>
                        <eb:PartyId type="urn:oasis:names:tc:ebcore:partyid-type:unregistered">COMAKO</eb:PartyId>
                        <eb:Role>http://docs.oasis-open.org/ebxml-msg/ebms/v3.0/ns/core/200704/responder</eb:Role>
                    </eb:To>
                </eb:PartyInfo>
                <eb:CollaborationInfo>
                    <eb:Service>urn:comako:services:edi</eb:Service>
                    <eb:Action>ProcessAPERAK</eb:Action>
                    <eb:ConversationId>{uuid.uuid4()}</eb:ConversationId>
                </eb:CollaborationInfo>
                <eb:PayloadInfo>
                    <eb:PartInfo href="cid:payload-001@sapisu.example.com">
                        <eb:PartProperties>
                            <eb:Property name="MimeType">application/xml</eb:Property>
                        </eb:PartProperties>
                    </eb:PartInfo>
                </eb:PayloadInfo>
            </eb:UserMessage>
        </eb:Messaging>
    </soap:Header>
    <soap:Body>
        <eb:PayloadContainer>
            <eb:Payload contentId="payload-001@sapisu.example.com" mimeType="application/xml">
                {base64.b64encode(b'<APERAK>Test APERAK Response</APERAK>').decode('utf-8')}
            </eb:Payload>
        </eb:PayloadContainer>
    </soap:Body>
</soap:Envelope>"""
    
    test_headers = {
        "Content-Type": "application/soap+xml",
        "SOAPAction": "ProcessAPERAK"
    }
    
    receive_result = await manager.server.process_soap_message(test_soap_message, test_headers)
    print(f"   Receive: {'✅ SUCCESS' if 'eb:Receipt' in receive_result else '❌ FAILED'}")
    print(f"   Receipt generated: {'✅ YES' if 'eb:Receipt' in receive_result else '❌ NO'}")
    
    # Show final status
    print("\n5. Final status...")
    final_status = manager.get_status()
    print(f"   Messages sent: {final_status['messages_sent']}")
    print(f"   Messages received: {final_status['messages_received']}")
    print(f"   Receipts sent: {final_status['receipts_sent']}")
    print(f"   Partners configured: {final_status['partners_count']}")
    
    # Stop server
    await manager.stop_server()
    print("\n=== AS4 Demo Complete ===")


if __name__ == "__main__":
    # Run demo
    asyncio.run(demo_as4_operations())
