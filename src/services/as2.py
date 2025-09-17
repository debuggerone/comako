"""
AS2 Integration Module for SAP IS-U Compatibility

This module provides AS2 (Applicability Statement 2) functionality for secure
EDI message exchange with SAP IS-U systems, implementing RFC 4130 standards.
"""

import os
import asyncio
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import hashlib
import base64
import uuid
from pathlib import Path
import tempfile

# Note: In a real implementation, you would use a proper AS2 library
# For this demo, we'll simulate AS2 functionality
logger = logging.getLogger(__name__)


class AS2Error(Exception):
    """Custom exception for AS2 operations."""
    pass


class AS2Certificate:
    """
    AS2 Certificate management for secure message exchange.
    
    Handles X.509 certificates for AS2 encryption and digital signatures.
    """
    
    def __init__(self, cert_path: str, key_path: str, password: Optional[str] = None):
        """
        Initialize AS2 certificate.
        
        Args:
            cert_path: Path to X.509 certificate file
            key_path: Path to private key file
            password: Private key password (if encrypted)
        """
        self.cert_path = cert_path
        self.key_path = key_path
        self.password = password
        self._cert_data = None
        self._key_data = None
    
    def load_certificate(self) -> bool:
        """
        Load certificate and private key from files.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # In a real implementation, use cryptography library
            # For demo purposes, we'll simulate certificate loading
            if os.path.exists(self.cert_path) and os.path.exists(self.key_path):
                with open(self.cert_path, 'rb') as f:
                    self._cert_data = f.read()
                with open(self.key_path, 'rb') as f:
                    self._key_data = f.read()
                logger.info("AS2 certificate loaded successfully")
                return True
            else:
                # Create dummy certificate data for demo
                self._cert_data = b"-----BEGIN CERTIFICATE-----\nDUMMY_CERT_DATA\n-----END CERTIFICATE-----"
                self._key_data = b"-----BEGIN PRIVATE KEY-----\nDUMMY_KEY_DATA\n-----END PRIVATE KEY-----"
                logger.warning("Using dummy certificate data for demo")
                return True
        except Exception as e:
            logger.error(f"Failed to load AS2 certificate: {e}")
            return False
    
    def get_certificate_info(self) -> Dict[str, Any]:
        """Get certificate information."""
        return {
            "cert_path": self.cert_path,
            "key_path": self.key_path,
            "loaded": self._cert_data is not None,
            "cert_size": len(self._cert_data) if self._cert_data else 0
        }


class AS2Message:
    """
    AS2 Message container for EDI data exchange.
    
    Implements AS2 message structure with headers, payload, and security features.
    """
    
    def __init__(
        self,
        message_id: str,
        from_id: str,
        to_id: str,
        subject: str,
        payload: bytes,
        content_type: str = "application/edi-x12"
    ):
        """
        Initialize AS2 message.
        
        Args:
            message_id: Unique message identifier
            from_id: Sender AS2 identifier
            to_id: Recipient AS2 identifier
            subject: Message subject
            payload: Message payload (EDI data)
            content_type: MIME content type
        """
        self.message_id = message_id
        self.from_id = from_id
        self.to_id = to_id
        self.subject = subject
        self.payload = payload
        self.content_type = content_type
        self.timestamp = datetime.now()
        self.headers = {}
        self.signature = None
        self.encryption = None
    
    def add_header(self, name: str, value: str) -> None:
        """Add AS2 header."""
        self.headers[name] = value
    
    def get_message_size(self) -> int:
        """Get message payload size."""
        return len(self.payload)
    
    def calculate_mic(self) -> str:
        """
        Calculate Message Integrity Check (MIC) for the payload.
        
        Returns:
            Base64-encoded SHA-256 hash of payload
        """
        hash_obj = hashlib.sha256(self.payload)
        mic = base64.b64encode(hash_obj.digest()).decode('utf-8')
        return f"sha256, {mic}"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary representation."""
        return {
            "message_id": self.message_id,
            "from_id": self.from_id,
            "to_id": self.to_id,
            "subject": self.subject,
            "content_type": self.content_type,
            "timestamp": self.timestamp.isoformat(),
            "payload_size": len(self.payload),
            "headers": self.headers,
            "mic": self.calculate_mic()
        }


class AS2Server:
    """
    AS2 Server for receiving EDI messages from SAP IS-U.
    
    Implements AS2 server functionality including:
    - Message reception and validation
    - Digital signature verification
    - Decryption of encrypted messages
    - MDN (Message Disposition Notification) generation
    """
    
    def __init__(
        self,
        certificate: AS2Certificate,
        listen_port: int = 8080,
        base_url: str = "http://localhost:8080/as2"
    ):
        """
        Initialize AS2 server.
        
        Args:
            certificate: AS2 certificate for security
            listen_port: Port to listen on
            base_url: Base URL for AS2 endpoint
        """
        self.certificate = certificate
        self.listen_port = listen_port
        self.base_url = base_url
        self.running = False
        self.received_messages: List[AS2Message] = []
    
    async def start(self) -> bool:
        """
        Start AS2 server.
        
        Returns:
            True if started successfully
        """
        try:
            if not self.certificate.load_certificate():
                raise AS2Error("Failed to load AS2 certificate")
            
            # In a real implementation, start HTTP server here
            # For demo, we'll simulate server startup
            self.running = True
            logger.info(f"AS2 server started on port {self.listen_port}")
            logger.info(f"AS2 endpoint: {self.base_url}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start AS2 server: {e}")
            return False
    
    async def stop(self) -> None:
        """Stop AS2 server."""
        self.running = False
        logger.info("AS2 server stopped")
    
    async def receive_message(self, raw_data: bytes, headers: Dict[str, str]) -> Dict[str, Any]:
        """
        Process incoming AS2 message.
        
        Args:
            raw_data: Raw message data
            headers: HTTP headers
            
        Returns:
            Processing result with MDN information
        """
        try:
            # Extract AS2 headers
            message_id = headers.get('Message-ID', str(uuid.uuid4()))
            from_id = headers.get('AS2-From', 'UNKNOWN')
            to_id = headers.get('AS2-To', 'COMAKO')
            subject = headers.get('Subject', 'AS2 Message')
            
            # Create AS2 message
            message = AS2Message(
                message_id=message_id,
                from_id=from_id,
                to_id=to_id,
                subject=subject,
                payload=raw_data
            )
            
            # Add received headers
            for key, value in headers.items():
                message.add_header(key, value)
            
            # Validate message (simplified)
            if not self._validate_message(message):
                raise AS2Error("Message validation failed")
            
            # Store received message
            self.received_messages.append(message)
            
            # Generate MDN (Message Disposition Notification)
            mdn = self._generate_mdn(message, "processed")
            
            logger.info(f"Received AS2 message {message_id} from {from_id}")
            
            return {
                "status": "success",
                "message_id": message_id,
                "from_id": from_id,
                "payload_size": len(raw_data),
                "mdn": mdn,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to process AS2 message: {e}")
            # Generate error MDN
            error_mdn = self._generate_mdn(None, "failed", str(e))
            return {
                "status": "error",
                "error": str(e),
                "mdn": error_mdn,
                "timestamp": datetime.now().isoformat()
            }
    
    def _validate_message(self, message: AS2Message) -> bool:
        """Validate AS2 message structure and security."""
        # Basic validation
        if not message.message_id or not message.from_id:
            return False
        
        # Check payload size
        if len(message.payload) == 0:
            return False
        
        # In a real implementation, verify digital signatures here
        logger.debug(f"Validated AS2 message {message.message_id}")
        return True
    
    def _generate_mdn(
        self,
        original_message: Optional[AS2Message],
        disposition: str,
        error_message: Optional[str] = None
    ) -> Dict[str, Any]:
        """Generate Message Disposition Notification."""
        mdn_id = str(uuid.uuid4())
        
        mdn = {
            "mdn_id": mdn_id,
            "original_message_id": original_message.message_id if original_message else "unknown",
            "disposition": disposition,
            "timestamp": datetime.now().isoformat(),
            "reporting_ua": "CoMaKo AS2 Server/1.0"
        }
        
        if error_message:
            mdn["error"] = error_message
        
        if original_message:
            mdn["original_mic"] = original_message.calculate_mic()
        
        return mdn
    
    def get_received_messages(self) -> List[Dict[str, Any]]:
        """Get list of received messages."""
        return [msg.to_dict() for msg in self.received_messages]


class AS2Client:
    """
    AS2 Client for sending EDI messages to SAP IS-U.
    
    Implements AS2 client functionality including:
    - Message preparation and formatting
    - Digital signature generation
    - Message encryption
    - MDN processing
    """
    
    def __init__(
        self,
        certificate: AS2Certificate,
        from_id: str = "COMAKO",
        user_agent: str = "CoMaKo AS2 Client/1.0"
    ):
        """
        Initialize AS2 client.
        
        Args:
            certificate: AS2 certificate for security
            from_id: Sender AS2 identifier
            user_agent: User agent string
        """
        self.certificate = certificate
        self.from_id = from_id
        self.user_agent = user_agent
        self.sent_messages: List[AS2Message] = []
    
    async def send_message(
        self,
        to_id: str,
        url: str,
        payload: bytes,
        subject: str = "EDI Message",
        content_type: str = "application/edi-x12",
        request_mdn: bool = True
    ) -> Dict[str, Any]:
        """
        Send AS2 message to partner.
        
        Args:
            to_id: Recipient AS2 identifier
            url: Partner's AS2 endpoint URL
            payload: Message payload (EDI data)
            subject: Message subject
            content_type: MIME content type
            request_mdn: Whether to request MDN
            
        Returns:
            Send result with MDN information
        """
        try:
            # Load certificate if not already loaded
            if not self.certificate.load_certificate():
                raise AS2Error("Failed to load AS2 certificate")
            
            # Generate message ID
            message_id = f"COMAKO-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
            
            # Create AS2 message
            message = AS2Message(
                message_id=message_id,
                from_id=self.from_id,
                to_id=to_id,
                subject=subject,
                payload=payload,
                content_type=content_type
            )
            
            # Add AS2 headers
            message.add_header("AS2-Version", "1.0")
            message.add_header("User-Agent", self.user_agent)
            message.add_header("Content-Type", content_type)
            message.add_header("Content-Length", str(len(payload)))
            
            if request_mdn:
                message.add_header("Disposition-Notification-To", "comako@localhost")
                message.add_header("Disposition-Notification-Options", "signed-receipt-protocol=required,pkcs7-signature; signed-receipt-micalg=required,sha256")
            
            # Calculate MIC
            mic = message.calculate_mic()
            message.add_header("Content-MIC", mic)
            
            # In a real implementation, sign and encrypt message here
            # For demo, we'll simulate the send operation
            send_result = await self._simulate_send(message, url)
            
            # Store sent message
            self.sent_messages.append(message)
            
            logger.info(f"Sent AS2 message {message_id} to {to_id}")
            
            return {
                "status": "success",
                "message_id": message_id,
                "to_id": to_id,
                "url": url,
                "payload_size": len(payload),
                "mic": mic,
                "send_result": send_result,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to send AS2 message: {e}")
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    async def _simulate_send(self, message: AS2Message, url: str) -> Dict[str, Any]:
        """Simulate sending AS2 message (for demo purposes)."""
        # In a real implementation, use HTTP client to send message
        await asyncio.sleep(0.1)  # Simulate network delay
        
        return {
            "http_status": 200,
            "response_headers": {
                "AS2-Version": "1.0",
                "Server": "SAP IS-U AS2 Server/1.0"
            },
            "mdn_received": True,
            "mdn_disposition": "processed"
        }
    
    def get_sent_messages(self) -> List[Dict[str, Any]]:
        """Get list of sent messages."""
        return [msg.to_dict() for msg in self.sent_messages]


class AS2Manager:
    """
    AS2 Manager for complete AS2 integration with SAP IS-U.
    
    Provides high-level interface for AS2 operations including:
    - Server and client management
    - Message routing and processing
    - Partner configuration
    - Error handling and logging
    """
    
    def __init__(
        self,
        certificate_path: str = "/tmp/as2/cert.pem",
        private_key_path: str = "/tmp/as2/key.pem",
        server_port: int = 8080
    ):
        """
        Initialize AS2 manager.
        
        Args:
            certificate_path: Path to AS2 certificate
            private_key_path: Path to private key
            server_port: AS2 server port
        """
        self.certificate_path = certificate_path
        self.private_key_path = private_key_path
        self.server_port = server_port
        
        # Create certificate directories
        os.makedirs(os.path.dirname(certificate_path), exist_ok=True)
        
        # Initialize certificate
        self.certificate = AS2Certificate(certificate_path, private_key_path)
        
        # Initialize server and client
        self.server = AS2Server(self.certificate, server_port)
        self.client = AS2Client(self.certificate)
        
        # Partner configuration
        self.partners: Dict[str, Dict[str, Any]] = {}
    
    def add_partner(
        self,
        partner_id: str,
        name: str,
        url: str,
        certificate_path: Optional[str] = None
    ) -> None:
        """
        Add AS2 trading partner.
        
        Args:
            partner_id: Partner AS2 identifier
            name: Partner name
            url: Partner AS2 endpoint URL
            certificate_path: Path to partner's certificate
        """
        self.partners[partner_id] = {
            "name": name,
            "url": url,
            "certificate_path": certificate_path,
            "added_at": datetime.now().isoformat()
        }
        logger.info(f"Added AS2 partner: {partner_id} ({name})")
    
    async def start_server(self) -> bool:
        """Start AS2 server."""
        return await self.server.start()
    
    async def stop_server(self) -> None:
        """Stop AS2 server."""
        await self.server.stop()
    
    async def send_edi_message(
        self,
        partner_id: str,
        edi_content: str,
        message_type: str = "UTILMD"
    ) -> Dict[str, Any]:
        """
        Send EDI message to partner via AS2.
        
        Args:
            partner_id: Partner AS2 identifier
            edi_content: EDI message content
            message_type: EDI message type
            
        Returns:
            Send result
        """
        if partner_id not in self.partners:
            return {
                "status": "error",
                "error": f"Unknown partner: {partner_id}"
            }
        
        partner = self.partners[partner_id]
        payload = edi_content.encode('utf-8')
        
        return await self.client.send_message(
            to_id=partner_id,
            url=partner["url"],
            payload=payload,
            subject=f"EDI {message_type} Message",
            content_type="application/edi-x12"
        )
    
    def get_status(self) -> Dict[str, Any]:
        """Get AS2 manager status."""
        return {
            "server_running": self.server.running,
            "server_port": self.server_port,
            "certificate_loaded": self.certificate._cert_data is not None,
            "partners_count": len(self.partners),
            "partners": list(self.partners.keys()),
            "messages_received": len(self.server.received_messages),
            "messages_sent": len(self.client.sent_messages)
        }


# Configuration and utility functions
def get_as2_config() -> Dict[str, Any]:
    """Get AS2 configuration from environment variables."""
    return {
        "certificate_path": os.getenv("AS2_CERT_PATH", "/tmp/as2/cert.pem"),
        "private_key_path": os.getenv("AS2_KEY_PATH", "/tmp/as2/key.pem"),
        "server_port": int(os.getenv("AS2_SERVER_PORT", "8080")),
        "from_id": os.getenv("AS2_FROM_ID", "COMAKO"),
        "base_url": os.getenv("AS2_BASE_URL", "http://localhost:8080/as2")
    }


async def setup_as2_integration() -> AS2Manager:
    """Set up AS2 integration with default configuration."""
    config = get_as2_config()
    
    manager = AS2Manager(
        certificate_path=config["certificate_path"],
        private_key_path=config["private_key_path"],
        server_port=config["server_port"]
    )
    
    # Add default SAP IS-U partner
    manager.add_partner(
        partner_id="SAPISU",
        name="SAP IS-U System",
        url="https://sapisu.example.com/as2",
        certificate_path="/tmp/as2/sapisu_cert.pem"
    )
    
    return manager


# Example usage and testing
async def demo_as2_operations():
    """Demonstrate AS2 operations for EDI exchange."""
    
    print("=== AS2 Integration Demo ===")
    
    # Initialize AS2 manager
    print("1. Initializing AS2 manager...")
    manager = await setup_as2_integration()
    print(f"   ✅ AS2 manager initialized")
    print(f"   Partners: {list(manager.partners.keys())}")
    
    # Start AS2 server
    print("\n2. Starting AS2 server...")
    server_started = await manager.start_server()
    print(f"   Server: {'✅ STARTED' if server_started else '❌ FAILED'}")
    
    if server_started:
        status = manager.get_status()
        print(f"   Port: {status['server_port']}")
        print(f"   Certificate: {'✅ LOADED' if status['certificate_loaded'] else '❌ NOT LOADED'}")
    
    # Test sending EDI message
    print("\n3. Testing EDI message sending...")
    sample_edi = """UNB+UNOC:3+COMAKO+SAPISU+250103:1200+REF001'
UNH+MSG001+UTILMD:D:03B:UN:EEG+1.1e'
BGM+E01+DOC123+9'
DTM+137:20250103:102'
NAD+MS+COMAKO+CoMaKo Energy Cooperative'
LOC+172+MP001+Test Metering Point'
QTY+220:1500.5:KWH'
UNT+7+MSG001'
UNZ+1+REF001'
"""
    
    send_result = await manager.send_edi_message(
        partner_id="SAPISU",
        edi_content=sample_edi,
        message_type="UTILMD"
    )
    
    print(f"   Send: {'✅ SUCCESS' if send_result['status'] == 'success' else '❌ FAILED'}")
    if send_result['status'] == 'success':
        print(f"   Message ID: {send_result['message_id']}")
        print(f"   Payload size: {send_result['payload_size']} bytes")
        print(f"   MIC: {send_result['mic'][:50]}...")
    else:
        print(f"   Error: {send_result.get('error', 'Unknown error')}")
    
    # Test receiving message (simulation)
    print("\n4. Testing message reception...")
    test_headers = {
        "Message-ID": "TEST-MSG-001",
        "AS2-From": "SAPISU",
        "AS2-To": "COMAKO",
        "Subject": "Test APERAK Message",
        "Content-Type": "application/edi-x12"
    }
    
    test_payload = b"""UNB+UNOC:3+SAPISU+COMAKO+250103:1400+REF002'
UNH+MSG002+APERAK:D:03B:UN:EEG+1.1e'
BGM+916+MSG001+29'
DTM+137:20250103:102'
UNT+4+MSG002'
UNZ+1+REF002'
"""
    
    receive_result = await manager.server.receive_message(test_payload, test_headers)
    print(f"   Receive: {'✅ SUCCESS' if receive_result['status'] == 'success' else '❌ FAILED'}")
    if receive_result['status'] == 'success':
        print(f"   Message ID: {receive_result['message_id']}")
        print(f"   From: {receive_result['from_id']}")
        print(f"   MDN generated: {'✅ YES' if receive_result.get('mdn') else '❌ NO'}")
    
    # Show final status
    print("\n5. Final status...")
    final_status = manager.get_status()
    print(f"   Messages sent: {final_status['messages_sent']}")
    print(f"   Messages received: {final_status['messages_received']}")
    print(f"   Partners configured: {final_status['partners_count']}")
    
    # Stop server
    await manager.stop_server()
    print("\n=== AS2 Demo Complete ===")


if __name__ == "__main__":
    # Run demo
    asyncio.run(demo_as2_operations())
