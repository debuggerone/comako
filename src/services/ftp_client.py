"""
FTP Client Service for SAP IS-U Compatibility

This module provides FTP client functionality for file-based EDI exchange
with SAP IS-U systems, supporting both upload and download operations.
"""

import os
import asyncio
import logging
from typing import List, Optional, Dict, Any
from pathlib import Path
from datetime import datetime
import ftplib
from ftplib import FTP
import tempfile

logger = logging.getLogger(__name__)


class FTPClientError(Exception):
    """Custom exception for FTP client operations."""
    pass


class FTPClient:
    """
    FTP client for EDI file exchange with SAP IS-U systems.
    
    Supports secure file transfer operations including:
    - Upload EDI files to SAP IS-U
    - Download EDI files from SAP IS-U
    - Directory management
    - File validation
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 21,
        username: str = "comako",
        password: str = "comako",
        timeout: int = 30
    ):
        """
        Initialize FTP client.
        
        Args:
            host: FTP server hostname
            port: FTP server port
            username: FTP username
            password: FTP password
            timeout: Connection timeout in seconds
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.timeout = timeout
        self._connection: Optional[FTP] = None
    
    async def connect(self) -> None:
        """Establish FTP connection."""
        try:
            self._connection = FTP()
            self._connection.connect(self.host, self.port, self.timeout)
            self._connection.login(self.username, self.password)
            logger.info(f"Connected to FTP server {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"Failed to connect to FTP server: {e}")
            raise FTPClientError(f"FTP connection failed: {e}")
    
    async def disconnect(self) -> None:
        """Close FTP connection."""
        if self._connection:
            try:
                self._connection.quit()
                logger.info("Disconnected from FTP server")
            except Exception as e:
                logger.warning(f"Error during FTP disconnect: {e}")
            finally:
                self._connection = None
    
    async def upload_file(
        self,
        local_path: str,
        remote_path: str,
        create_dirs: bool = True
    ) -> bool:
        """
        Upload a file to the FTP server.
        
        Args:
            local_path: Path to local file
            remote_path: Remote file path
            create_dirs: Whether to create remote directories if they don't exist
            
        Returns:
            True if upload successful, False otherwise
        """
        if not self._connection:
            await self.connect()
        
        try:
            # Create remote directories if needed
            if create_dirs:
                await self._create_remote_dirs(remote_path)
            
            # Upload file
            with open(local_path, 'rb') as file:
                self._connection.storbinary(f'STOR {remote_path}', file)
            
            logger.info(f"Uploaded file {local_path} to {remote_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to upload file {local_path}: {e}")
            raise FTPClientError(f"Upload failed: {e}")
    
    async def download_file(
        self,
        remote_path: str,
        local_path: str,
        create_dirs: bool = True
    ) -> bool:
        """
        Download a file from the FTP server.
        
        Args:
            remote_path: Remote file path
            local_path: Local file path
            create_dirs: Whether to create local directories if they don't exist
            
        Returns:
            True if download successful, False otherwise
        """
        if not self._connection:
            await self.connect()
        
        try:
            # Create local directories if needed
            if create_dirs:
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            # Download file
            with open(local_path, 'wb') as file:
                self._connection.retrbinary(f'RETR {remote_path}', file.write)
            
            logger.info(f"Downloaded file {remote_path} to {local_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to download file {remote_path}: {e}")
            raise FTPClientError(f"Download failed: {e}")
    
    async def list_files(self, remote_dir: str = "/") -> List[str]:
        """
        List files in a remote directory.
        
        Args:
            remote_dir: Remote directory path
            
        Returns:
            List of file names
        """
        if not self._connection:
            await self.connect()
        
        try:
            files = []
            self._connection.retrlines(f'NLST {remote_dir}', files.append)
            logger.info(f"Listed {len(files)} files in {remote_dir}")
            return files
            
        except Exception as e:
            logger.error(f"Failed to list files in {remote_dir}: {e}")
            raise FTPClientError(f"List files failed: {e}")
    
    async def file_exists(self, remote_path: str) -> bool:
        """
        Check if a file exists on the remote server.
        
        Args:
            remote_path: Remote file path
            
        Returns:
            True if file exists, False otherwise
        """
        if not self._connection:
            await self.connect()
        
        try:
            # Try to get file size (will raise exception if file doesn't exist)
            self._connection.size(remote_path)
            return True
        except ftplib.error_perm:
            return False
        except Exception as e:
            logger.error(f"Error checking file existence {remote_path}: {e}")
            return False
    
    async def delete_file(self, remote_path: str) -> bool:
        """
        Delete a file from the remote server.
        
        Args:
            remote_path: Remote file path
            
        Returns:
            True if deletion successful, False otherwise
        """
        if not self._connection:
            await self.connect()
        
        try:
            self._connection.delete(remote_path)
            logger.info(f"Deleted file {remote_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete file {remote_path}: {e}")
            raise FTPClientError(f"Delete failed: {e}")
    
    async def _create_remote_dirs(self, remote_path: str) -> None:
        """Create remote directories if they don't exist."""
        remote_dir = os.path.dirname(remote_path)
        if not remote_dir or remote_dir == "/":
            return
        
        # Split path into components
        path_parts = remote_dir.strip("/").split("/")
        current_path = ""
        
        for part in path_parts:
            current_path += f"/{part}"
            try:
                self._connection.mkd(current_path)
                logger.debug(f"Created directory {current_path}")
            except ftplib.error_perm:
                # Directory already exists
                pass
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()


class EDIFileManager:
    """
    Manager for EDI file operations via FTP.
    
    Handles the complete lifecycle of EDI files:
    - Upload outgoing EDI messages
    - Download incoming EDI messages
    - File validation and processing
    - Error handling and retry logic
    """
    
    def __init__(
        self,
        ftp_client: FTPClient,
        local_inbox: str = "/tmp/edi/inbox",
        local_outbox: str = "/tmp/edi/outbox",
        remote_inbox: str = "/edi/inbox",
        remote_outbox: str = "/edi/outbox"
    ):
        """
        Initialize EDI file manager.
        
        Args:
            ftp_client: FTP client instance
            local_inbox: Local directory for incoming files
            local_outbox: Local directory for outgoing files
            remote_inbox: Remote directory for incoming files
            remote_outbox: Remote directory for outgoing files
        """
        self.ftp_client = ftp_client
        self.local_inbox = local_inbox
        self.local_outbox = local_outbox
        self.remote_inbox = remote_inbox
        self.remote_outbox = remote_outbox
        
        # Ensure local directories exist
        os.makedirs(local_inbox, exist_ok=True)
        os.makedirs(local_outbox, exist_ok=True)
    
    async def send_edi_file(
        self,
        edi_content: str,
        filename: str,
        message_type: str = "UTILMD"
    ) -> Dict[str, Any]:
        """
        Send an EDI file to SAP IS-U via FTP.
        
        Args:
            edi_content: EDI message content
            filename: Target filename
            message_type: EDI message type
            
        Returns:
            Dictionary with operation results
        """
        try:
            # Generate timestamped filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_filename = f"{message_type}_{timestamp}_{filename}"
            
            # Write to local outbox
            local_file = os.path.join(self.local_outbox, safe_filename)
            with open(local_file, 'w', encoding='utf-8') as f:
                f.write(edi_content)
            
            # Upload to remote outbox
            remote_file = f"{self.remote_outbox}/{safe_filename}"
            
            async with self.ftp_client:
                success = await self.ftp_client.upload_file(local_file, remote_file)
            
            if success:
                logger.info(f"Successfully sent EDI file {safe_filename}")
                return {
                    "success": True,
                    "filename": safe_filename,
                    "local_path": local_file,
                    "remote_path": remote_file,
                    "message_type": message_type,
                    "timestamp": timestamp
                }
            else:
                raise FTPClientError("Upload failed")
                
        except Exception as e:
            logger.error(f"Failed to send EDI file {filename}: {e}")
            return {
                "success": False,
                "error": str(e),
                "filename": filename,
                "message_type": message_type
            }
    
    async def receive_edi_files(self) -> List[Dict[str, Any]]:
        """
        Receive EDI files from SAP IS-U via FTP.
        
        Returns:
            List of received file information
        """
        received_files = []
        
        try:
            async with self.ftp_client:
                # List files in remote inbox
                remote_files = await self.ftp_client.list_files(self.remote_inbox)
                
                for remote_filename in remote_files:
                    if not remote_filename.endswith('.edi'):
                        continue
                    
                    try:
                        # Download file
                        remote_path = f"{self.remote_inbox}/{remote_filename}"
                        local_path = os.path.join(self.local_inbox, remote_filename)
                        
                        success = await self.ftp_client.download_file(remote_path, local_path)
                        
                        if success:
                            # Read file content
                            with open(local_path, 'r', encoding='utf-8') as f:
                                content = f.read()
                            
                            # Extract message type from filename or content
                            message_type = self._extract_message_type(remote_filename, content)
                            
                            received_files.append({
                                "filename": remote_filename,
                                "local_path": local_path,
                                "remote_path": remote_path,
                                "content": content,
                                "message_type": message_type,
                                "received_at": datetime.now().isoformat()
                            })
                            
                            # Optionally delete from remote after successful download
                            # await self.ftp_client.delete_file(remote_path)
                            
                            logger.info(f"Received EDI file {remote_filename}")
                        
                    except Exception as e:
                        logger.error(f"Failed to process file {remote_filename}: {e}")
                        continue
            
            logger.info(f"Received {len(received_files)} EDI files")
            return received_files
            
        except Exception as e:
            logger.error(f"Failed to receive EDI files: {e}")
            return []
    
    def _extract_message_type(self, filename: str, content: str) -> str:
        """
        Extract EDI message type from filename or content.
        
        Args:
            filename: File name
            content: File content
            
        Returns:
            Message type (UTILMD, MSCONS, etc.)
        """
        # Try to extract from filename
        if "UTILMD" in filename.upper():
            return "UTILMD"
        elif "MSCONS" in filename.upper():
            return "MSCONS"
        elif "APERAK" in filename.upper():
            return "APERAK"
        
        # Try to extract from content (UNH segment)
        lines = content.split('\n')
        for line in lines:
            if line.startswith('UNH+'):
                segments = line.split('+')
                if len(segments) >= 3:
                    message_info = segments[2].split(':')
                    if message_info:
                        return message_info[0]
        
        return "UNKNOWN"


# Configuration and utility functions
def get_ftp_config() -> Dict[str, Any]:
    """Get FTP configuration from environment variables."""
    return {
        "host": os.getenv("FTP_HOST", "localhost"),
        "port": int(os.getenv("FTP_PORT", "21")),
        "username": os.getenv("FTP_USERNAME", "comako"),
        "password": os.getenv("FTP_PASSWORD", "comako"),
        "timeout": int(os.getenv("FTP_TIMEOUT", "30"))
    }


async def test_ftp_connection() -> bool:
    """Test FTP connection with current configuration."""
    try:
        config = get_ftp_config()
        client = FTPClient(**config)
        
        async with client:
            # Try to list root directory
            files = await client.list_files("/")
            logger.info(f"FTP connection test successful. Found {len(files)} files.")
            return True
            
    except Exception as e:
        logger.error(f"FTP connection test failed: {e}")
        return False


# Example usage and testing
async def demo_ftp_operations():
    """Demonstrate FTP operations for EDI file exchange."""
    
    print("=== FTP Client Demo ===")
    
    # Test connection
    print("1. Testing FTP connection...")
    connection_ok = await test_ftp_connection()
    print(f"   Connection: {'✅ SUCCESS' if connection_ok else '❌ FAILED'}")
    
    if not connection_ok:
        print("   Skipping file operations due to connection failure")
        return
    
    # Initialize FTP client and file manager
    config = get_ftp_config()
    ftp_client = FTPClient(**config)
    file_manager = EDIFileManager(ftp_client)
    
    # Test sending EDI file
    print("\n2. Testing EDI file upload...")
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
    
    result = await file_manager.send_edi_file(
        edi_content=sample_edi,
        filename="test_utilmd.edi",
        message_type="UTILMD"
    )
    
    print(f"   Upload: {'✅ SUCCESS' if result['success'] else '❌ FAILED'}")
    if result['success']:
        print(f"   File: {result['filename']}")
        print(f"   Remote path: {result['remote_path']}")
    else:
        print(f"   Error: {result.get('error', 'Unknown error')}")
    
    # Test receiving EDI files
    print("\n3. Testing EDI file download...")
    received_files = await file_manager.receive_edi_files()
    print(f"   Downloaded: {len(received_files)} files")
    
    for file_info in received_files:
        print(f"   - {file_info['filename']} ({file_info['message_type']})")
    
    print("\n=== FTP Demo Complete ===")


if __name__ == "__main__":
    # Run demo
    asyncio.run(demo_ftp_operations())
