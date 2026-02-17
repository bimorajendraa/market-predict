"""
Storage module for Finance Analytics.
Provides MinIO/S3 upload functionality via boto3.
"""

import hashlib
import logging
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from .config import config

logger = logging.getLogger(__name__)


def get_s3_client():
    """Create and return a boto3 S3 client configured for MinIO."""
    endpoint = config.MINIO_ENDPOINT
    
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=config.MINIO_ACCESS_KEY,
        aws_secret_access_key=config.MINIO_SECRET_KEY,
        config=BotoConfig(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )


def calculate_checksum(data: bytes) -> str:
    """Calculate SHA256 checksum of data."""
    return hashlib.sha256(data).hexdigest()


def get_file_extension(url: str, content_type: Optional[str] = None) -> str:
    """
    Determine file extension from URL or content type.
    
    Args:
        url: The source URL
        content_type: Optional Content-Type header value
        
    Returns:
        File extension (without dot)
    """
    # Try to get extension from URL
    parsed = urlparse(url)
    path = parsed.path.lower()
    
    if path.endswith(".pdf"):
        return "pdf"
    elif path.endswith(".html") or path.endswith(".htm"):
        return "html"
    elif path.endswith(".xml"):
        return "xml"
    elif path.endswith(".json"):
        return "json"
    
    # Fall back to content type
    if content_type:
        content_type = content_type.lower()
        if "pdf" in content_type:
            return "pdf"
        elif "html" in content_type:
            return "html"
        elif "xml" in content_type:
            return "xml"
        elif "json" in content_type:
            return "json"
    
    # Default to html for web content
    return "html"


def generate_object_key(
    source: str,
    doc_type: str,
    checksum: str,
    extension: str,
    ticker: Optional[str] = None,
    date: Optional[datetime] = None,
) -> str:
    """
    Generate a structured object key for MinIO.
    
    Pattern: {source}/{ticker or 'NA'}/{doc_type}/{yyyy-mm-dd}/{sha256}.{ext}
    
    Args:
        source: Source identifier (e.g., 'reuters', 'sec')
        doc_type: Document type (e.g., 'news', 'annual_report')
        checksum: SHA256 checksum of content
        extension: File extension
        ticker: Stock ticker symbol (optional)
        date: Date for the object (defaults to today)
        
    Returns:
        Object key string
    """
    ticker_part = ticker if ticker else "NA"
    date_part = (date or datetime.utcnow()).strftime("%Y-%m-%d")
    
    return f"{source}/{ticker_part}/{doc_type}/{date_part}/{checksum}.{extension}"


def upload_raw(
    data: bytes,
    source: str,
    doc_type: str,
    ticker: Optional[str] = None,
    content_type: Optional[str] = None,
    url: Optional[str] = None,
) -> tuple[str, str]:
    """
    Upload raw content to MinIO.
    
    Args:
        data: Raw bytes to upload
        source: Source identifier
        doc_type: Document type
        ticker: Optional stock ticker
        content_type: Optional content type
        url: Original URL (used to determine extension)
        
    Returns:
        Tuple of (object_key, checksum)
    """
    client = get_s3_client()
    
    # Calculate checksum
    checksum = calculate_checksum(data)
    
    # Determine extension
    extension = get_file_extension(url or "", content_type)
    
    # Generate object key
    object_key = generate_object_key(
        source=source,
        doc_type=doc_type,
        checksum=checksum,
        extension=extension,
        ticker=ticker,
    )
    
    # Upload to MinIO
    try:
        client.put_object(
            Bucket=config.MINIO_BUCKET,
            Key=object_key,
            Body=data,
            ContentType=content_type or "application/octet-stream",
            Metadata={
                "checksum": checksum,
                "source": source,
                "doc_type": doc_type,
                "ticker": ticker or "NA",
            },
        )
        logger.info(f"Uploaded to MinIO: {object_key}")
        return object_key, checksum
        
    except ClientError as e:
        logger.error(f"Failed to upload to MinIO: {e}")
        raise


def check_object_exists(object_key: str) -> bool:
    """Check if an object already exists in MinIO."""
    client = get_s3_client()
    
    try:
        client.head_object(Bucket=config.MINIO_BUCKET, Key=object_key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def download_raw(object_key: str) -> bytes:
    """Download raw content from MinIO."""
    client = get_s3_client()
    
    try:
        response = client.get_object(Bucket=config.MINIO_BUCKET, Key=object_key)
        return response["Body"].read()
    except ClientError as e:
        logger.error(f"Failed to download from MinIO: {e}")
        raise


def ensure_bucket_exists() -> None:
    """Ensure the raw bucket exists, create if not."""
    client = get_s3_client()
    
    try:
        client.head_bucket(Bucket=config.MINIO_BUCKET)
        logger.info(f"Bucket '{config.MINIO_BUCKET}' exists")
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            client.create_bucket(Bucket=config.MINIO_BUCKET)
            logger.info(f"Created bucket '{config.MINIO_BUCKET}'")
        else:
            raise
