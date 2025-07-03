"""
Data models for extracted file management
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any, List
from enum import Enum
import json


class FileStatus(Enum):
    """Status of extracted files"""
    EXTRACTED = "extracted"  # File has been downloaded to S3
    VALIDATED = "validated"  # File has been schema validated
    PROCESSED = "processed"  # File has been loaded into database
    FAILED = "failed"  # File processing failed
    DELETED = "deleted"  # File has been deleted


class FileType(Enum):
    """Type of extracted file"""
    NYC_ZIP = "nyc_zip"
    NYC_CSV = "nyc_csv"
    LONDON_CSV = "london_csv"


@dataclass
class FileMetadata:
    """Metadata for an extracted file"""
    filename: str
    s3_key: str
    file_type: FileType
    source_url: Optional[str] = None
    file_size_bytes: Optional[int] = None
    extracted_at: Optional[datetime] = None
    validated_at: Optional[datetime] = None
    processed_at: Optional[datetime] = None
    status: FileStatus = FileStatus.EXTRACTED
    validation_errors: List[str] = field(default_factory=list)
    processing_errors: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "filename": self.filename,
            "s3_key": self.s3_key,
            "file_type": self.file_type.value,
            "source_url": self.source_url,
            "file_size_bytes": self.file_size_bytes,
            "extracted_at": self.extracted_at.isoformat() if self.extracted_at else None,
            "validated_at": self.validated_at.isoformat() if self.validated_at else None,
            "processed_at": self.processed_at.isoformat() if self.processed_at else None,
            "status": self.status.value,
            "validation_errors": self.validation_errors,
            "processing_errors": self.processing_errors,
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FileMetadata':
        """Create from dictionary"""
        return cls(
            filename=data["filename"],
            s3_key=data["s3_key"],
            file_type=FileType(data["file_type"]),
            source_url=data.get("source_url"),
            file_size_bytes=data.get("file_size_bytes"),
            extracted_at=datetime.fromisoformat(data["extracted_at"]) if data.get("extracted_at") else None,
            validated_at=datetime.fromisoformat(data["validated_at"]) if data.get("validated_at") else None,
            processed_at=datetime.fromisoformat(data["processed_at"]) if data.get("processed_at") else None,
            status=FileStatus(data["status"]),
            validation_errors=data.get("validation_errors", []),
            processing_errors=data.get("processing_errors", []),
            metadata=data.get("metadata", {})
        )


@dataclass
class FileSummary:
    """Summary statistics for files"""
    total_files: int = 0
    extracted_files: int = 0
    validated_files: int = 0
    processed_files: int = 0
    failed_files: int = 0
    deleted_files: int = 0
    total_size_bytes: int = 0
    by_file_type: Dict[FileType, int] = field(default_factory=dict)
    
    def update_from_file(self, file_meta: FileMetadata):
        """Update summary from a file metadata"""
        self.total_files += 1
        self.total_size_bytes += file_meta.file_size_bytes or 0
        
        # Update status counts
        if file_meta.status == FileStatus.EXTRACTED:
            self.extracted_files += 1
        elif file_meta.status == FileStatus.VALIDATED:
            self.validated_files += 1
        elif file_meta.status == FileStatus.PROCESSED:
            self.processed_files += 1
        elif file_meta.status == FileStatus.FAILED:
            self.failed_files += 1
        elif file_meta.status == FileStatus.DELETED:
            self.deleted_files += 1
        
        # Update file type counts
        self.by_file_type[file_meta.file_type] = self.by_file_type.get(file_meta.file_type, 0) + 1 