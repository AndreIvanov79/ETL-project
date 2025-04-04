import logging
from enum import Enum
from dataclasses import dataclass, field
import uuid
from typing import Optional, Any, Dict
from datetime import datetime

class ErrorSeverity(Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

class ErrorCode(Enum):
    
    FILE_NOT_FOUND = 1001
    FILE_ACCESS_DENIED = 1002
    FILE_CORRUPTED = 1003
    
    INVALID_DATE_FORMAT = 2001
    MISSING_REQUIRED_FIELD = 2002
    INVALID_DATA_TYPE = 2003
    DATA_OUT_OF_RANGE = 2004
    
    DB_CONNECTION_ERROR = 3001
    DB_QUERY_ERROR = 3002
    DB_TRANSACTION_ERROR = 3003
    
    PROCESS_TIMEOUT = 4001
    PROCESS_INTERRUPTED = 4002
    
    UNKNOWN_ERROR = 9999

@dataclass
class ETLError:
    code: ErrorCode
    message: str
    timestamp: datetime
    severity: ErrorSeverity
    component: str
    source_file: Optional[str] = None
    record_id: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    
    def to_dict(self):
        return {
            "error_code": self.code.value,
            "error_type": self.code.name,
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
            "severity": self.severity.value,
            "component": self.component,
            "source_file": self.source_file,
            "record_id": self.record_id,
            "details": self.details
        }
    
    def log(self, logger):
        error_msg = f"[{self.code.name}] {self.message}"
        if self.source_file:
            error_msg += f" (Source: {self.source_file})"
        
        if self.severity == ErrorSeverity.INFO:
            logger.info(error_msg)
        elif self.severity == ErrorSeverity.WARNING:
            logger.warning(error_msg)
        elif self.severity == ErrorSeverity.ERROR:
            logger.error(error_msg)
        elif self.severity == ErrorSeverity.CRITICAL:
            logger.critical(error_msg)

class ErrorManager:
    def __init__(self, logger=None, db_connection=None):
        self.logger = logger or logging.getLogger('etl_error_logger')
        self.db_connection = db_connection
        self.errors = []
    
    def add_error(self, error: ETLError):
        self.errors.append(error)
        error.log(self.logger)
        self._store_error_in_db(error)
        return error
    
    def create_error(self, code: ErrorCode, message: str, severity: ErrorSeverity, 
                     component: str, source_file: str = None, record_id: str = None, 
                     details: Dict[str, Any] = None) -> ETLError:
        error = ETLError(
            code=code,
            message=message,
            timestamp=datetime.now(),
            severity=severity,
            component=component,
            source_file=source_file,
            record_id=record_id,
            details=details
        )
        return self.add_error(error)
    
    def _store_error_in_db(self, error: ETLError):
        if self.db_connection:
            try:
                error_dict = error.to_dict()
                if error_dict["details"]:
                    error_dict["details"] = str(error_dict["details"])
                
                self.db_connection.execute("""
                    INSERT INTO etl_errors (
                        id, error_code, error_type, message, timestamp, severity, 
                        component, source_file, record_id, details
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    error.id,
                    error_dict["error_code"],
                    error_dict["error_type"],
                    error_dict["message"],
                    error_dict["timestamp"],
                    error_dict["severity"],
                    error_dict["component"],
                    error_dict["source_file"],
                    error_dict["record_id"],
                    error_dict["details"]
                ))
            except Exception as e:
                self.logger.error(f"Failed to store error in database: {str(e)}")
    
    def get_errors(self, severity=None, component=None, code=None):
        filtered_errors = self.errors
        
        if severity:
            filtered_errors = [e for e in filtered_errors if e.severity == severity]
        if component:
            filtered_errors = [e for e in filtered_errors if e.component == component]
        if code:
            filtered_errors = [e for e in filtered_errors if e.code == code]
            
        return filtered_errors
    
    def has_critical_errors(self):
        return any(e.severity == ErrorSeverity.CRITICAL for e in self.errors)
    
    def summary(self):
        summary = {
            "total": len(self.errors),
            "by_severity": {
                "info": len([e for e in self.errors if e.severity == ErrorSeverity.INFO]),
                "warning": len([e for e in self.errors if e.severity == ErrorSeverity.WARNING]),
                "error": len([e for e in self.errors if e.severity == ErrorSeverity.ERROR]),
                "critical": len([e for e in self.errors if e.severity == ErrorSeverity.CRITICAL])
            },
            "by_component": {}
        }
        
        components = set(e.component for e in self.errors)
        for component in components:
            summary["by_component"][component] = len([e for e in self.errors if e.component == component])
            
        return summary
    