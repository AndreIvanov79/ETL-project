from typing import Dict, Any, List, Union, Optional
from datetime import datetime
import re

class ValidationRule:

    def validate(self, value: Any) -> bool:
        raise NotImplementedError("Subclasses must implement validate")
    
    def get_error_message(self, field_name: str) -> str:
        raise NotImplementedError("Subclasses must implement get_error_message")

class RequiredRule(ValidationRule):
    
    def validate(self, value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, str) and not value.strip():
            return False
        return True
    
    def get_error_message(self, field_name: str) -> str:
        return f"Field '{field_name}' is required but was empty or missing"

class DateFormatRule(ValidationRule):
    
    def __init__(self, format_str: str = "%Y-%m-%d"):
        self.format_str = format_str
    
    def validate(self, value: Any) -> bool:
        if not value:
            return True  
        
        if isinstance(value, datetime):
            return True
            
        try:
            if isinstance(value, str):
                datetime.strptime(value, self.format_str)
                return True
        except ValueError:
            return False
        
        return False
    
    def get_error_message(self, field_name: str) -> str:
        return f"Field '{field_name}' must be a valid date in format {self.format_str}"

class NumericRangeRule(ValidationRule):
    
    def __init__(self, min_val: Optional[float] = None, max_val: Optional[float] = None):
        self.min_val = min_val
        self.max_val = max_val
    
    def validate(self, value: Any) -> bool:
        if value is None:
            return True  
        
        try:
            num_value = float(value)
            
            if self.min_val is not None and num_value < self.min_val:
                return False
                
            if self.max_val is not None and num_value > self.max_val:
                return False
                
            return True
        except (ValueError, TypeError):
            return False
    
    def get_error_message(self, field_name: str) -> str:
        if self.min_val is not None and self.max_val is not None:
            return f"Field '{field_name}' must be between {self.min_val} and {self.max_val}"
        elif self.min_val is not None:
            return f"Field '{field_name}' must be greater than or equal to {self.min_val}"
        elif self.max_val is not None:
            return f"Field '{field_name}' must be less than or equal to {self.max_val}"
        return f"Field '{field_name}' must be a valid number"

class StringLengthRule(ValidationRule):
    
    def __init__(self, min_length: int = 0, max_length: Optional[int] = None):
        self.min_length = min_length
        self.max_length = max_length
    
    def validate(self, value: Any) -> bool:
        if value is None:
            return True  
        
        if not isinstance(value, str):
            return False
        
        length = len(value)
        
        if length < self.min_length:
            return False
            
        if self.max_length is not None and length > self.max_length:
            return False
            
        return True
    
    def get_error_message(self, field_name: str) -> str:
        if self.max_length is not None:
            return f"Field '{field_name}' must be between {self.min_length} and {self.max_length} characters"
        return f"Field '{field_name}' must be at least {self.min_length} characters"

class RegexRule(ValidationRule):
    
    def __init__(self, pattern: str, description: str = "valid format"):
        self.pattern = pattern
        self.regex = re.compile(pattern)
        self.description = description
    
    def validate(self, value: Any) -> bool:
        if value is None:
            return True  
        
        if not isinstance(value, str):
            return False
        
        return bool(self.regex.match(value))
    
    def get_error_message(self, field_name: str) -> str:
        return f"Field '{field_name}' must match {self.description}"

class CustomRule(ValidationRule):
    
    def __init__(self, validation_func, error_message: str):
        self.validation_func = validation_func
        self.error_message = error_message
    
    def validate(self, value: Any) -> bool:
        return self.validation_func(value)
    
    def get_error_message(self, field_name: str) -> str:
        return self.error_message.format(field_name=field_name)

class SchemaValidator:
    
    def __init__(self):
        self.schemas = {}
    
    def add_schema(self, schema_name: str, rules: Dict[str, List[ValidationRule]]):
        self.schemas[schema_name] = rules
    
    def validate(self, schema_name: str, data: Dict[str, Any]) -> List[str]:
        if schema_name not in self.schemas:
            raise ValueError(f"Unknown schema: {schema_name}")
        
        errors = []
        schema_rules = self.schemas[schema_name]
        
        for field_name, rules in schema_rules.items():
            field_value = data.get(field_name)
            
            for rule in rules:
                if not rule.validate(field_value):
                    errors.append(rule.get_error_message(field_name))
        
        return errors
    
    def is_valid(self, schema_name: str, data: Dict[str, Any]) -> bool:
        return len(self.validate(schema_name, data)) == 0


class DataCleaner:
    
    @staticmethod
    def clean_string(value: str) -> str:
        if value is None:
            return None
        return " ".join(str(value).strip().split())
    
    @staticmethod
    def normalize_date(date_value: Union[str, datetime], output_format: str = "%Y-%m-%d") -> str:
        if date_value is None:
            return None
            
        if isinstance(date_value, datetime):
            return date_value.strftime(output_format)
            
        formats = [
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%m/%d/%Y",
            "%Y/%m/%d",
            "%d-%m-%Y",
            "%m-%d-%Y",
            "%Y-%m-%d %H:%M:%S",
            "%m/%d/%y"  
        ]
        
        for fmt in formats:
            try:
                dt = datetime.strptime(date_value, fmt)
                return dt.strftime(output_format)
            except ValueError:
                continue
                
        raise ValueError(f"Could not parse date: {date_value}")
    
    @staticmethod
    def normalize_number(value: Any) -> Optional[float]:
        
        if value is None:
            return None
            
        if isinstance(value, (int, float)):
            return float(value)
            
        if isinstance(value, str):
            clean_value = value.replace('$', '').replace(',', '').strip()
            try:
                return float(clean_value)
            except ValueError:
                return None
                
        return None
    
    @staticmethod
    def handle_null_values(data: Dict[str, Any], default_values: Dict[str, Any]) -> Dict[str, Any]:
        result = data.copy()
        
        for key, default in default_values.items():
            if key in result and (result[key] is None or result[key] == ""):
                result[key] = default
                
        return result
    