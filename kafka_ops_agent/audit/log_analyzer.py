"""Log analysis and filtering capabilities for audit trails."""

import json
import re
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Iterator, Union
from pathlib import Path
from dataclasses import dataclass
from enum import Enum

from kafka_ops_agent.audit.audit_logger import AuditEvent, AuditEventType, AuditLevel


class FilterOperator(str, Enum):
    """Filter operators for log analysis."""
    EQUALS = "eq"
    NOT_EQUALS = "ne"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    GREATER_THAN = "gt"
    LESS_THAN = "lt"
    GREATER_EQUAL = "gte"
    LESS_EQUAL = "lte"
    IN = "in"
    NOT_IN = "not_in"
    REGEX = "regex"


@dataclass
class LogFilter:
    """Represents a log filter condition."""
    field: str
    operator: FilterOperator
    value: Union[str, int, float, List[str]]
    case_sensitive: bool = True


@dataclass
class LogQuery:
    """Represents a log query with filters and options."""
    filters: List[LogFilter]
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    limit: Optional[int] = None
    offset: int = 0
    sort_field: str = "timestamp"
    sort_descending: bool = True


class LogAnalyzer:
    """Analyzes and filters audit logs."""
    
    def __init__(self, log_file_path: Optional[str] = None):
        """Initialize log analyzer.
        
        Args:
            log_file_path: Path to audit log file
        """
        self.log_file_path = log_file_path or "logs/audit.log"
    
    def parse_log_line(self, line: str) -> Optional[Dict[str, Any]]:
        """Parse a log line into a dictionary.
        
        Args:
            line: Log line to parse
            
        Returns:
            Parsed log entry or None if parsing fails
        """
        try:
            # Try to parse as JSON
            return json.loads(line.strip())
        except json.JSONDecodeError:
            # Try to parse structured log format
            return self._parse_structured_line(line)
    
    def _parse_structured_line(self, line: str) -> Optional[Dict[str, Any]]:
        """Parse structured log line (non-JSON format).
        
        Args:
            line: Log line to parse
            
        Returns:
            Parsed log entry or None
        """
        # Pattern for structured logs: timestamp [level] logger: message
        pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[(\w+)\] ([^:]+): (.+)'
        match = re.match(pattern, line.strip())
        
        if match:
            timestamp_str, level, logger, message = match.groups()
            try:
                timestamp = datetime.fromisoformat(timestamp_str)
                return {
                    'timestamp': timestamp.isoformat(),
                    'level': level,
                    'logger': logger,
                    'message': message
                }
            except ValueError:
                pass
        
        return None
    
    def read_logs(self, query: LogQuery) -> Iterator[Dict[str, Any]]:
        """Read and filter logs based on query.
        
        Args:
            query: Log query with filters
            
        Yields:
            Filtered log entries
        """
        log_path = Path(self.log_file_path)
        if not log_path.exists():
            return
        
        entries = []
        
        # Read log file
        try:
            with open(log_path, 'r') as f:
                for line in f:
                    entry = self.parse_log_line(line)
                    if entry and self._matches_query(entry, query):
                        entries.append(entry)
        except Exception as e:
            print(f"Error reading log file: {e}")
            return
        
        # Sort entries
        entries.sort(
            key=lambda x: x.get(query.sort_field, ''),
            reverse=query.sort_descending
        )
        
        # Apply pagination
        start_idx = query.offset
        end_idx = start_idx + query.limit if query.limit else len(entries)
        
        for entry in entries[start_idx:end_idx]:
            yield entry
    
    def _matches_query(self, entry: Dict[str, Any], query: LogQuery) -> bool:
        """Check if log entry matches query filters.
        
        Args:
            entry: Log entry to check
            query: Query with filters
            
        Returns:
            True if entry matches all filters
        """
        # Check time range
        if query.start_time or query.end_time:
            entry_time_str = entry.get('timestamp')
            if entry_time_str:
                try:
                    entry_time = datetime.fromisoformat(entry_time_str.replace('Z', '+00:00'))
                    
                    if query.start_time and entry_time < query.start_time:
                        return False
                    
                    if query.end_time and entry_time > query.end_time:
                        return False
                except ValueError:
                    pass
        
        # Check filters
        for filter_condition in query.filters:
            if not self._matches_filter(entry, filter_condition):
                return False
        
        return True
    
    def _matches_filter(self, entry: Dict[str, Any], filter_condition: LogFilter) -> bool:
        """Check if entry matches a specific filter.
        
        Args:
            entry: Log entry to check
            filter_condition: Filter condition
            
        Returns:
            True if entry matches filter
        """
        field_value = self._get_nested_field(entry, filter_condition.field)
        filter_value = filter_condition.value
        
        if field_value is None:
            return filter_condition.operator == FilterOperator.NOT_EQUALS
        
        # Convert to string for string operations
        field_str = str(field_value)
        if not filter_condition.case_sensitive:
            field_str = field_str.lower()
            if isinstance(filter_value, str):
                filter_value = filter_value.lower()
        
        # Apply operator
        if filter_condition.operator == FilterOperator.EQUALS:
            return field_value == filter_value
        
        elif filter_condition.operator == FilterOperator.NOT_EQUALS:
            return field_value != filter_value
        
        elif filter_condition.operator == FilterOperator.CONTAINS:
            return str(filter_value) in field_str
        
        elif filter_condition.operator == FilterOperator.NOT_CONTAINS:
            return str(filter_value) not in field_str
        
        elif filter_condition.operator == FilterOperator.STARTS_WITH:
            return field_str.startswith(str(filter_value))
        
        elif filter_condition.operator == FilterOperator.ENDS_WITH:
            return field_str.endswith(str(filter_value))
        
        elif filter_condition.operator == FilterOperator.GREATER_THAN:
            try:
                return float(field_value) > float(filter_value)
            except (ValueError, TypeError):
                return False
        
        elif filter_condition.operator == FilterOperator.LESS_THAN:
            try:
                return float(field_value) < float(filter_value)
            except (ValueError, TypeError):
                return False
        
        elif filter_condition.operator == FilterOperator.GREATER_EQUAL:
            try:
                return float(field_value) >= float(filter_value)
            except (ValueError, TypeError):
                return False
        
        elif filter_condition.operator == FilterOperator.LESS_EQUAL:
            try:
                return float(field_value) <= float(filter_value)
            except (ValueError, TypeError):
                return False
        
        elif filter_condition.operator == FilterOperator.IN:
            if isinstance(filter_value, list):
                return field_value in filter_value
            return False
        
        elif filter_condition.operator == FilterOperator.NOT_IN:
            if isinstance(filter_value, list):
                return field_value not in filter_value
            return True
        
        elif filter_condition.operator == FilterOperator.REGEX:
            try:
                pattern = re.compile(str(filter_value), 
                                   re.IGNORECASE if not filter_condition.case_sensitive else 0)
                return bool(pattern.search(field_str))
            except re.error:
                return False
        
        return False
    
    def _get_nested_field(self, entry: Dict[str, Any], field_path: str) -> Any:
        """Get nested field value from entry.
        
        Args:
            entry: Log entry
            field_path: Dot-separated field path (e.g., 'details.cluster_id')
            
        Returns:
            Field value or None if not found
        """
        parts = field_path.split('.')
        value = entry
        
        for part in parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return None
        
        return value
    
    def aggregate_logs(self, query: LogQuery, group_by: str) -> Dict[str, int]:
        """Aggregate logs by a field.
        
        Args:
            query: Log query
            group_by: Field to group by
            
        Returns:
            Dictionary with aggregated counts
        """
        aggregation = {}
        
        for entry in self.read_logs(query):
            group_value = self._get_nested_field(entry, group_by)
            if group_value is not None:
                key = str(group_value)
                aggregation[key] = aggregation.get(key, 0) + 1
        
        return aggregation
    
    def get_log_statistics(self, query: LogQuery) -> Dict[str, Any]:
        """Get statistics for logs matching query.
        
        Args:
            query: Log query
            
        Returns:
            Statistics dictionary
        """
        entries = list(self.read_logs(query))
        
        if not entries:
            return {
                'total_entries': 0,
                'time_range': None,
                'levels': {},
                'event_types': {},
                'users': {},
                'resources': {}
            }
        
        # Calculate statistics
        levels = {}
        event_types = {}
        users = {}
        resources = {}
        timestamps = []
        
        for entry in entries:
            # Level distribution
            level = entry.get('level', 'unknown')
            levels[level] = levels.get(level, 0) + 1
            
            # Event type distribution
            event_type = entry.get('event_type', 'unknown')
            event_types[event_type] = event_types.get(event_type, 0) + 1
            
            # User distribution
            user_id = entry.get('user_id', 'unknown')
            users[user_id] = users.get(user_id, 0) + 1
            
            # Resource distribution
            resource_type = entry.get('resource_type')
            if resource_type:
                resources[resource_type] = resources.get(resource_type, 0) + 1
            
            # Collect timestamps
            timestamp_str = entry.get('timestamp')
            if timestamp_str:
                try:
                    timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    timestamps.append(timestamp)
                except ValueError:
                    pass
        
        # Time range
        time_range = None
        if timestamps:
            timestamps.sort()
            time_range = {
                'start': timestamps[0].isoformat(),
                'end': timestamps[-1].isoformat(),
                'duration_seconds': (timestamps[-1] - timestamps[0]).total_seconds()
            }
        
        return {
            'total_entries': len(entries),
            'time_range': time_range,
            'levels': levels,
            'event_types': event_types,
            'users': users,
            'resources': resources
        }
    
    def search_logs(
        self,
        search_term: str,
        fields: Optional[List[str]] = None,
        case_sensitive: bool = False,
        regex: bool = False,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Search logs for a term.
        
        Args:
            search_term: Term to search for
            fields: Fields to search in (default: all text fields)
            case_sensitive: Whether search is case sensitive
            regex: Whether to use regex search
            limit: Maximum results to return
            
        Returns:
            List of matching log entries
        """
        if fields is None:
            fields = ['message', 'user_id', 'resource_id', 'event_type']
        
        filters = []
        for field in fields:
            operator = FilterOperator.REGEX if regex else FilterOperator.CONTAINS
            filters.append(LogFilter(
                field=field,
                operator=operator,
                value=search_term,
                case_sensitive=case_sensitive
            ))
        
        # Create query with OR logic (any field matches)
        results = []
        query = LogQuery(filters=[], limit=limit * len(fields))  # Get more to account for OR logic
        
        for entry in self.read_logs(query):
            # Check if any field matches
            for filter_condition in filters:
                if self._matches_filter(entry, filter_condition):
                    results.append(entry)
                    break
            
            if len(results) >= limit:
                break
        
        return results[:limit]


def create_time_range_filter(hours_back: int) -> LogQuery:
    """Create a query for logs from the last N hours.
    
    Args:
        hours_back: Number of hours to look back
        
    Returns:
        Log query for time range
    """
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=hours_back)
    
    return LogQuery(
        filters=[],
        start_time=start_time,
        end_time=end_time
    )


def create_user_activity_query(user_id: str, hours_back: int = 24) -> LogQuery:
    """Create a query for user activity.
    
    Args:
        user_id: User identifier
        hours_back: Hours to look back
        
    Returns:
        Log query for user activity
    """
    query = create_time_range_filter(hours_back)
    query.filters.append(LogFilter(
        field='user_id',
        operator=FilterOperator.EQUALS,
        value=user_id
    ))
    
    return query


def create_error_logs_query(hours_back: int = 24) -> LogQuery:
    """Create a query for error logs.
    
    Args:
        hours_back: Hours to look back
        
    Returns:
        Log query for errors
    """
    query = create_time_range_filter(hours_back)
    query.filters.append(LogFilter(
        field='level',
        operator=FilterOperator.IN,
        value=['ERROR', 'CRITICAL']
    ))
    
    return query


def create_resource_activity_query(resource_type: str, resource_id: str, hours_back: int = 24) -> LogQuery:
    """Create a query for resource activity.
    
    Args:
        resource_type: Type of resource
        resource_id: Resource identifier
        hours_back: Hours to look back
        
    Returns:
        Log query for resource activity
    """
    query = create_time_range_filter(hours_back)
    query.filters.extend([
        LogFilter(
            field='resource_type',
            operator=FilterOperator.EQUALS,
            value=resource_type
        ),
        LogFilter(
            field='resource_id',
            operator=FilterOperator.EQUALS,
            value=resource_id
        )
    ])
    
    return query