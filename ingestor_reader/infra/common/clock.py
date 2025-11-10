"""Clock and UUID generation abstraction for testing."""
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Optional
import uuid


class Clock(ABC):
    """Abstract clock interface for time and UUID generation."""
    
    @abstractmethod
    def now(self) -> datetime:
        """Get current UTC datetime."""
        pass
    
    @abstractmethod
    def now_iso(self) -> str:
        """Get current UTC datetime as ISO string."""
        pass
    
    @abstractmethod
    def generate_uuid(self) -> str:
        """Generate a unique UUID string."""
        pass
    
    @abstractmethod
    def generate_version_ts(self) -> str:
        """Generate version timestamp string."""
        pass


class SystemClock(Clock):
    """System clock implementation using real time."""
    
    def now(self) -> datetime:
        """Get current UTC datetime."""
        return datetime.now(timezone.utc)
    
    def now_iso(self) -> str:
        """Get current UTC datetime as ISO string."""
        return self.now().isoformat()
    
    def generate_uuid(self) -> str:
        """Generate a unique UUID string."""
        return str(uuid.uuid4())
    
    def generate_version_ts(self) -> str:
        """Generate version timestamp in ISO format (safe for filenames)."""
        return self.now().isoformat().replace(":", "-").split(".")[0]


# Default instance
_default_clock: Optional[Clock] = None


def get_clock() -> Clock:
    """Get the default clock instance."""
    global _default_clock
    if _default_clock is None:
        _default_clock = SystemClock()
    return _default_clock


def set_clock(clock: Clock) -> None:
    """Set the default clock instance (for testing)."""
    global _default_clock
    _default_clock = clock

