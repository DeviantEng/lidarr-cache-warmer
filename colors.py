#!/usr/bin/env python3
import os
import sys


class Colors:
    """ANSI color codes for terminal output"""
    # Standard colors
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    
    # Text formatting
    BOLD = '\033[1m'
    DIM = '\033[2m'
    UNDERLINE = '\033[4m'
    
    # Reset
    END = '\033[0m'
    
    # Status colors
    SUCCESS = GREEN
    ERROR = RED
    WARNING = YELLOW
    INFO = BLUE
    
    @classmethod
    def is_supported(cls) -> bool:
        """Check if terminal supports ANSI colors"""
        # Force enable colors in Docker containers (common case)
        if os.environ.get('container') or os.path.exists('/.dockerenv'):
            return True
        
        # Check if we're in a TTY (not redirected to file)
        if not sys.stdout.isatty():
            return False
        
        # Check common environment variables that force color
        if any(env in os.environ for env in ['COLORTERM', 'FORCE_COLOR']):
            return True
        
        # Check if colors are explicitly disabled
        if os.environ.get('NO_COLOR'):
            return False
        
        # Check terminal type
        term = os.environ.get('TERM', '').lower()
        if term in ('dumb', ''):
            return False
        
        # Common terminals that support color
        color_terms = ['xterm', 'screen', 'tmux', 'linux', 'ansi', 'color']
        return any(color_term in term for color_term in color_terms)
    
    @classmethod
    def colorize(cls, text: str, color: str, enabled: bool = True) -> str:
        """Apply color to text if colors are enabled and supported"""
        if not enabled or not cls.is_supported():
            return text
        return f"{color}{text}{cls.END}"
    
    @classmethod
    def red(cls, text: str, enabled: bool = True) -> str:
        """Make text red"""
        return cls.colorize(text, cls.RED, enabled)
    
    @classmethod
    def green(cls, text: str, enabled: bool = True) -> str:
        """Make text green"""
        return cls.colorize(text, cls.GREEN, enabled)
    
    @classmethod
    def yellow(cls, text: str, enabled: bool = True) -> str:
        """Make text yellow"""
        return cls.colorize(text, cls.YELLOW, enabled)
    
    @classmethod
    def blue(cls, text: str, enabled: bool = True) -> str:
        """Make text blue"""
        return cls.colorize(text, cls.BLUE, enabled)
    
    @classmethod
    def magenta(cls, text: str, enabled: bool = True) -> str:
        """Make text magenta"""
        return cls.colorize(text, cls.MAGENTA, enabled)
    
    @classmethod
    def cyan(cls, text: str, enabled: bool = True) -> str:
        """Make text cyan"""
        return cls.colorize(text, cls.CYAN, enabled)
    
    @classmethod
    def bold(cls, text: str, enabled: bool = True) -> str:
        """Make text bold"""
        return cls.colorize(text, cls.BOLD, enabled)
    
    @classmethod
    def success(cls, text: str, enabled: bool = True) -> str:
        """Make text green (success color)"""
        return cls.green(text, enabled)
    
    @classmethod
    def error(cls, text: str, enabled: bool = True) -> str:
        """Make text red (error color)"""
        return cls.red(text, enabled)
    
    @classmethod
    def warning(cls, text: str, enabled: bool = True) -> str:
        """Make text yellow (warning color)"""
        return cls.yellow(text, enabled)
    
    @classmethod
    def info(cls, text: str, enabled: bool = True) -> str:
        """Make text blue (info color)"""
        return cls.blue(text, enabled)

    @classmethod
    def dim(cls, text: str, enabled: bool = True) -> str:
        """Make text dim/faded"""
        return cls.colorize(text, cls.DIM, enabled)


# Convenience function for easy imports
def colorize_status(status: str, enabled: bool = True) -> str:
    """Colorize common status messages"""
    status_lower = status.lower()
    
    if 'success' in status_lower:
        return Colors.success(status, enabled)
    elif any(word in status_lower for word in ['timeout', 'failed', 'error']):
        return Colors.error(status, enabled)
    elif any(word in status_lower for word in ['warning', 'skip']):
        return Colors.warning(status, enabled)
    else:
        return status
