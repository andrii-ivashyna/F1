# config.py
"""
Application configuration and constants.
Contains only configuration values, styling, and logging utilities.
"""

import sys
import time
from datetime import datetime

# --- Application Configuration ---
DB_FILE = 'data/formula.db'
API_BASE_URL = 'https://api.openf1.org/v1'
YEAR = 2025  # Fetch data from this year

# --- Terminal Logging Styling ---
class Style:
    """ANSI escape codes for terminal colors, primarily using the 256-color palette."""
    RESET, BOLD, UNDERLINE = '\033[0m', '\033[1m', '\033[4m'
    
    # Basic colors - Dark variants
    DARK_RED = '\033[38;5;88m'      # Dark red
    DARK_GREEN = '\033[38;5;22m'    # Dark green  
    DARK_YELLOW = '\033[38;5;58m'   # Dark yellow/olive
    DARK_BLUE = '\033[38;5;18m'     # Dark blue
    DARK_MAGENTA = '\033[38;5;90m'  # Dark magenta/purple
    DARK_CYAN = '\033[38;5;23m'     # Dark cyan/teal
    DARK_ORANGE = '\033[38;5;130m'  # Dark orange
    DARK_PINK = '\033[38;5;125m'    # Dark pink
    
    # Basic colors - Bright variants
    BRIGHT_RED = '\033[38;5;196m'    # Bright red
    BRIGHT_GREEN = '\033[38;5;46m'   # Bright green
    BRIGHT_YELLOW = '\033[38;5;226m' # Bright yellow
    BRIGHT_BLUE = '\033[38;5;21m'    # Bright blue
    BRIGHT_MAGENTA = '\033[38;5;201m'# Bright magenta
    BRIGHT_CYAN = '\033[38;5;51m'    # Bright cyan
    BRIGHT_ORANGE = '\033[38;5;208m' # Bright orange
    BRIGHT_PINK = '\033[38;5;205m'   # Bright pink
    
    # Special colors
    BLACK = '\033[38;5;16m'         # Near black (not pure #000000)
    WHITE = '\033[38;5;252m'        # Near white (not pure #ffffff)
    GRAY = '\033[38;5;8m'           # Dark gray
    LIGHT_GRAY = '\033[38;5;246m'   # Light gray

    @staticmethod
    def url(url_string):
        """Formats a URL for cleaner log output."""
        return f".../{url_string.split('/')[-1]}" if isinstance(url_string, str) and '/' in url_string else url_string

LOG_STYLES = {
    'INFO': Style.WHITE,
    'SUCCESS': f'{Style.BOLD}{Style.BRIGHT_GREEN}',
    'WARNING': f'{Style.BOLD}{Style.BRIGHT_YELLOW}',
    'ERROR': f'{Style.BOLD}{Style.BRIGHT_RED}',
    'HEADING': f'{Style.BOLD}{Style.BRIGHT_BLUE}',
    'SUBHEADING': f'{Style.BOLD}{Style.BRIGHT_PINK}',
}

def log(message, msg_type='INFO', indent=0, data=None):
    """Prints a styled log message to the console."""
    style = LOG_STYLES.get(msg_type, Style.RESET)

    if msg_type == 'HEADING' or msg_type == 'SUBHEADING':
        print() # Add empty line before heading/subheading
        full_message = f"{style}--- {message} ---{Style.RESET}"
    else:
        full_message = f"{style}{message}{Style.RESET}"

    print(full_message)
    sys.stdout.flush()

def show_progress_bar(current, total, prefix_text='', length=40, fill='█', start_time=None):
    """
    Displays a dynamic progress bar in the console with improved formatting.
    :param current: Current iteration.
    :param total: Total iterations.
    :param prefix_text: The descriptive text for the progress bar (e.g., "API | Meetings | 11").
    :param length: Character length of the bar.
    :param fill: Bar fill character.
    :param start_time: Optional, time.time() when the operation started, to display elapsed time.
    """
    percent = ("{0:.1f}").format(100 * (current / float(total)))
    filled_length = int(length * current // total)
    bar = fill * filled_length + '-' * (length - filled_length)

    time_str = ""
    if start_time is not None:
        elapsed = time.time() - start_time
        time_str = f" {Style.BRIGHT_YELLOW}({elapsed:.1f}s){Style.RESET}"

    parts = prefix_text.split(' | ')
    category = f"{parts[0]:<7}" if len(parts) > 0 else ""
    operation = f"{parts[1]:<15}" if len(parts) > 1 else ""
    count = f"{parts[2]:<7}" if len(parts) > 2 else f"{total:<7}" if len(parts) >= 2 else ""
    prefix_color = Style.BRIGHT_CYAN

    if len(parts) >= 2:
        formatted_prefix = f"{prefix_color}{category}| {operation}| {count}|"
    else:
        formatted_prefix = f"{prefix_color}{prefix_text:<30}|"

    sys.stdout.write(f"\r{formatted_prefix}{bar}| {percent}%{Style.RESET}{time_str}")
    sys.stdout.flush()
    if current == total:
        sys.stdout.write(f"\r{formatted_prefix}{bar}| {percent}%{Style.RESET}{time_str}\n")
        sys.stdout.flush()

def show_completion_summary(start_datetime):
    """
    Displays an optimized process completion summary, using only start_datetime.
    """
    end_datetime = datetime.now()
    duration = (end_datetime - start_datetime).total_seconds()
    time_color = f'{Style.BOLD}{Style.BRIGHT_ORANGE}'
    print()
    log(f"Process completed successfully!", 'SUCCESS')
    log(f"Start time: {time_color}{start_datetime.strftime('%H:%M:%S')}{Style.RESET}", 'SUCCESS')
    log(f"End time: {time_color}{end_datetime.strftime('%H:%M:%S')}{Style.RESET}", 'SUCCESS')
    log(f"Total duration: {time_color}{f'{duration:.1f}s'}{Style.RESET}", 'SUCCESS')
    print()
