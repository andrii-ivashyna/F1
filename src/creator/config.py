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
    """ANSI escape codes for terminal colors."""
    RESET, BOLD, UNDERLINE = '\033[0m', '\033[1m', '\033[4m'
    BLACK, RED, GREEN, YELLOW = '\033[90m', '\033[91m', '\033[92m', '\033[93m'
    BLUE, MAGENTA, CYAN, WHITE = '\033[94m', '\033[95m', '\033[96m', '\033[97m'
    ORANGE = '\033[38;5;208m'

    @staticmethod
    def url(url_string):
        """Formats a URL for cleaner log output."""
        return f".../{url_string.split('/')[-1]}" if isinstance(url_string, str) and '/' in url_string else url_string

LOG_STYLES = {
    'INFO': Style.ORANGE,
    'SUCCESS': f'{Style.BOLD}{Style.GREEN}',
    'WARNING': f'{Style.BOLD}{Style.YELLOW}',
    'ERROR': f'{Style.BOLD}{Style.RED}',
    'HEADING': f'{Style.BOLD}{Style.WHITE}',
    'SUBHEADING': f'{Style.BOLD}{Style.MAGENTA}'
}

def log(message, msg_type='INFO', indent=0, data=None):
    """Prints a styled log message to the console."""
    style = LOG_STYLES.get(msg_type, Style.RESET)

    if msg_type == 'HEADING':
        print()
        full_message = f"{style}--- {message} ---{Style.RESET}"
    elif msg_type == 'SUBHEADING':
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
        time_str = f" {Style.YELLOW}({elapsed:.1f}s){Style.RESET}"

    parts = prefix_text.split(' | ')
    category = f"{parts[0]:<7}" if len(parts) > 0 else ""
    operation = f"{parts[1]:<15}" if len(parts) > 1 else ""
    count = f"{parts[2]:<7}" if len(parts) > 2 else f"{total:<7}" if len(parts) >= 2 else ""
    prefix_color = Style.CYAN

    if len(parts) >= 2:
        formatted_prefix = f"{prefix_color}{category}| {operation}| {count}|"
    else:
        formatted_prefix = f"{prefix_color}{prefix_text:<30}|"

    sys.stdout.write(f"\r{formatted_prefix}{bar}| {percent}%{Style.RESET}{time_str}")
    sys.stdout.flush()
    if current == total:
        sys.stdout.write(f"\r{formatted_prefix}{bar}| {percent}%{Style.RESET}{time_str}\n")
        sys.stdout.flush()

def show_completion_summary(start_time, start_datetime):
    """
    Displays an optimized process completion summary.
    """
    end_time = time.time()
    end_datetime = datetime.now()
    duration = end_time - start_time
    print()
    log(f"Process completed successfully!", 'SUCCESS')
    log(f"Start time: {Style.YELLOW}{start_datetime.strftime('%H:%M:%S')}{Style.RESET}", 'SUCCESS')
    log(f"End time: {Style.YELLOW}{end_datetime.strftime('%H:%M:%S')}{Style.RESET}", 'SUCCESS')
    log(f"Total duration: {Style.YELLOW}{f'{duration:.1f}s'}{Style.RESET}", 'SUCCESS')
    print()
