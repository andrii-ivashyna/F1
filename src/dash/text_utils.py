"""Text processing utilities for labels and formatting"""

from settings import LAYOUT_CONFIG

def split_text_multiline(text: str, max_chars: int = None, max_lines: int = None) -> str:
    """Split text into multiple lines for better display"""
    if max_chars is None:
        max_chars = LAYOUT_CONFIG['max_label_length']
    if max_lines is None:
        max_lines = LAYOUT_CONFIG['max_label_lines']
    
    if len(text) <= max_chars:
        return text
    
    words = text.split()
    if len(words) == 1:
        # Single long word - split by characters
        lines = []
        for i in range(0, len(text), max_chars):
            lines.append(text[i:i+max_chars])
            if len(lines) >= max_lines:
                break
        return '<br>'.join(lines[:max_lines])
    
    # Multiple words - split by words
    lines = []
    current_line = ""
    
    for word in words:
        test_line = f"{current_line} {word}".strip()
        if len(test_line) <= max_chars:
            current_line = test_line
        else:
            if current_line:
                lines.append(current_line)
                if len(lines) >= max_lines:
                    break
            current_line = word
    
    if current_line and len(lines) < max_lines:
        lines.append(current_line)
    
    return '<br>'.join(lines[:max_lines])

def format_labels_list(labels: list) -> list:
    """Format a list of labels for multiline display"""
    return [split_text_multiline(str(label)) for label in labels]

def clean_field_name(field_name: str) -> str:
    """Clean field name for display"""
    return field_name.replace('_', ' ').replace('  ', ' ').title()
