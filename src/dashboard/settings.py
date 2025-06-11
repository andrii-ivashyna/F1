"""Configuration settings for F1 Dashboard"""

# Color palette for high visibility
COLORS = [
    '#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FECA57', '#FF9FF3',
    '#54A0FF', '#5F27CD', '#00D2D3', '#FF9F43', '#EE5A6F', '#C44569',
    '#F8B500', '#6C5CE7', '#A29BFE', '#FD79A8', '#E17055', '#00B894',
    '#FDCB6E', '#6C5CE7', '#74B9FF', '#81ECEC', '#FAB1A0', '#E84393'
]

# Table icons mapping
TABLE_ICONS = {
    'meetings': '🏁', 'sessions': '⏱️', 'race_control': '🚩',
    'pit': '🔧', 'stints': '🏎️', 'team_radio': '📻',
    'drivers': '👨‍🏁', 'weather': '🌤️', 'position': '📍',
    'laps': '⭕', 'intervals': '⏰', 'location': '🗺️',
    'car_data': '📊'
}

# Layout settings
LAYOUT_CONFIG = {
    'paper_bgcolor': 'rgba(0,0,0,0)',
    'plot_bgcolor': 'rgba(0,0,0,0)',
    'font_family': 'Arial, sans-serif',
    'title_font_size': 16,
    'axis_font_size': 12,
    'max_label_length': 12,  # Max chars per line in labels
    'max_label_lines': 3     # Max lines for labels
}

# Chart specific settings
CHART_CONFIG = {
    'bar_height': 400,
    'box_height': 300,
    'overview_height': 400,
    'margin_bottom': 20,
    'subplot_spacing': 0.15
}

def get_colors(n: int) -> list:
    """Get n colors from palette, repeating if necessary"""
    if n <= len(COLORS):
        return COLORS[:n]
    return (COLORS * ((n // len(COLORS)) + 1))[:n]

def get_table_icon(filename: str) -> str:
    """Get appropriate icon for table type"""
    for key, icon in TABLE_ICONS.items():
        if key in filename:
            return icon
    return '📋'
