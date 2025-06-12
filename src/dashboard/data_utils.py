"""Data processing utilities"""

from settings import LAYOUT_CONFIG

def categorize_fields(data: dict) -> dict:
    """Categorize data fields by type"""
    if "data" not in data:
        return {"categorical": [], "numerical": [], "date": [], "foreign_key": []}
    
    table_data = data["data"]
    categories = {"categorical": [], "numerical": [], "date": [], "foreign_key": []}
    
    for key in table_data.keys():
        if key in ['table_total_records', 'table_null_records']:
            continue
            
        if 'FK_' in key or key.endswith('_key'):
            categories["foreign_key"].append(key)
        elif 'date' in key.lower() or 'time' in key.lower():
            categories["date"].append(key)
        elif any(suffix in key for suffix in ['_total_count', '_null_count', '_unique_count', 
                                            '_mean', '_median', '_std', '_min', '_max', '_q25', '_q75']):
            categories["numerical"].append(key)
        elif key.endswith('_top_values'):
            categories["categorical"].append(key)
    
    return categories

def get_base_layout():
    """Get base layout configuration for all plots"""
    return {
        'paper_bgcolor': LAYOUT_CONFIG['paper_bgcolor'],
        'plot_bgcolor': LAYOUT_CONFIG['plot_bgcolor'],
        'font': {'family': LAYOUT_CONFIG['font_family']},
        'showlegend': True,
        'xaxis': {'tickangle': 0},  # No rotation for x-axis labels
        'yaxis': {'tickangle': 0}   # Keep y-axis labels vertical (default)
    }
