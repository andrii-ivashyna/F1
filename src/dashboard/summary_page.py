"""Summary dashboard page"""

import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dash import dcc
from typing import Dict, Any, List
from .settings import get_colors, CHART_CONFIG
from .text_utils import format_labels_list, clean_field_name
from .data_utils import get_base_layout

class SummaryPage:
    """Creates summary dashboard visualizations"""
    
    @staticmethod
    def create_overview_cards(data: Dict[str, Any]) -> dcc.Graph:
        """Create overview metrics cards"""
        general = data.get("data", {}).get("general", {})
        
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=("Total Records", "Sample Size", "Tables Available", "Tables Completed"),
            specs=[[{"type": "indicator"}, {"type": "indicator"}],
                   [{"type": "indicator"}, {"type": "indicator"}]],
            vertical_spacing=0.4
        )
        
        metrics = [
            (general.get("total_records", 0), 1, 1),
            (general.get("sample_size", 0), 1, 2),
            (general.get("tables_available", 0), 2, 1),
            (general.get("tables_completed", 0), 2, 2)
        ]
        
        for value, row, col in metrics:
            fig.add_trace(
                go.Indicator(
                    mode="number",
                    value=value,
                    number={"font": {"size": 24}}
                ),
                row=row, col=col
            )
        
        layout = get_base_layout()
        layout.update({
            'title': "📊 Database Overview",
            'height': CHART_CONFIG['overview_height']
        })
        fig.update_layout(layout)
        
        return dcc.Graph(figure=fig, style={'marginBottom': CHART_CONFIG['margin_bottom']})
    
    @staticmethod
    def create_tables_chart(data: Dict[str, Any]) -> dcc.Graph:
        """Create tables records distribution chart"""
        tables_data = data.get("data", {}).get("tables_overview", {})
        
        # Extract table records
        records = {}
        for key, value in tables_data.items():
            if key.endswith('_total_records'):
                table_name = key.replace('_total_records', '')
                records[clean_field_name(table_name)] = value
        
        sorted_items = sorted(records.items(), key=lambda x: x[1], reverse=True)
        labels = format_labels_list([item[0] for item in sorted_items])
        values = [item[1] for item in sorted_items]
        colors = get_colors(len(sorted_items))
        
        fig = go.Figure(data=[
            go.Bar(
                x=values,
                y=labels,
                orientation='h',
                marker=dict(color=colors),
                text=[f"{v:,}" for v in values],
                textposition='auto',
            )
        ])
        
        layout = get_base_layout()
        layout.update({
            'title': "📈 Records by Table",
            'xaxis_title': "Records Count",
            'xaxis_type': "log",
            'height': CHART_CONFIG['bar_height']
        })
        fig.update_layout(layout)
        
        return dcc.Graph(figure=fig, style={'marginBottom': CHART_CONFIG['margin_bottom']})
    
    @staticmethod
    def create_quality_chart(data: Dict[str, Any]) -> dcc.Graph:
        """Create data quality overview chart"""
        tables_data = data.get("data", {}).get("tables_overview", {})
        
        quality_data = []
        for key, value in tables_data.items():
            if key.endswith('_total_records'):
                table_name = key.replace('_total_records', '')
                null_key = f"{table_name}_null_records"
                
                if null_key in tables_data:
                    total = value
                    null_count = tables_data[null_key]
                    valid_count = total - null_count
                    
                    quality_data.append({
                        'table': clean_field_name(table_name),
                        'valid_pct': (valid_count / total * 100) if total > 0 else 0,
                        'null_pct': (null_count / total * 100) if total > 0 else 0
                    })
        
        quality_data.sort(key=lambda x: x['valid_pct'], reverse=True)
        labels = format_labels_list([item['table'] for item in quality_data])
        
        fig = go.Figure()
        fig.add_trace(go.Bar(
            name='Valid',
            x=labels,
            y=[item['valid_pct'] for item in quality_data],
            marker_color='#2ECC71'
        ))
        fig.add_trace(go.Bar(
            name='Missing',
            x=labels,
            y=[item['null_pct'] for item in quality_data],
            marker_color='#E74C3C'
        ))
        
        layout = get_base_layout()
        layout.update({
            'title': "🎯 Data Quality (%)",
            'yaxis_title': "Percentage",
            'barmode': 'stack',
            'height': CHART_CONFIG['bar_height']
        })
        fig.update_layout(layout)
        
        return dcc.Graph(figure=fig, style={'marginBottom': CHART_CONFIG['margin_bottom']})
