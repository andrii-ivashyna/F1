"""Individual table dashboard page"""

import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dash import dcc
from typing import Dict, Any, List
from .settings import get_colors, CHART_CONFIG
from .text_utils import format_labels_list, clean_field_name
from .data_utils import get_base_layout

class TablePage:
    """Creates individual table visualizations"""
    
    @staticmethod
    def create_table_overview(table_name: str, total: int, null: int) -> dcc.Graph:
        """Create table overview cards"""
        valid = total - null
        quality_score = (valid / total * 100) if total > 0 else 0
        
        fig = make_subplots(
            rows=1, cols=3,
            subplot_titles=("Total Records", "Quality %", "Missing"),
            specs=[[{"type": "indicator"}, {"type": "indicator"}, {"type": "indicator"}]],
            horizontal_spacing=CHART_CONFIG['subplot_spacing']
        )
        
        fig.add_trace(go.Indicator(
            mode="number",
            value=total,
            number={"font": {"size": 24, "color": '#3498DB'}}
        ), row=1, col=1)
        
        fig.add_trace(go.Indicator(
            mode="gauge+number",
            value=quality_score,
            gauge={
                'axis': {'range': [None, 100]},
                'bar': {'color': '#2ECC71'},
                'steps': [
                    {'range': [0, 50], 'color': '#E74C3C'},
                    {'range': [50, 80], 'color': '#F39C12'},
                    {'range': [80, 100], 'color': '#2ECC71'}
                ]
            }
        ), row=1, col=2)
        
        fig.add_trace(go.Indicator(
            mode="number",
            value=null,
            number={"font": {"size": 24, "color": '#E74C3C'}}
        ), row=1, col=3)
        
        layout = get_base_layout()
        layout.update({
            'title': f"📋 {table_name} Overview",
            'height': CHART_CONFIG['overview_height']
        })
        fig.update_layout(layout)
        
        return dcc.Graph(figure=fig, style={'marginBottom': CHART_CONFIG['margin_bottom']})
    
    @staticmethod
    def create_completeness_chart(data: Dict[str, Any], fields: List[str]) -> dcc.Graph:
        """Create data completeness chart"""
        completeness_data = []
        
        for field in fields:
            if field.endswith('_total_count'):
                base_field = field.replace('_total_count', '')
                null_key = f"{base_field}_null_count"
                
                if null_key in data:
                    total = data[field]
                    null_count = data[null_key]
                    valid_count = total - null_count
                    
                    completeness_data.append({
                        'field': clean_field_name(base_field),
                        'valid_pct': (valid_count / total * 100) if total > 0 else 0,
                        'null_pct': (null_count / total * 100) if total > 0 else 0
                    })
        
        if not completeness_data:
            return dcc.Graph(figure=go.Figure(), style={'display': 'none'})
        
        labels = format_labels_list([item['field'] for item in completeness_data])
        
        fig = go.Figure()
        fig.add_trace(go.Bar(
            name='Valid',
            x=labels,
            y=[item['valid_pct'] for item in completeness_data],
            marker_color='#2ECC71'
        ))
        fig.add_trace(go.Bar(
            name='Missing',
            x=labels,
            y=[item['null_pct'] for item in completeness_data],
            marker_color='#E74C3C'
        ))
        
        layout = get_base_layout()
        layout.update({
            'title': "📊 Field Completeness (%)",
            'yaxis_title': "Percentage",
            'yaxis': {'range': [0, 100]},
            'barmode': 'stack',
            'height': CHART_CONFIG['bar_height']
        })
        fig.update_layout(layout)
        
        return dcc.Graph(figure=fig, style={'marginBottom': CHART_CONFIG['margin_bottom']})
    
    @staticmethod
    def create_box_plots(data: Dict[str, Any], fields: List[str]) -> dcc.Graph:
        """Create box plots for numerical statistics - NO X LABELS"""
        stats_data = {}
        
        for field in fields:
            if field.endswith('_mean'):
                base_field = field.replace('_mean', '')
                stats = {}
                
                for stat in ['mean', 'median', 'std', 'min', 'max', 'q25', 'q75']:
                    key = f"{base_field}_{stat}"
                    if key in data:
                        stats[stat] = data[key]
                
                if len(stats) >= 5:
                    stats_data[clean_field_name(base_field)] = stats
        
        if not stats_data:
            return dcc.Graph(figure=go.Figure(), style={'display': 'none'})
        
        n_fields = len(stats_data)
        cols = min(2, n_fields)
        rows = (n_fields + cols - 1) // cols
        
        fig = make_subplots(
            rows=rows, cols=cols,
            subplot_titles=list(stats_data.keys()),
            specs=[[{"type": "box"}] * cols for _ in range(rows)]
        )
        
        colors = get_colors(n_fields)
        
        for i, (field, stats) in enumerate(stats_data.items()):
            row = i // cols + 1
            col = i % cols + 1
            
            fig.add_trace(go.Box(
                q1=[stats.get('q25', 0)],
                median=[stats.get('median', 0)],
                q3=[stats.get('q75', 0)],
                lowerfence=[stats.get('min', 0)],
                upperfence=[stats.get('max', 0)],
                mean=[stats.get('mean', 0)],
                boxpoints=False,
                marker_color=colors[i],
                showlegend=False,
                # NO X-AXIS LABELS - this removes x labels completely
                x=[field]  # Single category per box
            ), row=row, col=col)
            
            # Remove x-axis labels for this subplot
            fig.update_xaxes(showticklabels=False, row=row, col=col)
        
        layout = get_base_layout()
        layout.update({
            'title': "📈 Numerical Distribution",
            'height': CHART_CONFIG['box_height'] * rows
        })
        fig.update_layout(layout)
        
        return dcc.Graph(figure=fig, style={'marginBottom': CHART_CONFIG['margin_bottom']})
    
    @staticmethod
    def create_categorical_charts(data: Dict[str, Any], fields: List[str]) -> dcc.Graph:
        """Create categorical data charts"""
        if not fields:
            return dcc.Graph(figure=go.Figure(), style={'display': 'none'})
        
        n_fields = len(fields)
        cols = min(2, n_fields)
        rows = (n_fields + cols - 1) // cols
        
        field_titles = [clean_field_name(f.replace('_top_values', '')) for f in fields]
        
        fig = make_subplots(
            rows=rows, cols=cols,
            subplot_titles=field_titles,
            specs=[[{"type": "bar"}] * cols for _ in range(rows)]
        )
        
        for i, field in enumerate(fields):
            row = i // cols + 1
            col = i % cols + 1
            
            if field in data and isinstance(data[field], dict):
                values = data[field]
                labels = format_labels_list(list(values.keys()))
                field_colors = get_colors(len(values))
                
                fig.add_trace(
                    go.Bar(
                        x=labels,
                        y=list(values.values()),
                        marker_color=field_colors,
                        showlegend=False
                    ),
                    row=row, col=col
                )
        
        layout = get_base_layout()
        layout.update({
            'title': "📊 Top Values",
            'height': CHART_CONFIG['bar_height'] * rows
        })
        fig.update_layout(layout)
        
        return dcc.Graph(figure=fig, style={'marginBottom': CHART_CONFIG['margin_bottom']})
