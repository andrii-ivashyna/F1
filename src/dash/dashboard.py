"""Main Dashboard Application"""

import dash
from dash import dcc, html, Input, Output
from typing import Dict, List
from settings import get_table_icon
from data_loader import DataLoader
from summary_page import SummaryPage
from table_page import TablePage
from data_utils import categorize_fields
from text_utils import clean_field_name

class F1Dashboard:
    """Main F1 Dashboard application"""
    
    def __init__(self, db_name: str = "f1db_YR=2024"):
        self.db_name = db_name
        self.data_loader = DataLoader(f"data/{db_name}/analysis/")
        self.summary_page = SummaryPage()
        self.table_page = TablePage()
        self.app = dash.Dash(__name__)
        self._setup_layout()
        self._setup_callbacks()
    
    def _get_table_options(self) -> List[Dict[str, str]]:
        """Get dropdown options for tables"""
        options = [{'label': '📊 Analysis Summary', 'value': 'analysis_summary.json'}]
        
        for file in self.data_loader.get_file_list():
            table_name = clean_field_name(file.replace('_statistics.json', ''))
            icon = get_table_icon(file)
            options.append({'label': f'{icon} {table_name}', 'value': file})
        
        return options
    
    def _setup_layout(self):
        """Setup dashboard layout"""
        self.app.layout = html.Div([
            # Header
            html.Div([
                html.H1("🏎️ F1 Data Dashboard", 
                       style={'textAlign': 'center', 'color': '#2c3e50'}),
                html.P(f"Database: {self.db_name}", 
                      style={'textAlign': 'center', 'color': '#7f8c8d'})
            ], style={'marginBottom': 30}),
            
            # Controls
            html.Div([
                html.Div([
                    html.Label("Select Table:", style={'fontWeight': 'bold'}),
                    dcc.Dropdown(
                        id='table-dropdown',
                        options=self._get_table_options(),
                        value='analysis_summary.json',
                        clearable=False
                    )
                ], style={'width': '48%', 'display': 'inline-block'}),
                
                html.Div(id='timestamp', 
                        style={'width': '48%', 'float': 'right', 'textAlign': 'right', 
                               'color': '#7f8c8d', 'marginTop': 25})
            ], style={'marginBottom': 30}),
            
            # Content
            html.Div(id='content'),
            
            # Footer
            html.Hr(),
            html.P("F1 Data Analysis Dashboard", 
                  style={'textAlign': 'center', 'color': '#bdc3c7', 'fontSize': 12})
        ], style={'maxWidth': '1200px', 'margin': '0 auto', 'padding': '20px'})
    
    def _setup_callbacks(self):
        """Setup dashboard callbacks"""
        @self.app.callback(
            [Output('content', 'children'), Output('timestamp', 'children')],
            [Input('table-dropdown', 'value')]
        )
        def update_content(selected_file):
            if not selected_file:
                return html.Div("Select a table"), ""
            
            data = self.data_loader.load_data(selected_file)
            if "error" in data:
                return html.Div(f"❌ {data['error']}"), ""
            
            timestamp = data.get('timestamp', '')
            timestamp_display = f"Updated: {timestamp}" if timestamp else ""
            
            if selected_file == 'analysis_summary.json':
                figures = [
                    self.summary_page.create_overview_cards(data),
                    self.summary_page.create_tables_chart(data),
                    self.summary_page.create_quality_chart(data)
                ]
            else:
                table_name = clean_field_name(selected_file.replace('_statistics.json', ''))
                table_data = data.get("data", {})
                categories = categorize_fields(data)
                
                figures = []
                
                # Table overview
                total = table_data.get("table_total_records", 0)
                null = table_data.get("table_null_records", 0)
                if total > 0:
                    figures.append(self.table_page.create_table_overview(table_name, total, null))
                
                # Data completeness
                if categories["numerical"]:
                    figures.append(self.table_page.create_completeness_chart(table_data, categories["numerical"]))
                
                # Box plots (no x labels)
                if categories["numerical"]:
                    figures.append(self.table_page.create_box_plots(table_data, categories["numerical"]))
                
                # Categorical charts
                if categories["categorical"]:
                    figures.append(self.table_page.create_categorical_charts(table_data, categories["categorical"]))
            
            return html.Div(figures) if figures else html.Div("No data available"), timestamp_display
    
    def run(self, debug=True, port=8050):
        """Run the dashboard"""
        print(f"🚀 Starting F1 Dashboard: {self.db_name}")
        print(f"🌐 Available at: http://localhost:{port}")
        self.app.run(debug=debug, port=port, host='0.0.0.0')
