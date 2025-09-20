"""
Dash dashboard for LiveGuard AI
Provides real-time visualization of data and alerts
"""
import os
import logging
import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, html, dcc, callback, Output, Input
import dash_bootstrap_components as dbc
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

# Configuration
DASHBOARD_HOST = os.getenv('DASHBOARD_HOST', '0.0.0.0')
DASHBOARD_PORT = int(os.getenv('DASHBOARD_PORT', '8050'))
API_HOST = os.getenv('API_HOST', '0.0.0.0')
API_PORT = int(os.getenv('API_PORT', '8000'))
API_URL = f"http://{API_HOST}:{API_PORT}"

# Initialize Dash app
app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.DARKLY],
    title="LiveGuard AI Dashboard"
)

# Define layout
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H1("LiveGuard AI Dashboard", className="text-center my-4"),
            html.P("Real-Time Intelligence for Finance, Security, and Beyond", className="text-center lead mb-4")
        ])
    ]),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Active Alerts"),
                dbc.CardBody([
                    html.Div(id="alerts-container", style={"maxHeight": "300px", "overflow": "auto"})
                ])
            ], className="mb-4")
        ], width=12)
    ]),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Financial Transactions"),
                dbc.CardBody([
                    dcc.Graph(id="financial-chart")
                ])
            ])
        ], width=6),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Security Events"),
                dbc.CardBody([
                    dcc.Graph(id="security-chart")
                ])
            ])
        ], width=6)
    ], className="mb-4"),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Anomaly Detection"),
                dbc.CardBody([
                    dcc.Graph(id="anomaly-chart")
                ])
            ])
        ], width=12)
    ], className="mb-4"),
    
    dcc.Interval(
        id='interval-component',
        interval=5*1000,  # in milliseconds (5 seconds)
        n_intervals=0
    )
], fluid=True)

# Callbacks
@app.callback(
    [
        Output("alerts-container", "children"),
        Output("financial-chart", "figure"),
        Output("security-chart", "figure"),
        Output("anomaly-chart", "figure")
    ],
    [Input("interval-component", "n_intervals")]
)
def update_dashboard(n):
    """Update dashboard with latest data"""
    # Get alerts
    alerts_html = get_alerts_html()
    
    # Get financial chart
    financial_fig = get_financial_chart()
    
    # Get security chart
    security_fig = get_security_chart()
    
    # Get anomaly chart
    anomaly_fig = get_anomaly_chart()
    
    return alerts_html, financial_fig, security_fig, anomaly_fig

def get_alerts_html():
    """Get alerts and format as HTML"""
    try:
        response = requests.get(f"{API_URL}/alerts")
        alerts = response.json()
        
        if not alerts:
            return html.P("No active alerts", className="text-muted")
        
        alert_cards = []
        for alert in alerts:
            severity_color = {
                "high": "danger",
                "medium": "warning",
                "low": "info"
            }.get(alert.get("severity", "low"), "info")
            
            alert_cards.append(
                dbc.Alert(
                    [
                        html.H5(alert.get("message", "Alert"), className="alert-heading"),
                        html.P(f"Type: {alert.get('type', 'Unknown')}"),
                        html.P(f"Time: {alert.get('timestamp', 'Unknown')}"),
                        html.Hr(),
                        html.P(json.dumps(alert.get("data", {})), className="mb-0 text-small")
                    ],
                    color=severity_color,
                    className="mb-3"
                )
            )
        
        return alert_cards
    except Exception as e:
        logger.error(f"Error getting alerts: {e}")
        return html.P(f"Error loading alerts: {str(e)}", className="text-danger")

def get_financial_chart():
    """Generate financial transactions chart"""
    # In a real app, this would fetch data from the API
    # For demo, we'll generate random data
    dates = pd.date_range(start=datetime.now() - timedelta(hours=1), end=datetime.now(), freq='5min')
    df = pd.DataFrame({
        'timestamp': dates,
        'amount': np.random.normal(500, 200, size=len(dates)),
        'transaction_type': np.random.choice(['purchase', 'withdrawal', 'transfer', 'deposit'], size=len(dates))
    })
    
    fig = px.line(
        df, 
        x='timestamp', 
        y='amount', 
        color='transaction_type',
        title='Financial Transactions (Last Hour)'
    )
    
    fig.update_layout(
        template='plotly_dark',
        xaxis_title='Time',
        yaxis_title='Amount ($)',
        legend_title='Transaction Type'
    )
    
    return fig

def get_security_chart():
    """Generate security events chart"""
    # In a real app, this would fetch data from the API
    # For demo, we'll generate random data
    dates = pd.date_range(start=datetime.now() - timedelta(hours=1), end=datetime.now(), freq='5min')
    event_types = ['login', 'logout', 'file_access', 'admin_action', 'password_change', 'api_access']
    
    data = []
    for event_type in event_types:
        data.append({
            'event_type': event_type,
            'count': np.random.randint(5, 50)
        })
    
    df = pd.DataFrame(data)
    
    fig = px.bar(
        df,
        x='event_type',
        y='count',
        title='Security Events by Type (Last Hour)'
    )
    
    fig.update_layout(
        template='plotly_dark',
        xaxis_title='Event Type',
        yaxis_title='Count'
    )
    
    return fig

def get_anomaly_chart():
    """Generate anomaly detection chart"""
    # In a real app, this would fetch data from the API
    # For demo, we'll generate random data
    dates = pd.date_range(start=datetime.now() - timedelta(hours=24), end=datetime.now(), freq='1h')
    
    df = pd.DataFrame({
        'timestamp': dates,
        'anomaly_score': np.random.normal(0.3, 0.2, size=len(dates))
    })
    
    # Add some anomalies
    anomaly_indices = np.random.choice(len(df), size=3, replace=False)
    df.loc[anomaly_indices, 'anomaly_score'] = np.random.uniform(0.8, 0.95, size=len(anomaly_indices))
    
    # Create figure
    fig = go.Figure()
    
    # Add normal points
    normal_mask = df['anomaly_score'] < 0.7
    fig.add_trace(go.Scatter(
        x=df.loc[normal_mask, 'timestamp'],
        y=df.loc[normal_mask, 'anomaly_score'],
        mode='markers',
        name='Normal',
        marker=dict(color='blue', size=8)
    ))
    
    # Add anomaly points
    anomaly_mask = df['anomaly_score'] >= 0.7
    fig.add_trace(go.Scatter(
        x=df.loc[anomaly_mask, 'timestamp'],
        y=df.loc[anomaly_mask, 'anomaly_score'],
        mode='markers',
        name='Anomaly',
        marker=dict(color='red', size=12)
    ))
    
    # Add threshold line
    fig.add_shape(
        type="line",
        x0=df['timestamp'].min(),
        y0=0.7,
        x1=df['timestamp'].max(),
        y1=0.7,
        line=dict(color="yellow", width=2, dash="dash")
    )
    
    fig.update_layout(
        title='Anomaly Detection (Last 24 Hours)',
        template='plotly_dark',
        xaxis_title='Time',
        yaxis_title='Anomaly Score',
        showlegend=True
    )
    
    return fig

def start_dashboard():
    """Start the Dash dashboard"""
    logger.info(f"Starting dashboard at http://{DASHBOARD_HOST}:{DASHBOARD_PORT}")
    app.run_server(host=DASHBOARD_HOST, port=DASHBOARD_PORT)

if __name__ == "__main__":
    # Configure logging when run directly
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    start_dashboard()