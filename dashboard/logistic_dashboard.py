import streamlit as st
import psycopg2
import redis
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time

class LogisticsDashboard:
    def __init__(self):
        self.db_config = {
            'host': 'localhost',
            'database': 'logistics_db',
            'user': 'admin',
            'password': 'password'
        }
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0)
    
    def get_realtime_kpis(self):
        """Fetch real-time KPIs"""
        with psycopg2.connect(**self.db_config) as conn:
            # Active drivers
            active_drivers = pd.read_sql("""
                SELECT COUNT(DISTINCT driver_id) as count
                FROM driver_locations 
                WHERE timestamp > NOW() - INTERVAL '5 minutes'
                AND status != 'idle'
            """, conn)
            
            # Total deliveries today
            deliveries_today = pd.read_sql("""
                SELECT COUNT(*) as count
                FROM delivery_analytics 
                WHERE DATE(timestamp) = CURRENT_DATE
                AND status = 'delivered'
            """, conn)
            
            # Average delay
            avg_delay = pd.read_sql("""
                SELECT AVG(delay_minutes) as avg_delay
                FROM delivery_analytics 
                WHERE timestamp > NOW() - INTERVAL '1 hour'
                AND delay_minutes > 0
            """, conn)
            
            return {
                'active_drivers': active_drivers.iloc[0]['count'] if not active_drivers.empty else 0,
                'deliveries_today': deliveries_today.iloc[0]['count'] if not deliveries_today.empty else 0,
                'avg_delay': round(avg_delay.iloc[0]['avg_delay'] or 0, 1)
            }
    
    def get_driver_locations(self):
        """Fetch recent driver locations"""
        with psycopg2.connect(**self.db_config) as conn:
            return pd.read_sql("""
                SELECT driver_id, latitude, longitude, speed, status
                FROM driver_locations 
                WHERE timestamp > NOW() - INTERVAL '2 minutes'
            """, conn)
    
    def get_delivery_trends(self):
        """Fetch delivery trends"""
        with psycopg2.connect(**self.db_config) as conn:
            return pd.read_sql("""
                SELECT 
                    DATE_TRUNC('hour', timestamp) as hour,
                    COUNT(*) as deliveries,
                    AVG(delay_minutes) as avg_delay
                FROM delivery_analytics 
                WHERE timestamp > NOW() - INTERVAL '24 hours'
                GROUP BY hour
                ORDER BY hour
            """, conn)
    
    def get_active_alerts(self):
        """Fetch active alerts from Redis"""
        alerts = []
        for key in self.redis_client.scan_iter(match="alert:*"):
            alert_data = json.loads(self.redis_client.get(key))
            alerts.append(alert_data)
        return alerts
    
    def render_dashboard(self):
        """Render the main dashboard"""
        st.set_page_config(page_title="Porter Logistics Dashboard", layout="wide")
        
        st.title("🚚 Porter Logistics Real-Time Dashboard")
        
        # Auto-refresh every 10 seconds
        if st.checkbox("Auto-refresh (10s)", value=True):
            time.sleep(10)
            st.experimental_rerun()
        
        # KPIs Row
        col1, col2, col3, col4 = st.columns(4)
        
        kpis = self.get_realtime_kpis()
        
        with col1:
            st.metric("Active Drivers", kpis['active_drivers'], delta=None)
        
        with col2:
            st.metric("Deliveries Today", kpis['deliveries_today'], delta=None)
        
        with col3:
            st.metric("Avg Delay (min)", kpis['avg_delay'], delta=None)
        
        with col4:
            alerts = self.get_active_alerts()
            st.metric("Active Alerts", len(alerts), delta=None)
        
        # Maps and Charts Row
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.subheader("🗺️ Driver Locations")
            driver_locations = self.get_driver_locations()
            
            if not driver_locations.empty:
                # Create map
                fig = px.scatter_mapbox(
                    driver_locations,
                    lat="latitude",
                    lon="longitude",
                    color="status",
                    size="speed",
                    hover_data=["driver_id", "speed"],
                    mapbox_style="open-street-map",
                    zoom=10,
                    height=400
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No recent driver location data")
        
        with col2:
            st.subheader("⚠️ Active Alerts")
            if alerts:
                for alert in alerts[-5:]:  # Show last 5 alerts
                    alert_type = alert.get('anomaly_type', 'unknown')
                    if alert_type == 'high_delay':
                        st.error(f"🚨 High Delay: Order {alert['order_id']} - {alert['delay_minutes']} min")
                    elif alert_type == 'cancellation':
                        st.warning(f"❌ Cancellation: Order {alert['order_id']}")
            else:
                st.success("No active alerts")
        
        # Delivery Trends
        st.subheader("📊 Delivery Trends (24h)")
        trends = self.get_delivery_trends()
        
        if not trends.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.line(trends, x='hour', y='deliveries', title='Deliveries per Hour')
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                fig = px.line(trends, x='hour', y='avg_delay', title='Average Delay per Hour')
                st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    dashboard = LogisticsDashboard()
    dashboard.render_dashboard()