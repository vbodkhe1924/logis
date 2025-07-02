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
            'port': 5432,
            'database': 'logistics_db',
            'user': 'vb',
            'password': 'varad123'
        }
        try:
            self.redis_client = redis.Redis(host='localhost', port=6379, db=0, socket_timeout=5)
            # Test Redis connection
            self.redis_client.ping()
            self.redis_connected = True
        except:
            self.redis_connected = False
    
    def test_db_connection(self):
        """Test database connection and show table info"""
        try:
            with psycopg2.connect(**self.db_config) as conn:
                # Check tables
                tables = pd.read_sql("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                """, conn)
                
                # Check row counts
                counts = {}
                for table in ['driver_locations', 'delivery_analytics']:
                    try:
                        count_df = pd.read_sql(f"SELECT COUNT(*) as count FROM {table}", conn)
                        counts[table] = count_df.iloc[0]['count']
                    except:
                        counts[table] = "Table doesn't exist"
                
                return True, tables, counts
        except Exception as e:
            return False, str(e), {}
    
    def get_realtime_kpis(self):
        """Fetch real-time KPIs with error handling"""
        try:
            with psycopg2.connect(**self.db_config) as conn:
                # Active drivers
                try:
                    active_drivers = pd.read_sql("""
                        SELECT COUNT(DISTINCT driver_id) as count
                        FROM driver_locations 
                        WHERE timestamp > NOW() - INTERVAL '5 minutes'
                        AND status != 'idle'
                    """, conn)
                    active_count = active_drivers.iloc[0]['count'] if not active_drivers.empty else 0
                except:
                    active_count = 0
                
                # Total deliveries today
                try:
                    deliveries_today = pd.read_sql("""
                        SELECT COUNT(*) as count
                        FROM delivery_analytics 
                        WHERE DATE(timestamp) = CURRENT_DATE
                        AND status = 'delivered'
                    """, conn)
                    delivery_count = deliveries_today.iloc[0]['count'] if not deliveries_today.empty else 0
                except:
                    delivery_count = 0
                
                # Average delay
                try:
                    avg_delay = pd.read_sql("""
                        SELECT AVG(delay_minutes) as avg_delay
                        FROM delivery_analytics 
                        WHERE timestamp > NOW() - INTERVAL '1 hour'
                        AND delay_minutes > 0
                    """, conn)
                    delay = round(avg_delay.iloc[0]['avg_delay'] or 0, 1)
                except:
                    delay = 0
                
                return {
                    'active_drivers': active_count,
                    'deliveries_today': delivery_count,
                    'avg_delay': delay
                }
        except Exception as e:
            st.error(f"Error fetching KPIs: {e}")
            return {'active_drivers': 0, 'deliveries_today': 0, 'avg_delay': 0}
    
    def get_driver_locations(self):
        """Fetch recent driver locations with error handling"""
        try:
            with psycopg2.connect(**self.db_config) as conn:
                return pd.read_sql("""
                    SELECT driver_id, latitude, longitude, speed, status
                    FROM driver_locations 
                    WHERE timestamp > NOW() - INTERVAL '2 minutes'
                """, conn)
        except Exception as e:
            st.error(f"Error fetching driver locations: {e}")
            return pd.DataFrame()
    
    def get_delivery_trends(self):
        """Fetch delivery trends with error handling"""
        try:
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
        except Exception as e:
            st.error(f"Error fetching delivery trends: {e}")
            return pd.DataFrame()
    
    def get_active_alerts(self):
        """Fetch active alerts from Redis with error handling"""
        if not self.redis_connected:
            return []
        
        try:
            alerts = []
            for key in self.redis_client.scan_iter(match="alert:*"):
                alert_data = json.loads(self.redis_client.get(key))
                alerts.append(alert_data)
            return alerts
        except Exception as e:
            st.error(f"Error fetching alerts: {e}")
            return []
    
    def insert_sample_data(self):
        """Insert sample data for testing"""
        try:
            with psycopg2.connect(**self.db_config) as conn:
                cursor = conn.cursor()
                
                # Sample driver locations
                cursor.execute("""
                    INSERT INTO driver_locations (driver_id, latitude, longitude, speed, status, timestamp)
                    VALUES 
                    ('D001', 18.5204, 73.8567, 45, 'active', NOW()),
                    ('D002', 18.5304, 73.8467, 30, 'active', NOW()),
                    ('D003', 18.5104, 73.8667, 0, 'idle', NOW())
                    ON CONFLICT DO NOTHING
                """)
                
                # Sample delivery analytics
                cursor.execute("""
                    INSERT INTO delivery_analytics (order_id, driver_id, status, delay_minutes, timestamp)
                    VALUES 
                    ('O001', 'D001', 'delivered', 5, NOW()),
                    ('O002', 'D002', 'delivered', 0, NOW()),
                    ('O003', 'D003', 'in_transit', 10, NOW())
                    ON CONFLICT DO NOTHING
                """)
                
                conn.commit()
                return True
        except Exception as e:
            st.error(f"Error inserting sample data: {e}")
            return False
    
    def render_dashboard(self):
        """Render the main dashboard with debugging"""
        st.set_page_config(page_title="Logistics Dashboard", layout="wide")
        
        st.title("Logistics Real-Time Dashboard")
        
        # Debug section
        with st.expander("🔍 Debug Information", expanded=True):
            db_connected, tables, counts = self.test_db_connection()
            
            col1, col2 = st.columns(2)
            with col1:
                if db_connected:
                    st.success("✅ Database Connected")
                    st.write("**Available Tables:**")
                    st.dataframe(tables)
                    st.write("**Row Counts:**")
                    for table, count in counts.items():
                        st.write(f"- {table}: {count}")
                else:
                    st.error(f"❌ Database Error: {tables}")
            
            with col2:
                if self.redis_connected:
                    st.success("✅ Redis Connected")
                else:
                    st.error("❌ Redis Not Connected")
                
                if st.button("Insert Sample Data"):
                    if self.insert_sample_data():
                        st.success("Sample data inserted!")
                        st.rerun()
        
        # Auto-refresh toggle
        auto_refresh = st.checkbox("Auto-refresh (10s)", value=False)
        
        # KPIs Row
        st.subheader("📊 Key Performance Indicators")
        col1, col2, col3, col4 = st.columns(4)
        
        kpis = self.get_realtime_kpis()
        
        with col1:
            st.metric("Active Drivers", kpis['active_drivers'])
        
        with col2:
            st.metric("Deliveries Today", kpis['deliveries_today'])
        
        with col3:
            st.metric("Avg Delay (min)", kpis['avg_delay'])
        
        with col4:
            alerts = self.get_active_alerts()
            st.metric("Active Alerts", len(alerts))
        
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
                st.info("No recent driver location data available")
                st.write("**Debug:** Raw driver locations data:")
                st.dataframe(driver_locations)
        
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
        else:
            st.info("No delivery trend data available")
        
        # Auto-refresh logic
        if auto_refresh:
            time.sleep(10)
            st.rerun()

if __name__ == "__main__":
    dashboard = LogisticsDashboard()
    dashboard.render_dashboard()