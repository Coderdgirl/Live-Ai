import React, { useState, useEffect } from 'react';
import { Grid, Typography, Box, Paper, Stack } from '@mui/material';
import AlertCard from '../components/AlertCard';
import TransactionChart from '../components/TransactionChart';
import SecurityEventsChart from '../components/SecurityEventsChart';
import AnomalyCard from '../components/AnomalyCard';
import DownloadButton from '../components/DownloadButton';

// API base URL from environment variable
const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';

const Dashboard = () => {
  const [alerts, setAlerts] = useState([]);
  const [transactions, setTransactions] = useState([]);
  const [securityEvents, setSecurityEvents] = useState([]);
  const [anomalies, setAnomalies] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // Sample data for development
  const sampleData = {
    alerts: [
      { id: 'a1', title: 'Suspicious Login Attempt', description: 'Multiple failed login attempts from IP 192.168.1.105', severity: 'high', timestamp: new Date().toISOString() },
      { id: 'a2', title: 'Unusual Transaction Pattern', description: 'Multiple small transactions followed by large withdrawal', severity: 'medium', timestamp: new Date(Date.now() - 300000).toISOString() }
    ],
    transactions: Array(20).fill().map((_, i) => ({
      id: `t${i}`,
      amount: Math.floor(Math.random() * 10000) / 100,
      timestamp: new Date(Date.now() - i * 60000).toISOString(),
      account: `ACC-${1000 + i}`
    })),
    securityEvents: Array(10).fill().map((_, i) => ({
      id: `e${i}`,
      type: ['login', 'logout', 'access', 'transfer', 'update'][Math.floor(Math.random() * 5)],
      count: Math.floor(Math.random() * 10) + 1,
      timestamp: new Date(Date.now() - i * 120000).toISOString()
    })),
    anomalies: [
      { id: 'an1', type: 'transaction', description: 'Unusual transaction amount detected', score: 0.89, timestamp: new Date().toISOString() },
      { id: 'an2', type: 'access', description: 'Access from unusual location', score: 0.76, timestamp: new Date(Date.now() - 450000).toISOString() }
    ]
  };

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        
        // In a production environment, we would fetch from the API
        // const alertsResponse = await fetch(`${API_URL}/api/alerts`);
        // const transactionsResponse = await fetch(`${API_URL}/api/transactions`);
        // const eventsResponse = await fetch(`${API_URL}/api/security-events`);
        // const anomaliesResponse = await fetch(`${API_URL}/api/anomalies`);
        
        // For development, use sample data
        setAlerts(sampleData.alerts);
        setTransactions(sampleData.transactions);
        setSecurityEvents(sampleData.securityEvents);
        setAnomalies(sampleData.anomalies);
        
        setLoading(false);
      } catch (err) {
        console.error('Error fetching dashboard data:', err);
        setError('Failed to load dashboard data. Please try again later.');
        setLoading(false);
      }
    };

    fetchData();

    // Set up polling interval for real-time updates
    const interval = setInterval(fetchData, 5000);
    
    // Clean up interval on component unmount
    return () => clearInterval(interval);
  }, []);

  if (loading) {
    return <Typography>Loading dashboard data...</Typography>;
  }

  if (error) {
    return <Typography color="error">{error}</Typography>;
  }

  return (
    <Box sx={{ flexGrow: 1, mt: 2 }}>
      <Typography variant="h4" gutterBottom>
        LiveGuard AI Dashboard
      </Typography>
      
      <Grid container spacing={3}>
        {/* Active Alerts Section */}
        <Grid item xs={12} md={4}>
          <Paper sx={{ p: 2, height: '100%' }}>
            <Typography variant="h6" gutterBottom>
              Active Alerts
            </Typography>
            {alerts.length > 0 ? (
              alerts.map(alert => (
                <AlertCard key={alert.id} alert={alert} />
              ))
            ) : (
              <Typography>No active alerts</Typography>
            )}
          </Paper>
        </Grid>
        
        {/* Anomalies Section */}
        <Grid item xs={12} md={8}>
          <Paper sx={{ p: 2, height: '100%' }}>
            <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
              <Typography variant="h6">
                Detected Anomalies
              </Typography>
              <Stack direction="row" spacing={1}>
                <DownloadButton 
                  endpoint="/download/financial_anomalies" 
                  label="Financial CSV" 
                  size="small"
                  color="primary"
                />
                <DownloadButton 
                  endpoint="/security/anomalies/csv" 
                  label="Security CSV" 
                  size="small"
                  color="secondary"
                />
              </Stack>
            </Box>
            {anomalies.length > 0 ? (
              anomalies.map(anomaly => (
                <AnomalyCard key={anomaly.id} anomaly={anomaly} />
              ))
            ) : (
              <Typography>No anomalies detected</Typography>
            )}
          </Paper>
        </Grid>
        
        {/* Financial Transactions Chart */}
        <Grid item xs={12} md={6}>
          <TransactionChart data={transactions} />
        </Grid>
        
        {/* Security Events Chart */}
        <Grid item xs={12} md={6}>
          <SecurityEventsChart data={securityEvents} />
        </Grid>
      </Grid>
    </Box>
  );
};

export default Dashboard;