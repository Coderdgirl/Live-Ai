import React from 'react';
import { Card, CardContent, Typography, Box, Chip } from '@mui/material';
import WarningIcon from '@mui/icons-material/Warning';

const AlertCard = ({ alert }) => {
  const getSeverityColor = (severity) => {
    switch (severity.toLowerCase()) {
      case 'high':
        return 'error';
      case 'medium':
        return 'warning';
      case 'low':
        return 'info';
      default:
        return 'default';
    }
  };

  return (
    <Card sx={{ mb: 2, border: `1px solid ${alert.severity === 'high' ? '#ff4d4f' : '#f0f0f0'}` }}>
      <CardContent>
        <Box display="flex" justifyContent="space-between" alignItems="center" mb={1}>
          <Box display="flex" alignItems="center">
            <WarningIcon color="error" sx={{ mr: 1 }} />
            <Typography variant="h6">{alert.title}</Typography>
          </Box>
          <Chip 
            label={alert.severity} 
            color={getSeverityColor(alert.severity)} 
            size="small" 
          />
        </Box>
        <Typography variant="body2" color="text.secondary" mb={1}>
          {alert.description}
        </Typography>
        <Box display="flex" justifyContent="space-between">
          <Typography variant="caption" color="text.secondary">
            ID: {alert.id}
          </Typography>
          <Typography variant="caption" color="text.secondary">
            {new Date(alert.timestamp).toLocaleString()}
          </Typography>
        </Box>
      </CardContent>
    </Card>
  );
};

export default AlertCard;