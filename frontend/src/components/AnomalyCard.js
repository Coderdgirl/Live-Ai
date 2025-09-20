import React from 'react';
import { Card, CardContent, Typography, Box, Chip } from '@mui/material';
import ErrorOutlineIcon from '@mui/icons-material/ErrorOutline';

const AnomalyCard = ({ anomaly }) => {
  return (
    <Card sx={{ mb: 2, border: '1px solid #ff9800' }}>
      <CardContent>
        <Box display="flex" justifyContent="space-between" alignItems="center" mb={1}>
          <Box display="flex" alignItems="center">
            <ErrorOutlineIcon color="warning" sx={{ mr: 1 }} />
            <Typography variant="h6">Anomaly Detected</Typography>
          </Box>
          <Chip 
            label={`Score: ${anomaly.score.toFixed(2)}`} 
            color="warning" 
            size="small" 
          />
        </Box>
        <Typography variant="body2" mb={1}>
          {anomaly.description}
        </Typography>
        <Box display="flex" justifyContent="space-between">
          <Typography variant="caption" color="text.secondary">
            Type: {anomaly.type}
          </Typography>
          <Typography variant="caption" color="text.secondary">
            {new Date(anomaly.timestamp).toLocaleString()}
          </Typography>
        </Box>
      </CardContent>
    </Card>
  );
};

export default AnomalyCard;