import React, { useState } from 'react';
import { Button, Snackbar, Alert } from '@mui/material';
import DownloadIcon from '@mui/icons-material/Download';

const DownloadButton = ({ endpoint, label, variant = "outlined", size = "small", color = "primary" }) => {
  const [error, setError] = useState(null);
  
  const handleDownload = async () => {
    try {
      // Clear any previous errors
      setError(null);
      
      // API base URL from environment variable or default
      const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';
      console.log(`Downloading from: ${API_URL}${endpoint}`);
      
      const response = await fetch(`${API_URL}${endpoint}`, {
        method: 'GET',
        mode: 'cors',
        credentials: 'omit', // Changed back to 'omit' to match backend configuration
        headers: {
          'Accept': 'text/csv, application/octet-stream, application/json',
          'Content-Type': 'application/json',
        },
      });
      
      if (!response.ok) {
        console.error('Download failed with status:', response.status);
        // Try to get more detailed error message from response
        let errorText;
        try {
          const errorData = await response.json();
          errorText = errorData.detail || errorData.message || `Status: ${response.status}`;
        } catch (e) {
          errorText = response.statusText || `Status code: ${response.status}`;
        }
        throw new Error(`Failed to download: ${errorText}`);
      }
      
      // Get filename from Content-Disposition header or use a default
      const contentDisposition = response.headers.get('Content-Disposition');
      let filename = 'download.csv';
      
      if (contentDisposition) {
        const filenameMatch = contentDisposition.match(/filename="(.+)"/);
        if (filenameMatch && filenameMatch[1]) {
          filename = filenameMatch[1];
        }
      }
      
      // Create a blob from the response
      const blob = await response.blob();
      
      // Create a download link and trigger it
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', filename);
      document.body.appendChild(link);
      link.click();
      
      // Clean up
      link.parentNode.removeChild(link);
      window.URL.revokeObjectURL(url);
    } catch (error) {
      console.error('Download error:', error);
      
      // Handle specific network errors
      let errorMessage;
      if (error.message === 'Failed to fetch') {
        errorMessage = 'Network error: Unable to connect to the server. Please check your internet connection and make sure the server is running.';
      } else if (error.name === 'AbortError') {
        errorMessage = 'Request was aborted. Please try again.';
      } else if (error.name === 'TypeError' && error.message.includes('NetworkError')) {
        errorMessage = 'Network error: Unable to reach the server. Please check your connection.';
      } else {
        errorMessage = error.message || 'Unknown error';
      }
      
      const apiUrl = process.env.REACT_APP_API_URL || 'http://localhost:8000';
      console.log('Error details:', {
        message: errorMessage,
        originalError: error.toString(),
        endpoint: endpoint,
        url: `${apiUrl}${endpoint}`
      });
      
      // Set error state instead of using alert
      setError(errorMessage);
    }
  };

  return (
    <>
      <Button 
        variant={variant}
        size={size}
        color={color}
        startIcon={<DownloadIcon />}
        onClick={handleDownload}
      >
        {label}
      </Button>
      
      <Snackbar 
        open={!!error} 
        autoHideDuration={6000} 
        onClose={() => setError(null)}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
      >
        <Alert onClose={() => setError(null)} severity="error" sx={{ width: '100%' }}>
          {error}
        </Alert>
      </Snackbar>
    </>
  );
};

export default DownloadButton;