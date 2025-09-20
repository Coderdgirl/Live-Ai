import React, { useState, useEffect } from 'react';
import { w3cwebsocket as W3CWebSocket } from "websocket";
import { Box, TextField, Button, Paper, Typography } from '@mui/material';

const client = new W3CWebSocket('ws://127.0.0.1:8000/ws/chatbot');

const Chatbot = () => {
    const [messages, setMessages] = useState([]);
    const [inputValue, setInputValue] = useState('');

    useEffect(() => {
        client.onopen = () => {
            console.log('WebSocket Client Connected');
        };
        client.onmessage = (message) => {
            setMessages(prevMessages => [...prevMessages, { text: message.data, sender: 'bot' }]);
        };
    }, []);

    const sendMessage = () => {
        if (inputValue.trim()) {
            client.send(inputValue);
            setMessages(prevMessages => [...prevMessages, { text: inputValue, sender: 'user' }]);
            setInputValue('');
        }
    };

    return (
        <Box sx={{ p: 2, display: 'flex', flexDirection: 'column', height: 'calc(100vh - 100px)' }}>
            <Paper elevation={3} sx={{ flexGrow: 1, p: 2, overflowY: 'auto', mb: 2 }}>
                {messages.map((msg, index) => (
                    <Box key={index} sx={{ mb: 1, textAlign: msg.sender === 'user' ? 'right' : 'left' }}>
                        <Typography variant="body1" component="span" sx={{
                            bgcolor: msg.sender === 'user' ? 'primary.main' : 'grey.300',
                            color: msg.sender === 'user' ? 'primary.contrastText' : 'inherit',
                            p: 1,
                            borderRadius: 2
                        }}>
                            <strong>{msg.sender}: </strong>{msg.text}
                        </Typography>
                    </Box>
                ))}
            </Paper>
            <Box sx={{ display: 'flex' }}>
                <TextField
                    fullWidth
                    variant="outlined"
                    value={inputValue}
                    onChange={(e) => setInputValue(e.target.value)}
                    onKeyPress={(e) => e.key === 'Enter' && sendMessage()}
                    placeholder="Type a message..."
                />
                <Button variant="contained" color="primary" onClick={sendMessage} sx={{ ml: 1 }}>
                    Send
                </Button>
            </Box>
        </Box>
    );
};

export default Chatbot;