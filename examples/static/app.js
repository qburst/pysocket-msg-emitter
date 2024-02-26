
document.addEventListener('DOMContentLoaded', () => {
    const namespace = '/QB_space';
    const socket = io(`http://127.0.0.1:5000${namespace}`, {
        transports: ['websocket', 'polling']
    });
    const roomInput = document.getElementById('roomInput');
    const joinRoomBtn = document.getElementById('joinRoomBtn');
    const messagesDiv = document.getElementById('messages');

    joinRoomBtn.onclick = function() {
        const room = roomInput.value.trim();
        if (room) {
            socket.emit('join', { room: room });
            console.log(`Attempted to join room: ${room}`);
        }
    };

    socket.on('connect', () => {
        console.log('Connected to the server.');
    });

    socket.on('disconnect', (reason) => {
        console.log('Disconnected from the server. Reason:', reason);
    });

    // Listening to Kafka events
    socket.on('kafka_event', (data) => {
        console.log('Kafka Event Received:', data);
        messagesDiv.innerHTML += `<p>Kafka Event: ${JSON.stringify(data)}</p>`;
    });
    socket.on('kafkabinary_event', (data) => {
        // Assuming data is an object with a data array inside
        if (data && Array.isArray(data.data)) {
            data.data.forEach(item => {
                if (item._is_binary && item.data) {
                    // Decode the base64-encoded string
                    const binaryString = atob(item.data);
                    // Convert the binary string to a Blob or another format depending on your needs
                    // For text data, you might directly decode it to a string
                    const textString = decodeURIComponent(escape(binaryString));
    
                    // Display or process the decoded string
                    console.log("Decoded binary data:", textString);
                    
                    // Update the UI to show the decoded data
                    const messagesDiv = document.getElementById('messages');
                    const messageElement = document.createElement('p');
                    messageElement.textContent = `Decoded binary data: ${textString}`;
                    messagesDiv.appendChild(messageElement);
                }
            });
        } else {
            console.log("Received unexpected data structure:", data);
        }
    });
    
    
    socket.on('event', (data) => {
        console.log('Kafka Event Received:', data);
        messagesDiv.innerHTML += `<p>Kafka Event: ${JSON.stringify(data)}</p>`;
    });

    // Listening to Redis events
    socket.on('binary_event', (data) => {
        console.log('Redis binary Event Received:', data);
        messagesDiv.innerHTML += `<p>Redis Binary  Event: ${JSON.stringify(data)}</p>`;
    });
    socket.on('text_event', (data) => {
        console.log('Redis Text Event Received:', data);
        messagesDiv.innerHTML += `<p>Redis  Event: ${JSON.stringify(data)}</p>`;
    });

    // Listening to broadcast events from Redis
    socket.on('json_broadcast_event', (data) => {
        console.log('JSON Broadcast Event Received:', data);
        messagesDiv.innerHTML += `<p>JSON redis Broadcast Event: ${JSON.stringify(data)}</p>`;
    });

    // Listening to broadcast events specifically from Kafka
    socket.on('broadcast_event_kafka', (data) => {
        console.log('Kafka Broadcast Event Received:', data);
        messagesDiv.innerHTML += `<p>Kafka Broadcast Event: ${JSON.stringify(data)}</p>`;
    });

    socket.on('join_success', (data) => {
        console.log(data.message);
        messagesDiv.innerHTML += `<p>${data.message}</p>`;
    });

    socket.onAny((eventName, ...args) => {
        console.log(`Event Received - Name: ${eventName}, Data:`, args);
        
    });
});
