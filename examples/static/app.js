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

    socket.on('binary_event', (data) => {
        console.log('Binary Event Received:', data.data);
        messagesDiv.innerHTML += `<p>Binary Event: ${data.data}</p>`;
    });

    socket.on('text_event', (data) => {
        console.log('Redis Text Event Received:', data.data);
        messagesDiv.innerHTML += `<p>Redis Text Event: ${data.data}</p>`;
    });

    socket.on('json_event', (data) => {
        console.log('JSON Event Received:', data.data);
        messagesDiv.innerHTML += `<p>json Event: ${data.data}</p>`;
        // Process JSON data here
    });

    socket.on('broadcast', (data) => {
        console.log('Broadcast message Received:', data.data);
        messagesDiv.innerHTML += `<p>Broadcast Event: ${data.data}</p>`;
    });

    socket.on('binary_broadcast_event', (data) => {
        console.log('binary_broadcast_event message Received:', data.data);
        messagesDiv.innerHTML += `<p>Binary Broadcast Event: ${data.data}</p>`;
    });

    socket.on('kafka_event', (data) => {
        console.log('Kafka Text Event Received:', data.data);
        messagesDiv.innerHTML += `<p>Kafka Text Event: ${data.data}</p>`;
    });

    socket.on('join_success', (data) => {
        console.log(data.message);
        messagesDiv.innerHTML += `<p>${data.message}</p>`;
    });

    socket.onAny((eventName, ...args) => {
        console.log(`Event Received - Name: ${eventName}, Data:`, args);
    });
});