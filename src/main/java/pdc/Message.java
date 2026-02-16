package pdc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.io.InputStream;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * This is a CUSTOM BINARY PROTOCOL - NOT JSON, NOT Java Serialization.
 * 
 * Message Format (Total: variable length):
 * - Magic Number (4 bytes): 0xDEADBEEF - identifies our protocol
 * - Version (1 byte): protocol version
 * - Message Type (1 byte): 0=REGISTER, 1=TASK, 2=RESULT, 3=HEARTBEAT, 4=ACK
 * - Sender ID Length (2 bytes): how long is the sender name
 * - Sender ID (variable): the actual sender name
 * - Task ID (4 bytes): which task this message belongs to
 * - Payload Length (4 bytes): how big is the data
 * - Payload (variable): the actual matrix data
 * - Timestamp (8 bytes): when this message was sent
 */
public class Message {
    // Message types as constants for clarity
    public static final byte TYPE_REGISTER = 0;
    public static final byte TYPE_TASK = 1;
    public static final byte TYPE_RESULT = 2;
    public static final byte TYPE_HEARTBEAT = 3;
    public static final byte TYPE_ACK = 4;
    
    // Protocol constants
    private static final int MAGIC_NUMBER = 0xDEADBEEF;
    private static final byte VERSION = 1;
    
    // Message fields
    private byte type;
    private String sender;
    private int taskId;
    private byte[] payload;
    private long timestamp;

    // Constructor
    public Message(byte type, String sender, int taskId, byte[] payload) {
        this.type = type;
        this.sender = sender != null ? sender : "";
        this.taskId = taskId;
        this.payload = payload != null ? payload : new byte[0];
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * Converts the message to a byte stream for network transmission.
     */
    public byte[] pack() {
        // Convert sender string to bytes
        byte[] senderBytes = sender.getBytes(StandardCharsets.UTF_8);
        
        // Calculate total message size
        int totalSize = 
            4 + // Magic number
            1 + // Version
            1 + // Type
            2 + // Sender length
            senderBytes.length + // Sender
            4 + // Task ID
            4 + // Payload length
            payload.length + // Payload
            8; // Timestamp
        
        // Create buffer with exactly the right size
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.order(ByteOrder.BIG_ENDIAN); // Use network byte order
        
        // Write each field in order
        buffer.putInt(MAGIC_NUMBER);     // 4 bytes: Magic number
        buffer.put(VERSION);              // 1 byte: Version
        buffer.put(type);                 // 1 byte: Message type
        buffer.putShort((short) senderBytes.length); // 2 bytes: Sender length
        buffer.put(senderBytes);           // variable: Sender
        buffer.putInt(taskId);             // 4 bytes: Task ID
        buffer.putInt(payload.length);      // 4 bytes: Payload length
        buffer.put(payload);                // variable: Payload
        buffer.putLong(timestamp);          // 8 bytes: Timestamp
        
        return buffer.array();
    }

    /**
     * Reconstructs a Message from a byte stream.
     * This MUST read in the EXACT same order as pack() wrote.
     */
    public static Message unpack(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.order(ByteOrder.BIG_ENDIAN);
        
        // Read in the SAME ORDER as pack()
        int magic = buffer.getInt();
        if (magic != MAGIC_NUMBER) {
            throw new IllegalArgumentException("Invalid magic number: not a valid message");
        }
        
        byte version = buffer.get(); // Version
        byte type = buffer.get();
        
        short senderLen = buffer.getShort();
        byte[] senderBytes = new byte[senderLen];
        buffer.get(senderBytes);
        String sender = new String(senderBytes, StandardCharsets.UTF_8);
        
        int taskId = buffer.getInt();
        
        int payloadLen = buffer.getInt();
        byte[] payload = new byte[payloadLen];
        buffer.get(payload);
        
        long timestamp = buffer.getLong();
        
        // Create and return the message
        Message msg = new Message(type, sender, taskId, payload);
        msg.timestamp = timestamp; // Use the original timestamp
        return msg;
    }

    /**
     * Helper method to read a complete message from an input stream.
     * Handles TCP fragmentation by reading until we have a full message.
     */
    public static Message readFromStream(InputStream in) throws IOException {
        // Buffer to hold header parts
        byte[] headerBuffer = new byte[1024]; 
        int bytesRead = 0;
        
        // 1. Read fixed start of header to get Sender Length (Magic+Ver+Type+SenderLen = 8 bytes)
        while (bytesRead < 8) { 
            int result = in.read(headerBuffer, bytesRead, 8 - bytesRead);
            if (result == -1) return null; // Stream ended
            bytesRead += result;
        }
        
        // 2. Peek at sender length to calculate remaining header size
        ByteBuffer headerPeek = ByteBuffer.wrap(headerBuffer, 0, bytesRead);
        headerPeek.order(ByteOrder.BIG_ENDIAN);
        headerPeek.getInt(); // Skip magic
        headerPeek.get(); // Skip version
        headerPeek.get(); // Skip type
        short senderLen = headerPeek.getShort();
        
        // 3. Read rest of header (TaskID + PayloadLen = 8 bytes)
        // Total header before payload = 8 (initial) + 8 (rest) = 16 bytes excluding sender string
        // Actually: Magic(4)+Ver(1)+Type(1)+SenderLen(2)+Sender(var)+Task(4)+PayloadLen(4)
        // We have read 8 bytes. We need to read Sender(var) + 4 + 4.
        
        byte[] restOfHeader = new byte[senderLen + 8]; 
        bytesRead = 0;
        while (bytesRead < restOfHeader.length) {
            int result = in.read(restOfHeader, bytesRead, restOfHeader.length - bytesRead);
            if (result == -1) throw new IOException("Stream ended mid-header");
            bytesRead += result;
        }
        
        // 4. Extract Payload Length from the combined header data
        // PayloadLen is at offset: 8 (initial) + senderLen
        ByteBuffer fullHeaderCheck = ByteBuffer.allocate(8 + senderLen + 8);
        fullHeaderCheck.order(ByteOrder.BIG_ENDIAN);
        fullHeaderCheck.put(headerBuffer, 0, 8);
        fullHeaderCheck.put(restOfHeader);
        
        // Position to read PayloadLen: Magic(4)+Ver(1)+Type(1)+SenderLen(2)+Sender(var)+TaskID(4)
        int payloadLenOffset = 4 + 1 + 1 + 2 + senderLen + 4;
        fullHeaderCheck.position(payloadLenOffset);
        int payloadLen = fullHeaderCheck.getInt();
        
        // 5. Read the actual payload
        byte[] payload = new byte[payloadLen];
        bytesRead = 0;
        while (bytesRead < payloadLen) {
            int result = in.read(payload, bytesRead, payloadLen - bytesRead);
            if (result == -1) throw new IOException("Stream ended mid-payload");
            bytesRead += result;
        }
        
        // 6. Reconstruct the complete message byte array for unpacking
        // Total = 8 (start) + senderLen + 8 (task+paylen) + payloadLen
        // Note: restOfHeader contains senderBytes + taskID + payloadLen
        byte[] completeMsg = new byte[8 + restOfHeader.length + payloadLen]; 
        System.arraycopy(headerBuffer, 0, completeMsg, 0, 8);
        System.arraycopy(restOfHeader, 0, completeMsg, 8, restOfHeader.length);
        System.arraycopy(payload, 0, completeMsg, 8 + restOfHeader.length, payloadLen);
        
        return unpack(completeMsg);
    }

    // Getters
    public byte getType() { return type; }
    public String getSender() { return sender; }
    public int getTaskId() { return taskId; }
    public byte[] getPayload() { return payload; }
    public long getTimestamp() { return timestamp; }
}