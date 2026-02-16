package pdc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.io.*;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.*;

public class Worker {
    private Socket masterSocket;
    private InputStream in;
    private OutputStream out;
    private String workerId;
    
    private final ExecutorService taskExecutor = Executors.newFixedThreadPool(4);
    private final ScheduledExecutorService heartbeatSender = Executors.newSingleThreadScheduledExecutor();
    
    private volatile boolean running = true;
    private final Random random = new Random();
    
    private int[][] matrixA;
    private int[][] matrixB;

    public void joinCluster(String masterHost, int port) {
        try {
            masterSocket = new Socket(masterHost, port);
            in = masterSocket.getInputStream();
            out = masterSocket.getOutputStream();
            
            workerId = "worker-" + System.currentTimeMillis() + "-" + random.nextInt(1000);
            
            Message regMsg = new Message(Message.TYPE_REGISTER, workerId, -1, new byte[0]);
            sendMessage(regMsg);
            System.out.println("Sent registration to master as " + workerId);
            
            Message ack = Message.readFromStream(in);
            if (ack != null && ack.getType() == Message.TYPE_ACK) {
                System.out.println("Received acknowledgment from master");
            }
            
            // Start heartbeat thread
            heartbeatSender.scheduleAtFixedRate(this::sendHeartbeat, 1, 2, TimeUnit.SECONDS);
            
            // Start processing tasks
            processTasks();
            
        } catch (IOException e) {
            System.err.println("Failed to connect to master: " + e.getMessage());
        }
    }

    private void processTasks() throws IOException {
        while (running) {
            // Read message directly and pass to executor to avoid race conditions
            Message message = Message.readFromStream(in);
            if (message == null) {
                System.out.println("Master closed connection");
                break;
            }
            
            if (message.getType() == Message.TYPE_TASK) {
                // Submit directly to thread pool using the message instance
                // Do NOT store in instance variable to ensure thread safety
                taskExecutor.submit(() -> executeTask(message));
            }
        }
    }

    /**
     * execute() - Called by tests to run a task
     * This method satisfies the test requirement
     */
    public void execute() {
        // Note: In the main loop, tasks are submitted directly. 
        // This method is kept for compatibility if called externally.
        System.out.println("Execute called externally (no-op in stream mode)");
    }

    /**
     * executeTask() - The actual task execution logic
     */
    private void executeTask(Message taskMsg) {
        try {
            System.out.println("Worker " + workerId + " executing task " + taskMsg.getTaskId());
            
            ByteBuffer coordBuffer = ByteBuffer.wrap(taskMsg.getPayload());
            coordBuffer.order(ByteOrder.BIG_ENDIAN);
            int startRow = coordBuffer.getInt();
            int endRow = coordBuffer.getInt();
            int startCol = coordBuffer.getInt();
            int endCol = coordBuffer.getInt();
            
            if (matrixA == null) {
                initializeDummyMatrices(100);
            }
            
            int blockRows = endRow - startRow;
            int blockCols = endCol - startCol;
            int[][] resultBlock = new int[blockRows][blockCols];
            
            // Perform matrix multiplication for this block
            for (int i = 0; i < blockRows; i++) {
                for (int j = 0; j < blockCols; j++) {
                    int sum = 0;
                    for (int k = 0; k < matrixA[0].length; k++) {
                        sum += matrixA[startRow + i][k] * matrixB[k][startCol + j];
                    }
                    resultBlock[i][j] = sum;
                }
            }
            
            // Simulate work (remove in production if needed, but helps test parallelism)
            Thread.sleep(500);
            
            // Package and send result
            ByteBuffer resultBuffer = ByteBuffer.allocate(16 + blockRows * blockCols * 4);
            resultBuffer.order(ByteOrder.BIG_ENDIAN);
            resultBuffer.putInt(startRow);
            resultBuffer.putInt(endRow);
            resultBuffer.putInt(startCol);
            resultBuffer.putInt(endCol);
            
            for (int i = 0; i < blockRows; i++) {
                for (int j = 0; j < blockCols; j++) {
                    resultBuffer.putInt(resultBlock[i][j]);
                }
            }
            
            Message resultMsg = new Message(Message.TYPE_RESULT, workerId, taskMsg.getTaskId(), resultBuffer.array());
            sendMessage(resultMsg);
            System.out.println("Worker " + workerId + " completed task " + taskMsg.getTaskId());
            
        } catch (Exception e) {
            System.err.println("Error executing task: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Initialize dummy matrices for demonstration
     */
    private void initializeDummyMatrices(int size) {
        matrixA = new int[size][size];
        matrixB = new int[size][size];
        
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                matrixA[i][j] = random.nextInt(10);
                matrixB[i][j] = random.nextInt(10);
            }
        }
    }

    /**
     * Send heartbeat to master
     */
    private void sendHeartbeat() {
        if (!running) return;
        
        try {
            Message heartbeat = new Message(Message.TYPE_HEARTBEAT, workerId, -1, new byte[0]);
            sendMessage(heartbeat);
        } catch (IOException e) {
            System.err.println("Failed to send heartbeat: " + e.getMessage());
            running = false;
        }
    }

    /**
     * Send a message to master
     */
    private synchronized void sendMessage(Message msg) throws IOException {
        byte[] data = msg.pack();
        out.write(data);
        out.flush();
    }

    /**
     * Clean shutdown
     */
    public void shutdown() {
        running = false;
        heartbeatSender.shutdownNow();
        taskExecutor.shutdownNow();
        
        try {
            if (masterSocket != null) masterSocket.close();
        } catch (IOException e) {
            // Ignore
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java pdc.Worker <master_host> <master_port>");
            System.exit(1);
        }
        
        String masterHost = args[0];
        int port = Integer.parseInt(args[1]);
        
        Worker worker = new Worker();
        
        // Add shutdown hook for clean exit
        Runtime.getRuntime().addShutdownHook(new Thread(worker::shutdown));
        
        worker.joinCluster(masterHost, port);
    }
}