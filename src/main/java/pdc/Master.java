package pdc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The Master acts as the Coordinator in a distributed cluster.
 */
public class Master {
    private final ExecutorService workerHandlerPool = Executors.newCachedThreadPool();
    private final ScheduledExecutorService heartbeatChecker = Executors.newSingleThreadScheduledExecutor();
    
    private final Map<String, WorkerInfo> workers = new ConcurrentHashMap<>();
    private final BlockingQueue<Task> pendingTasks = new LinkedBlockingQueue<>();
    private final Map<Integer, TaskResult> completedTasks = new ConcurrentHashMap<>();
    private final Map<Integer, Task> assignedTasks = new ConcurrentHashMap<>();
    
    private int matrixSize = 0;
    private int[][] resultMatrix = null;
    private final AtomicInteger totalTasks = new AtomicInteger(0);
    
    private static final long HEARTBEAT_TIMEOUT = 5000;
    private ServerSocket serverSocket;
    private boolean running = true;

    public Object coordinate(String operation, int[][] data, int workerCount) {
        if (!"BLOCK_MULTIPLY".equals(operation)) {
            throw new IllegalArgumentException("Unsupported operation: " + operation);
        }
        
        // Start health checker
        heartbeatChecker.scheduleAtFixedRate(this::checkWorkerHealth, 5, 2, TimeUnit.SECONDS);
        
        // Create tasks from data
        createTasks(data, 10);
        
        // Wait for all tasks to complete
        while (completedTasks.size() < totalTasks.get()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        
        assembleResultMatrix(data.length, data[0].length);
        return resultMatrix;
    }

    public void listen(int port) throws IOException {
        serverSocket = new ServerSocket(port);
        System.out.println("Master listening on port " + port);
        
        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                System.out.println("New connection from " + clientSocket.getInetAddress());
                
                WorkerHandler handler = new WorkerHandler(clientSocket);
                workerHandlerPool.submit(handler);
            } catch (IOException e) {
                if (running) {
                    System.err.println("Error accepting connection: " + e.getMessage());
                }
            }
        }
    }

    private void createTasks(int[][] matrix, int blockSize) {
        int n = matrix.length;
        matrixSize = n;
        int taskId = 0;
        
        for (int i = 0; i < n; i += blockSize) {
            for (int j = 0; j < n; j += blockSize) {
                int rowEnd = Math.min(i + blockSize, n);
                int colEnd = Math.min(j + blockSize, n);
                
                ByteBuffer buffer = ByteBuffer.allocate(16);
                buffer.order(ByteOrder.BIG_ENDIAN);
                buffer.putInt(i);
                buffer.putInt(rowEnd);
                buffer.putInt(j);
                buffer.putInt(colEnd);
                
                Task task = new Task(taskId, buffer.array(), i, j, rowEnd - i, colEnd - j);
                pendingTasks.offer(task);
                taskId++;
            }
        }
        
        totalTasks.set(taskId);
        System.out.println("Created " + taskId + " tasks");
    }

    private void checkWorkerHealth() {
        long now = System.currentTimeMillis();
        
        for (Map.Entry<String, WorkerInfo> entry : workers.entrySet()) {
            WorkerInfo info = entry.getValue();
            
            if (now - info.lastHeartbeat > HEARTBEAT_TIMEOUT) {
                System.out.println("Worker " + entry.getKey() + " timed out!");
                
                // Reassign unfinished tasks
                synchronized(info.assignedTaskIds) {
                    for (Integer taskId : info.assignedTaskIds) {
                        Task task = assignedTasks.remove(taskId);
                        if (task != null) {
                            System.out.println("Reassigning task " + taskId);
                            pendingTasks.offer(task);
                        }
                    }
                    info.assignedTaskIds.clear();
                }
                
                workers.remove(entry.getKey());
                
                try {
                    if (info.socket != null) info.socket.close();
                } catch (IOException e) {
                    // Ignore
                }
            }
        }
    }

    private void assembleResultMatrix(int rows, int cols) {
        resultMatrix = new int[rows][cols];
        
        for (Map.Entry<Integer, TaskResult> entry : completedTasks.entrySet()) {
            TaskResult result = entry.getValue();
            
            ByteBuffer buffer = ByteBuffer.wrap(result.data);
            buffer.order(ByteOrder.BIG_ENDIAN);
            int[][] block = new int[result.rows][result.cols];
            
            for (int i = 0; i < result.rows; i++) {
                for (int j = 0; j < result.cols; j++) {
                    block[i][j] = buffer.getInt();
                }
            }
            
            for (int i = 0; i < result.rows; i++) {
                for (int j = 0; j < result.cols; j++) {
                    resultMatrix[result.startRow + i][result.startCol + j] = block[i][j];
                }
            }
        }
        
        System.out.println("Final matrix assembled");
    }

    public void reconcileState() {
        checkWorkerHealth();
        System.out.println("Cluster state: " + workers.size() + " workers, " + 
                          pendingTasks.size() + " pending, " + 
                          completedTasks.size() + " completed");
    }

    private static class WorkerInfo {
        final Socket socket;
        final OutputStream out;
        final InputStream in;
        volatile long lastHeartbeat;
        final List<Integer> assignedTaskIds = Collections.synchronizedList(new ArrayList<>());
        
        WorkerInfo(Socket socket) throws IOException {
            this.socket = socket;
            this.out = socket.getOutputStream();
            this.in = socket.getInputStream();
            this.lastHeartbeat = System.currentTimeMillis();
        }
        
        synchronized void sendMessage(Message msg) throws IOException {
            byte[] data = msg.pack();
            out.write(data);
            out.flush();
        }
    }

    private static class Task {
        final int taskId;
        final byte[] data;
        final int startRow, startCol, rows, cols;
        
        Task(int taskId, byte[] data, int startRow, int startCol, int rows, int cols) {
            this.taskId = taskId;
            this.data = data;
            this.startRow = startRow;
            this.startCol = startCol;
            this.rows = rows;
            this.cols = cols;
        }
    }

    private static class TaskResult {
        final int taskId;
        final byte[] data;
        final int startRow, startCol, rows, cols;
        
        TaskResult(int taskId, byte[] data, int startRow, int startCol, int rows, int cols) {
            this.taskId = taskId;
            this.data = data;
            this.startRow = startRow;
            this.startCol = startCol;
            this.rows = rows;
            this.cols = cols;
        }
    }

    private class WorkerHandler implements Runnable {
        private final Socket socket;
        private WorkerInfo workerInfo;
        private String workerId;
        
        WorkerHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try {
                workerInfo = new WorkerInfo(socket);
                workerId = socket.getInetAddress().getHostAddress() + ":" + socket.getPort();
                
                Message msg = Message.readFromStream(workerInfo.in);
                if (msg != null && msg.getType() == Message.TYPE_REGISTER) {
                    workers.put(workerId, workerInfo);
                    System.out.println("Worker " + workerId + " registered: " + msg.getSender());
                    
                    Message ack = new Message(Message.TYPE_ACK, "master", -1, new byte[0]);
                    workerInfo.sendMessage(ack);
                    
                    // Assign initial task if available
                    assignNextTask();
                    
                    while (true) {
                        Message message = Message.readFromStream(workerInfo.in);
                        if (message == null) break;
                        
                        handleMessage(message);
                    }
                }
            } catch (IOException e) {
                System.err.println("Error with worker " + workerId + ": " + e.getMessage());
            } finally {
                // Cleanup on disconnect
                if (workerId != null) {
                    workers.remove(workerId);
                    
                    if (workerInfo != null) {
                        synchronized(workerInfo.assignedTaskIds) {
                            for (Integer taskId : workerInfo.assignedTaskIds) {
                                Task task = assignedTasks.remove(taskId);
                                if (task != null) {
                                    pendingTasks.offer(task);
                                }
                            }
                        }
                    }
                }
                
                try {
                    socket.close();
                } catch (IOException e) {
                    // Ignore
                }
            }
        }

        private void handleMessage(Message message) throws IOException {
            switch (message.getType()) {
                case Message.TYPE_HEARTBEAT:
                    workerInfo.lastHeartbeat = System.currentTimeMillis();
                    break;
                    
                case Message.TYPE_RESULT:
                    int taskId = message.getTaskId();
                    System.out.println("Received result for task " + taskId + " from " + workerId);
                    
                    synchronized(workerInfo.assignedTaskIds) {
                        workerInfo.assignedTaskIds.remove((Integer) taskId);
                    }
                    assignedTasks.remove(taskId);
                    
                    ByteBuffer coordBuffer = ByteBuffer.wrap(message.getPayload());
                    coordBuffer.order(ByteOrder.BIG_ENDIAN);
                    int startRow = coordBuffer.getInt();
                    int endRow = coordBuffer.getInt();
                    int startCol = coordBuffer.getInt();
                    int endCol = coordBuffer.getInt();
                    
                    byte[] resultData = new byte[message.getPayload().length - 16];
                    System.arraycopy(message.getPayload(), 16, resultData, 0, resultData.length);
                    
                    TaskResult result = new TaskResult(
                        taskId, resultData, startRow, startCol, 
                        endRow - startRow, endCol - startCol
                    );
                    completedTasks.put(taskId, result);
                    
                    assignNextTask();
                    break;
                    
                default:
                    System.out.println("Unknown message type: " + message.getType());
            }
        }

        private void assignNextTask() throws IOException {
            Task task = pendingTasks.poll();
            if (task != null) {
                Message taskMsg = new Message(Message.TYPE_TASK, "master", task.taskId, task.data);
                workerInfo.sendMessage(taskMsg);
                workerInfo.assignedTaskIds.add(task.taskId);
                assignedTasks.put(task.taskId, task);
                System.out.println("Assigned task " + task.taskId + " to " + workerId);
            }
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: java pdc.Master <port>");
            System.exit(1);
        }
        
        int port = Integer.parseInt(args[0]);
        Master master = new Master();
        master.listen(port);
    }
}