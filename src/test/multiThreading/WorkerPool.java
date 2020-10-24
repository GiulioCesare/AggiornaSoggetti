package test.multiThreading;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class WorkerPool {
	
    public static void main(String args[]) throws InterruptedException{
    	final int CORE_POOL_SIZE = 4;
    	final int MAXIMUM_POOL_SIZE = 4;
    	final int KEEP_ALIVE_SECONDS = 5;  
    	final int MAXIMUM_QUEUE_SIZE = MAXIMUM_POOL_SIZE;
    	final int EXECUTE_WAIT_SECONDS = 1; // time to wait to see if the queue has freed up some space  
//    	final int MONITOR_WAIT_MILLISECONDS = 3*1000;
    	final int MONITOR_WAIT_SECONDS = 5;
    	
    	//RejectedExecutionHandler implementation
        RejectedExecutionHandlerImpl rejectionHandler = new RejectedExecutionHandlerImpl();
        //Get the ThreadFactory implementation to use
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        //creating the ThreadPoolExecutor
        ThreadPoolExecutor executorPool = new ThreadPoolExecutor(CORE_POOL_SIZE, MAXIMUM_POOL_SIZE, KEEP_ALIVE_SECONDS, 
        		TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(MAXIMUM_QUEUE_SIZE), threadFactory, rejectionHandler);
        
        //start the monitoring thread
//        MyMonitorThread monitor = new MyMonitorThread(executorPool, MONITOR_WAIT_SECONDS);
//        Thread monitorThread = new Thread(monitor);
//        monitorThread.start();
        
        
        //submit work to the thread pool
        int queueRemainingCapacity;
        for(int i=0; i<10; i++){

        	// Can we submit another task without being rejected?
            while (executorPool.getQueue().remainingCapacity() == 0) // is the task queue full? 
            { // wait till we have a free slot for a new task
                try {
                    Thread.sleep(EXECUTE_WAIT_SECONDS*1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            	
            }
//            queueRemainingCapacity = executorPool.getQueue().remainingCapacity();
//            System.out.println( String.format("[main loop] queue remaining capacity %d", queueRemainingCapacity));

            executorPool.execute(new WorkerRunnable("cmd"+i));
            
            
        }
        
//        System.out.println("sleep 30 secs");
//        Thread.sleep(30000);

        while (executorPool.getActiveCount() != 0) 
        { 
            try {
                System.out.println("waiting to shut down the pool");
                Thread.sleep(1*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        	
        }
        
        
        System.out.println("shutting down the pool");
        executorPool.shutdown();

        //shut down the monitor thread
//        Thread.sleep(5000);
//        monitor.shutdown();
        
    }
}