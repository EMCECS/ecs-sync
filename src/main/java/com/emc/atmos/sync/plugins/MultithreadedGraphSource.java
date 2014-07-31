/*
 * Copyright 2013 EMC Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.emc.atmos.sync.plugins;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.BreadthFirstIterator;

import com.emc.atmos.sync.TaskNode;

/**
 * This class implements a base for multithreaded sources.  This implements
 * a graph-based algorithm for computing task dependencies.  Operations to
 * execute should be implemented as TaskNode objects.  These objects may have
 * zero or more TaskNodes as parent tasks.  The TaskNode will not execute until
 * all of its parent tasks have completed.  The general procedure for using
 * this class is:
 * <ol>
 * <li>In your run() method, call initQueue().
 * <li>Create one or more TaskNode object.  Add these to the "graph" property.
 * <li>Start execution by calling runQueue().  This will exit when all tasks
 * have completed.
 * </ol>
 * Note that tasks can in turn spawn child tasks.  Also, when processing a
 * "flat" list of objects, it may be needed to throttle input to keep the
 * graph to a reasonable size (e.g. 10x - 100x threadCount).
 * @see AtmosSource#readOIDs for a sample of throttling input.
 * @author cwikj
 * @see com.emc.atmos.sync.TaskNode
 */
public abstract class MultithreadedGraphSource extends SourcePlugin {
	private static final Logger l4j = Logger.getLogger(MultithreadedGraphSource.class);
	
	protected boolean running;
	protected int threadCount = 1;
	protected LinkedBlockingQueue<Runnable> queue;
	protected ThreadPoolExecutor pool;
	protected SimpleDirectedGraph<TaskNode, DefaultEdge> graph;
	protected long start;
	private Set<SyncObject> failedItems;

	private long byteCount;
	private int completedCount;
	private int failedCount;

	private boolean rememberFailed = true;

	/**
	 * Initializes the graph, the thread pool, and the task queue.
	 */
	protected void initQueue() {
		start = System.currentTimeMillis();
		queue = new LinkedBlockingQueue<Runnable>();
		pool = new ThreadPoolExecutor(threadCount, threadCount, 15, TimeUnit.SECONDS, queue);
		failedItems = Collections.synchronizedSet( new HashSet<SyncObject>() );
		
		graph = new SimpleDirectedGraph<TaskNode, DefaultEdge>(DefaultEdge.class);
	}
	
	/**
	 * Inspects the graph, looking for nodes with no parents.  These nodes
	 * are added to the task queue.  This method will run until the graph is
	 * empty.
	 */
	protected void runQueue() {
		// Start filling the pool with tasks
		while(running) {
			synchronized (graph) {
				if( graph.vertexSet().size() == 0 ) {
					// We're done
					pool.shutdownNow();
					break;
				}
				
				// Look for available unsubmitted tasks
				BreadthFirstIterator<TaskNode, DefaultEdge> i = new BreadthFirstIterator<TaskNode, DefaultEdge>(graph);
				while( i.hasNext() ) {
					TaskNode t = i.next();
					if( graph.inDegreeOf(t) == 0 && !t.isQueued() ) {
						t.setQueued(true);
						l4j.debug( "Submitting " + t );
						pool.submit(t);
					}
				}
			}
			
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// Ignore
			}
		}
		
		if(!running) {
			// We were terminated
			pool.shutdownNow();
		}

	}

	/**
	 * Call this method from your TaskNode on success.  It will update the
	 * internal statistics for printing the summary at the end of execution.
	 * @param obj the SyncObject that has completed successfully.
	 */
	public synchronized void complete(SyncObject obj) {
		completedCount++;
		byteCount += obj.getBytesRead();
	}

	/**
	 * Call this method from your TaskNode on failure.  It will update the 
	 * internal statistics for printing the summary at the end of execution.
	 * @param obj the object that has failed
	 * @param t the Exception that caused the failure.
	 */
	public synchronized void failed(SyncObject obj, Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null) cause = cause.getCause();
        LogMF.warn(l4j, "Object {0} failed: [{1}] {2}", obj, t, cause);
        StackTraceElement[] elements = cause.getStackTrace();
        for (int i=0; i < 5 && i < elements.length; i++) {
            LogMF.debug(l4j, "    at {0}", elements[i]);
        }
		failedCount++;
		if(rememberFailed ) {
			failedItems.add(obj);
		}
	}
	
	@Override
	public boolean parseOptions(CommandLine line) {
		if(line.hasOption(CommonOptions.SOURCE_THREADS_OPTION)) {
			threadCount = Integer.parseInt(
					line.getOptionValue(CommonOptions.SOURCE_THREADS_OPTION));
		}
		return false;
	}

	@Override
	public void printStats() {
		long end = System.currentTimeMillis();
		long secs = ((end-start)/1000);
		if( secs == 0 ) {
			secs = 1;
		}
		
		long rate = byteCount / secs;
		double objrate = (double)completedCount / secs;
		System.out.println(MessageFormat.format(
				"Transferred {0} bytes in {1} seconds ({2} bytes/s)", 
				byteCount, secs, rate) );
		System.out.println(MessageFormat.format(
				"Successful Files: {0} ({2,number,#.##}/s) Failed Files: {1}",
				completedCount, failedCount, objrate));
		System.out.println("Failed Files: " + failedItems );
	}

	/**
	 * Returns the number of threads to be used.
	 * @return the threadCount
	 */
	public int getThreadCount() {
		return threadCount;
	}

	/**
	 * Sets the number of threads to use to process items.
	 * @param threadCount the threadCount to set
	 */
	public void setThreadCount(int threadCount) {
		this.threadCount = threadCount;
	}

	/**
	 * Returns the set of items that failed to transfer.
	 * @return the failedItems
	 */
	public Set<SyncObject> getFailedItems() {
		return failedItems;
	}

	/**
	 * Returns the total number of bytes that were transferred to the 
	 * destination, exclusive of metadata.
	 * @return the byteCount
	 */
	public long getByteCount() {
		return byteCount;
	}

	/**
	 * Returns the number of items that completed successfully.
	 * @return the completedCount
	 */
	public int getCompletedCount() {
		return completedCount;
	}

	/**
	 * Returns the number of items that failed.
	 * @return the failedCount
	 */
	public int getFailedCount() {
		return failedCount;
	}

	/**
	 * @return the rememberFailed
	 */
	public boolean isRememberFailed() {
		return rememberFailed;
	}

	/**
	 * @param rememberFailed the rememberFailed to set
	 */
	public void setRememberFailed(boolean rememberFailed) {
		this.rememberFailed = rememberFailed;
	}
	

}
