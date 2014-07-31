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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;

/**
 * This class implements a base for multithreaded sources. This implements a
 * graph-based algorithm for computing task dependencies. Operations to execute
 * should be implemented as TaskNode objects. These objects may have zero or
 * more TaskNodes as parent tasks. The TaskNode will not execute until all of
 * its parent tasks have completed. The general procedure for using this class
 * is:
 * <ol>
 * <li>In your run() method, call initQueue().
 * <li>Create one or more TaskNode object. Add these to the "graph" property.
 * <li>Start execution by calling runQueue(). This will exit when all tasks have
 * completed.
 * </ol>
 * Note that tasks can in turn spawn child tasks. Also, when processing a "flat"
 * list of objects, it may be needed to throttle input to keep the graph to a
 * reasonable size (e.g. 10x - 100x threadCount).
 * 
 * @see AtmosSource#readOIDs for a sample of throttling input.
 * @author cwikj
 * @see com.emc.atmos.sync.TaskNode
 */
public abstract class MultithreadedCrawlSource extends SourcePlugin {
	private static final Logger l4j = Logger
			.getLogger(MultithreadedCrawlSource.class);

	public static final String CRAWL_THREADS_OPT = "crawl-threads";
	public static final String CRAWL_THREADS_DESC = "Sets the number of threads to crawl directory structures. Default is 2.";

	protected boolean running;
	protected int transferThreadCount = 1;
	protected int crawlerThreadCount = 2;
	private LinkedBlockingQueue<Runnable> transferQueue;
	private CountingExecutor transferPool;
	private LinkedBlockingQueue<Runnable> crawlerQueue;
	private CountingExecutor crawlerPool;
	protected long start;
	private Set<SyncObject> failedItems;

	private long byteCount;
	private int completedCount;
	private int failedCount;

	private AtomicLong remainingTasks = new AtomicLong();

	private boolean rememberFailed = true;

	@SuppressWarnings("static-access")
	protected void addOptions(Options opts) {
		opts.addOption(OptionBuilder.withLongOpt(CRAWL_THREADS_OPT)
				.withDescription(CRAWL_THREADS_DESC).hasArg().create());
	}

	/**
	 * Initializes the graph, the thread pool, and the task queue.
	 */
	protected void initQueue() {
		start = System.currentTimeMillis();
		transferQueue = new LinkedBlockingQueue<Runnable>(
				transferThreadCount * 100);
		transferPool = new CountingExecutor(transferThreadCount,
				transferThreadCount, 15, TimeUnit.SECONDS, transferQueue);
		failedItems = Collections.synchronizedSet(new HashSet<SyncObject>());
		crawlerQueue = new LinkedBlockingQueue<Runnable>();
		crawlerPool = new CountingExecutor(crawlerThreadCount,
				crawlerThreadCount, 15, TimeUnit.SECONDS, crawlerQueue);
	}

	/**
	 * Runs until both the transfer and crawler queues are empty and there are
	 * no running tasks.
	 */
	protected void runQueue() {
		long start = System.currentTimeMillis();
		while (running) {
			// Every minute print queue sizes
			if (System.currentTimeMillis() - start >= 60000) {
				LogMF.debug(
						l4j,
						"Remaining Tasks: {0}, transferQueue: {1}, "
								+ "activeTransfers: {2}, crawlerQueue: {3}, "
								+ "activeCrawls: {4}",
						new Object[] { remainingTasks.get(),
								transferQueue.size(),
								transferPool.getActiveCount(),
								crawlerQueue.size(),
								crawlerPool.getActiveCount() });
				LogMF.info(l4j, "Completed Tasks: {0}, Failed Tasks: {1}",
						completedCount, failedCount);
				start = System.currentTimeMillis();
			}
			if (remainingTasks.get() < 1) {
				l4j.info("All tasks complete.  Shutting down");
				running = false;
				transferPool.shutdown();
				crawlerPool.shutdown();
				return;
			} else {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		if (!running) {
			// We were terminated early
			transferPool.shutdownNow();
			crawlerPool.shutdownNow();
		}

	}

	public void submitCrawlTask(Runnable task) {
		submitTask(crawlerPool, task);
	}

	public void submitTransferTask(Runnable task) {
		submitTask(transferPool, task);
	}

	private void submitTask(CountingExecutor pool, Runnable task) {
		remainingTasks.incrementAndGet();
		pool.blockingSubmit(task);
	}

	/**
	 * Call this method from your TaskNode on success. It will update the
	 * internal statistics for printing the summary at the end of execution.
	 * 
	 * @param obj
	 *            the SyncObject that has completed successfully.
	 */
	public synchronized void complete(SyncObject obj) {
		completedCount++;
		byteCount += obj.getBytesRead();
	}

	/**
	 * Call this method from your TaskNode on failure. It will update the
	 * internal statistics for printing the summary at the end of execution.
	 * 
	 * @param obj
	 *            the object that has failed
	 * @param t
	 *            the Exception that caused the failure.
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
		if (rememberFailed) {
			failedItems.add(obj);
		}
	}

	@Override
	public boolean parseOptions(CommandLine line) {
		if (line.hasOption(CommonOptions.SOURCE_THREADS_OPTION)) {
			transferThreadCount = Integer.parseInt(line
					.getOptionValue(CommonOptions.SOURCE_THREADS_OPTION));
		}
		if (line.hasOption(CRAWL_THREADS_OPT)) {
			crawlerThreadCount = Integer.parseInt(line
					.getOptionValue(CRAWL_THREADS_OPT));
		}
		return false;
	}

	@Override
	public void printStats() {
		long end = System.currentTimeMillis();
		long secs = ((end - start) / 1000);
		if (secs == 0) {
			secs = 1;
		}

		long rate = byteCount / secs;
		double objrate = (double) completedCount / secs;
		System.out.println(MessageFormat.format(
				"Transferred {0} bytes in {1} seconds ({2} bytes/s)",
				byteCount, secs, rate));
		System.out.println(MessageFormat.format(
				"Successful Files: {0} ({2,number,#.##}/s) Failed Files: {1}",
				completedCount, failedCount, objrate));
		System.out.println("Failed Files: " + failedItems);
	}

	/**
	 * Returns the number of threads to be used.
	 * 
	 * @return the threadCount
	 */
	public int getThreadCount() {
		return transferThreadCount;
	}

	/**
	 * Sets the number of threads to use to process items.
	 * 
	 * @param threadCount
	 *            the threadCount to set
	 */
	public void setThreadCount(int threadCount) {
		this.transferThreadCount = threadCount;
	}

	/**
	 * Returns the set of items that failed to transfer.
	 * 
	 * @return the failedItems
	 */
	public Set<SyncObject> getFailedItems() {
		return failedItems;
	}

	/**
	 * Returns the total number of bytes that were transferred to the
	 * destination, exclusive of metadata.
	 * 
	 * @return the byteCount
	 */
	public long getByteCount() {
		return byteCount;
	}

	/**
	 * Returns the number of items that completed successfully.
	 * 
	 * @return the completedCount
	 */
	public int getCompletedCount() {
		return completedCount;
	}

	/**
	 * Returns the number of items that failed.
	 * 
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
	 * @param rememberFailed
	 *            the rememberFailed to set
	 */
	public void setRememberFailed(boolean rememberFailed) {
		this.rememberFailed = rememberFailed;
	}

	class CountingExecutor extends ThreadPoolExecutor {
		private Object syncObject = new Object();

		public CountingExecutor(int corePoolSize, int maximumPoolSize,
				long keepAliveTime, TimeUnit unit,
				BlockingQueue<Runnable> workQueue) {
			super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
		}

		public void blockingSubmit(Runnable task) {
			while(true) {
				if(this.isShutdown() || this.isTerminated() || this.isTerminating()) {
					throw new RuntimeException("Pool is not accepting tasks");
				}

				synchronized(syncObject) {
					try {
						this.submit(task);
						return;
					} catch(Exception e) {
						LogMF.debug(l4j,
								"Pool full trying to submit {0}.  Current size {1}, "
										+ "reason: {2}.", task, this.getQueue().size(),
								e.getMessage());
					}
					if(this.isShutdown() || this.isTerminated() || this.isTerminating()) {
						throw new RuntimeException("Pool is not accepting tasks");
					}
					try {
						syncObject.wait();
					} catch (InterruptedException e) {
						// Ignore
					}
				}
			}
			
		}
		
		// A new task started.  The queue should be smaller.
		@Override
		protected void beforeExecute(Thread t, Runnable r) {
			synchronized(syncObject) {
				syncObject.notify();
			}
			super.beforeExecute(t, r);
		}

		@Override
		protected void afterExecute(Runnable r, Throwable t) {
			remainingTasks.decrementAndGet();
            super.afterExecute(r, t);
		}
		
	}

	/**
	 * @return the crawlerThreadCount
	 */
	public int getCrawlerThreadCount() {
		return crawlerThreadCount;
	}

	/**
	 * @param crawlerThreadCount
	 *            the crawlerThreadCount to set
	 */
	public void setCrawlerThreadCount(int crawlerThreadCount) {
		this.crawlerThreadCount = crawlerThreadCount;
	}
}
