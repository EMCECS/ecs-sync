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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;

import com.emc.esu.api.EsuException;

/**
 * @author cwikj
 * 
 */
public class RetryPlugin extends SyncPlugin {
	private static final Logger l4j = Logger.getLogger(RetryPlugin.class);
	private static final String RETRY_OPTION = "retries";
	private static final String RETRY_DESC = "Activates the retry plugin and " +
			"sets the max number of retries";
	private static final String RETRY_OPT_DESC = "max-retries";

    // timed operations
    private static final String OPERATION_RETRY = "RetryAttempt";

	private int maxRetries = 3;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.emc.atmos.sync.plugins.SyncPlugin#filter(com.emc.atmos.sync.plugins
	 * .SyncObject)
	 */
	@Override
	public void filter(SyncObject obj) {
		Throwable lastError = null;
		int retryCount = 0;
		while(retryCount < maxRetries) {
			try {
				getNext().filter(obj);

                if (retryCount > 0) { // we retried
                    timeOperationStart(OPERATION_RETRY + "::" + lastError);
                    timeOperationComplete(OPERATION_RETRY + "::" + lastError);
                }

				return;
			} catch(Throwable t) {
                if (retryCount > 0) { // the last retry failed
                    timeOperationStart(OPERATION_RETRY + "::" + lastError);
                    timeOperationFailed(OPERATION_RETRY + "::" + lastError);
                }
                lastError = t;
                while (t.getCause() != null) t = t.getCause();

                if (t instanceof EsuException) {
                    EsuException e = (EsuException) t;

                    // By default, don't retry 400s (Bad Request)
                    if(e.getHttpCode() >= 400 && e.getHttpCode() <= 499) {
                        LogMF.warn(l4j, "Not retrying error {0}", e.getAtmosCode());
                        throw e;
                    }

                    // For Atmos 1040 (server too busy), wait a few seconds
                    if(e.getAtmosCode() == 1040) {
                        l4j.info("Atmos code 1040 (too busy) for obj, sleeping 2 sec");
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException e1) {
                        }
                    }
                }

                retryCount++;
				LogMF.warn( l4j, "Retry #{0}, Error: {1}", retryCount, t );
			}
		}
		
		throw new RuntimeException("Retry failed: " + lastError, lastError);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getOptions()
	 */
	@SuppressWarnings("static-access")
	@Override
	public Options getOptions() {
		Options opts = new Options();
		
		opts.addOption(OptionBuilder.withLongOpt(RETRY_OPTION)
				.withDescription(RETRY_DESC).hasArg()
				.withArgName(RETRY_OPT_DESC).create());
		
		return opts;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.emc.atmos.sync.plugins.SyncPlugin#parseOptions(org.apache.commons
	 * .cli.CommandLine)
	 */
	@Override
	public boolean parseOptions(CommandLine line) {
		if(line.hasOption(RETRY_OPTION)) {
			setMaxRetries(Integer.parseInt(line.getOptionValue(RETRY_OPTION)));
			LogMF.info(l4j, "Operations will be retried up to {0} times.", 
					maxRetries);
			return true;
		} else {
			return false;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.emc.atmos.sync.plugins.SyncPlugin#validateChain(com.emc.atmos.sync
	 * .plugins.SyncPlugin)
	 */
	@Override
	public void validateChain(SyncPlugin first) {
		while(first != null) {
			if(first.getNext() == null) {
				// Dest
				if(!(first instanceof AtmosDestination || first instanceof DummyDestination)) {
					throw new RuntimeException("The RetryPlugin can only be " +
							"used with an Atmos or dummy destination");
				}
			}
			first = first.getNext();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getName()
	 */
	@Override
	public String getName() {
		return "Retry";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getDocumentation()
	 */
	@Override
	public String getDocumentation() {
		return "Allows for retrying operations on an Atmos destination.  On " +
				"any non-client error (e.g. HTTP 5XXs), the operation will " +
				"be retried.  In the case of Atmos code 1040 (server busy), " +
				"the plugin will pause the thread for 5 seconds before it " +
				"retries.  Note that if you are using multiple plugins " +
				"between the source and destination, you should use a Spring " +
				"configuration since when using plugins from the command " + 
				"line you cannot guarantee execution order.";
	}

	/**
	 * @return the maxRetries
	 */
	public int getMaxRetries() {
		return maxRetries;
	}

	/**
	 * @param maxRetries
	 *            the maxRetries to set
	 */
	public void setMaxRetries(int maxRetries) {
		this.maxRetries = maxRetries;
	}

}
