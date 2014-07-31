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

/**
 * The base class for all source plugins.  Source plugins will be executed
 * and then "push" data down the chain.  So, instead of implementing your
 * logic in filter(), you implement it in run().
 * @author cwikj
 */
public abstract class SourcePlugin extends SyncPlugin implements Runnable {

	/**
	 * Throws an exception since SourcePlugins dont ever filter() objects.
	 */
	@Override
	public void filter(SyncObject obj) {
		throw new UnsupportedOperationException("Source Plugins don't filter");
	}

	/**
	 * Starts execution of the source.  This should determine the objects to
	 * transfer, create SyncObjects for them, and send them down the pluign
	 * chain.  Once all objects have completed transferring, this method
	 * should return.
	 */
	public abstract void run();
	
	/**
	 * When called, this should shut down the transfer operations as soon as
	 * possible.
	 */
	public abstract void terminate();
	
	/**
	 * Print statistics about the transfers to stdout.  This will be called
	 * by the main AtmosSync2 class after run() has completed to print a 
	 * summary to the console.
	 */
	public abstract void printStats();

}
