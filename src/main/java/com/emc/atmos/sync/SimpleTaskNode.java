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
package com.emc.atmos.sync;

import java.util.Set;

public class SimpleTaskNode extends TaskNode {
	public String message;
	
	public SimpleTaskNode( String message, Set<TaskNode> parents ) {
		super( parents );
		this.message = message;
	}
	
	public SimpleTaskNode( String message ) {
		this( message, null );
	}

	@Override
	public TaskResult execute() throws Exception {
		System.out.println( Thread.currentThread() + ": " + message );
		
		return new TaskResult(true);
	}

	@Override
	public String toString() {
		return "SimpleTaskNode: " + message;
	}
	
	

}
