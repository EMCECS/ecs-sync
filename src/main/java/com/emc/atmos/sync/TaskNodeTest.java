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

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.BreadthFirstIterator;

public class TaskNodeTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SimpleDirectedGraph<TaskNode, DefaultEdge> graph = new SimpleDirectedGraph<TaskNode, DefaultEdge>(DefaultEdge.class);

		SimpleTaskNode dir1 = new SimpleTaskNode("mkdir /root");
		dir1.addToGraph(graph);
		
		SimpleTaskNode dir2 = new SimpleTaskNode("mkdir /root/dir2" );
		dir2.addParent(dir1);
		dir2.addToGraph(graph);
		
		SimpleTaskNode file1 = new SimpleTaskNode("upload /root/dir2/file1.txt");
		file1.addParent(dir2);
		file1.addToGraph(graph);
		
		SimpleTaskNode file2 = new SimpleTaskNode("upload /root/dir2/file2.txt");
		file2.addParent(dir2);
		file2.addToGraph(graph);
		
		SimpleTaskNode dir3 = new SimpleTaskNode( "mkdir /root/dir3" );
		dir3.addParent(dir1);
		dir3.addToGraph(graph);
		
		// Floating node
		SimpleTaskNode dir4 = new SimpleTaskNode( "mkdir /root2" );
		dir4.addToGraph(graph);
		
		// Separate tree
		SimpleTaskNode dir5 = new SimpleTaskNode("mkdir /root3" );
		dir5.addToGraph(graph);
		
		SimpleTaskNode file3 = new SimpleTaskNode("upload /root3/file3.txt");
		file3.addParent(dir5);
		file3.addToGraph(graph);
		
		LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
		ThreadPoolExecutor pool = new ThreadPoolExecutor(8, 8, 15, TimeUnit.SECONDS, queue);
		
		while(true) {
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
						System.out.println( "Submitting " + t );
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
		
		System.out.println( "Complete" );
		System.exit(0);

	}

}
