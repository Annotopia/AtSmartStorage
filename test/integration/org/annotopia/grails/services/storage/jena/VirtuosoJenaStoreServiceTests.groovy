/*
 * Copyright 2014 Massachusetts General Hospital
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.annotopia.grails.services.storage.jena;

import static org.junit.Assert.*
import grails.test.mixin.TestFor

import org.junit.After
import org.junit.Before

import com.hp.hpl.jena.query.Dataset

/**
 * Tests for the service that allows to connect to the Virtuoso
 * triple store via Jena APIs.
 *
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
@TestFor(org.annotopia.grails.services.storage.jena.VirtuosoJenaStoreService)
class VirtuosoJenaStoreServiceTests {

	def virtuosoJenaStoreService;
	
	@Before
	public void setUp() throws Exception {
	}
	
	/**
	 * Loads an Open Annotation graph from a JSON-LD file, verifies the 
	 * graph existence in the triple store, deletes the graph and
	 * verifies the not existence of the the graph.
	 */
	public void testStoreJsonLdGraphFromFile() {
		String currentDir = new File(".").getCanonicalPath()
		File file = new File(currentDir + '/tests/oa-test-example-with-graph.json')
		
		virtuosoJenaStoreService.store(file);
		assert "Graph creation", virtuosoJenaStoreService.doesGraphExists("http://example.org/tests/graph/001");
		
		virtuosoJenaStoreService.dropGraph("http://example.org/tests/graph/001")
		assertFalse "Graph deletion", virtuosoJenaStoreService.doesGraphExists("http://example.org/tests/graph/001");
	}
	
	/**
	 * Loads an Open Annotation graph from a JSON-LD String, verifies the
	 * graph existence in the triple store, deletes the graph and
	 * verifies the not existence of the the graph.
	 */
	public void testStoreJsonLdGraphFromTextualContent() {
		String currentDir = new File(".").getCanonicalPath()
		String fileContent = new File(currentDir + '/tests/oa-test-example-with-graph.json').text
		
		virtuosoJenaStoreService.store(fileContent);
		assert "Graph creation", virtuosoJenaStoreService.doesGraphExists("http://example.org/tests/graph/001");
		
		virtuosoJenaStoreService.dropGraph("http://example.org/tests/graph/001")
		assertFalse "Graph deletion", virtuosoJenaStoreService.doesGraphExists("http://example.org/tests/graph/001");
	}

	@After
	public void tearDown() throws Exception {
	}

}
