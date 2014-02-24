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
package org.annotopia.grails.services.storage.jena.openannotation;

import static org.junit.Assert.*
import grails.test.mixin.TestFor
import groovy.util.GroovyTestCase;

import org.codehaus.groovy.grails.web.json.JSONObject
import org.junit.After
import org.junit.Before

/**
 * Tests for the service that allows to connect to the Virtuoso
 * triple store via Jena APIs.
 *
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class OpenAnnotationValidationServiceTests extends GroovyTestCase {

	def jenaVirtuosoStoreService;
	def openAnnotationValidationService;
	
	@Before
	public void setUp() throws Exception {
	}
	
	/**
	 * Loads and validates a test file.
	 */
	public void testValidateJsonLdGraphFromTextualContent() {
		String currentDir = new File(".").getCanonicalPath()
		
		List<HashMap<String,Object>> results =
			openAnnotationValidationService.validate(new FileInputStream(new File(currentDir + '/tests/oa-test-example-with-graph.json')), "application/json");
			
		for(resultExternal in results) {
			//HashMap<String,Object> resultExternal = results.get(0);
			if(resultExternal.containsKey("result") && resultExternal.get("result")!=null) {
				
				assertEquals (resultExternal.get("total"), 55)
				assertEquals (resultExternal.get("model"), "http://example.org/tests/graph/001")
				assertEquals (resultExternal.get("warn"), 5)
				assertEquals (resultExternal.get("error"), 0)
				assertEquals (resultExternal.get("skip"), 41)
				assertEquals (resultExternal.get("pass"), 9)				
			}
		}
	}

	@After
	public void tearDown() throws Exception {
	}

}
