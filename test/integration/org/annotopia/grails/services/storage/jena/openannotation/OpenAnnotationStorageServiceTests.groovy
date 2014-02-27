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
package org.annotopia.grails.services.storage.jena.openannotation

import groovy.util.GroovyTestCase;

import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.RDFLanguages
import org.junit.Before;

import com.hp.hpl.jena.query.Dataset
import com.hp.hpl.jena.query.DatasetFactory

/**
 * Testing of basic Annotation storage functionalities.
 *
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class OpenAnnotationStorageServiceTests extends GroovyTestCase {
	
	def grailsApplication = new org.codehaus.groovy.grails.commons.DefaultGrailsApplication()
	
	def jenaVirtuosoStoreService;
	def openAnnotationStorageService;

	@Before
	public void setUp() throws Exception {
	}
	
	/**
	 * Loads an Open Annotation graph from a JSON-LD String, 
	 * saves the data, verifies and removes.
	 */
	public void testSaveAnnotationDataset() {
		String currentDir = new File(".").getCanonicalPath()
		String fileContent = new File(currentDir + '/tests/oa-test-example-no-graph.json').text
		
		Dataset workingDataset = DatasetFactory.createMem();
		RDFDataMgr.read(workingDataset, new ByteArrayInputStream(fileContent.toString().getBytes("UTF-8")), RDFLanguages.JSONLD);
		
		Set<String> graphs = new HashSet<String>();
		Dataset savedDataset = openAnnotationStorageService.saveAnnotationDataset(grailsApplication.config.annotopia.storage.testing.apiKey, System.currentTimeMillis(), workingDataset);
		savedDataset.listNames().each() { graphs.add(it); }
		
		graphs.each { graph ->
			// Check if graph exists in the repository
			assert "Graph creation", jenaVirtuosoStoreService.doesGraphExists(grailsApplication.config.annotopia.storage.testing.apiKey, graph);
			// Drop the graph
			jenaVirtuosoStoreService.dropGraph(grailsApplication.config.annotopia.storage.testing.apiKey, graph)
			// Check if the graph has been deleted
			assertFalse "Graph deletion", jenaVirtuosoStoreService.doesGraphExists(grailsApplication.config.annotopia.storage.testing.apiKey, graph);
		}
	}
	
	/**
	 * Loads an Open Annotation graph from a JSON-LD String,
	 * saves the data, verifies and removes.
	 */
	public void testUpdateAnnotationDataset() {
		String currentDir = new File(".").getCanonicalPath()
		String fileContent = new File(currentDir + '/tests/oa-test-example-no-graph.json').text
		
		Dataset workingDataset = DatasetFactory.createMem();
		RDFDataMgr.read(workingDataset, new ByteArrayInputStream(fileContent.toString().getBytes("UTF-8")), RDFLanguages.JSONLD);
		
		Set<String> graphs = new HashSet<String>();
		Dataset savedDataset = openAnnotationStorageService.saveAnnotationDataset(grailsApplication.config.annotopia.storage.testing.apiKey, System.currentTimeMillis(), workingDataset);
		savedDataset.listNames().each() { graphs.add(it); }
		
		graphs.each { graph ->
			// Check if graph exists in the repository
			assert "Graph creation", jenaVirtuosoStoreService.doesGraphExists(grailsApplication.config.annotopia.storage.testing.apiKey, graph);
		}
			
		openAnnotationStorageService.updateAnnotationDataset(grailsApplication.config.annotopia.storage.testing.apiKey, System.currentTimeMillis(), savedDataset);
		
		graphs.each { graph ->
			// Check if graph exists in the repository
			assert "Graph creation", jenaVirtuosoStoreService.doesGraphExists(grailsApplication.config.annotopia.storage.testing.apiKey, graph);
			// Drop the graph
			jenaVirtuosoStoreService.dropGraph(grailsApplication.config.annotopia.storage.testing.apiKey, graph)
			// Check if the graph has been deleted
			assertFalse "Graph deletion", jenaVirtuosoStoreService.doesGraphExists(grailsApplication.config.annotopia.storage.testing.apiKey, graph);
		}
	}
}
