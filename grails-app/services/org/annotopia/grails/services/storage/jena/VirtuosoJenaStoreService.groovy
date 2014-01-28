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
package org.annotopia.grails.services.storage.jena

import grails.util.Environment

import org.annotopia.groovy.service.store.ITripleStore
import org.apache.jena.riot.RDFDataMgr

import virtuoso.jena.driver.VirtGraph
import virtuoso.jena.driver.VirtModel

import com.github.jsonldjava.jena.JenaJSONLD
import com.hp.hpl.jena.query.Dataset
import com.hp.hpl.jena.query.DatasetFactory
import com.hp.hpl.jena.rdf.model.Model


/**
 * This is the service that allows to connect to the Virtuoso
 * triple store via Jena APIs.
 * 
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class VirtuosoJenaStoreService implements ITripleStore {

	def grailsApplication
	
	@Override
	public String store(File annotationFile) {
		
		log.info 'Loading file: ' + annotationFile.getName();	
		store(annotationFile, null);
	}
	
	@Override
	public String store(File annotationFile, String baseUri) {
		log.info 'Loading file: ' + annotationFile.getName() + ' with baseUri: ' + baseUri;
		
		try {
			try {
				if(annotationFile == null || !annotationFile.exists()) {
					log.error "File not found: " + annotationFile;
					throw new IllegalArgumentException("File not found: " + annotationFile);
				}
				InputStream inputStream = new FileInputStream(annotationFile);
				storeGraphs(inputStream, baseUri);	
			} finally {

			}
		} catch (Exception e) {
			System.out.println( e.getMessage());
		}
	}

	@Override
	public String store(String content) {
		log.info 'Loading content: ' + content;	
		store(content, null);
	}

	@Override
	public String store(String content, String baseUri) {
		log.info 'Loading content with baseUri: ' + baseUri;
		
		try {
			try {
				if(content == null || content.isEmpty()) {
					log.error "Content not valid: " + content;
					throw new IllegalArgumentException("Content not valid: " + content);
				}
				
				InputStream inputStream = new ByteArrayInputStream(content.getBytes());
				storeGraphs(inputStream, baseUri);		
			} finally {

			}
		} catch (Exception e) {
			System.out.println( e.getMessage());
		}
	}
	
	private storeGraphs(InputStream inputStream, String baseUri) {
		JenaJSONLD.init(); // Only needed once
		
		Dataset dataset = DatasetFactory.createMem();
		
		// Using the RIOT reader
		if(baseUri!=null && !baseUri.isEmpty()) RDFDataMgr.read(dataset, inputStream, baseUri, JenaJSONLD.JSONLD);
		else RDFDataMgr.read(dataset, inputStream, JenaJSONLD.JSONLD);
		printDebugData(dataset);
		
		// Default graph management
		if(dataset.getDefaultModel()!=null && dataset.getDefaultModel().size()>0) {
			log.debug "graph: * (default)"
			log.debug grailsApplication.config.annotopia.storage.triplestore.host
			VirtGraph virtGraph = new VirtGraph (
				grailsApplication.config.annotopia.storage.triplestore.host,
				grailsApplication.config.annotopia.storage.triplestore.user,
				grailsApplication.config.annotopia.storage.triplestore.pass);
			VirtModel virtModel = new VirtModel(virtGraph);
			virtModel.add(dataset.getDefaultModel())
			printDebugData(dataset.getDefaultModel());
		}
		
		Iterator<String> names = dataset.listNames()
		while(names.hasNext()) {
			String name = names.next();
			log.debug "graph: " + name
			Model model = dataset.getNamedModel(name)
			VirtGraph virtGraph = new VirtGraph (name,
				grailsApplication.config.annotopia.storage.triplestore.host,
				grailsApplication.config.annotopia.storage.triplestore.user,
				grailsApplication.config.annotopia.storage.triplestore.pass);
			VirtModel virtModel = new VirtModel(virtGraph);
			virtModel.add(model);
			printDebugData(model);
		}
	}
	
	private printDebugData(def data) {
		if (Environment.current == Environment.DEVELOPMENT && 
				Boolean.parseBoolean(grailsApplication.config.annotopia.debug.storage.environment.trace.data)) {
			println '-----START-DEVELOPMENT-DEBUG-DATA-----';
			RDFDataMgr.write(System.out, data, JenaJSONLD.JSONLD);
			println '\n-----END-DEVELOPMENT-DEBUG-DATA-----';
		}
	}
}
