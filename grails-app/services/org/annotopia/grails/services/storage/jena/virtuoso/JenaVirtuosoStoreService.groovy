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
package org.annotopia.grails.services.storage.jena.virtuoso

import grails.util.Environment

import org.annotopia.groovy.service.store.ITripleStore
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.RDFFormat

import virtuoso.jena.driver.VirtGraph
import virtuoso.jena.driver.VirtModel
import virtuoso.jena.driver.VirtuosoQueryExecution
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory
import virtuoso.jena.driver.VirtuosoUpdateFactory
import virtuoso.jena.driver.VirtuosoUpdateRequest

import com.hp.hpl.jena.query.Dataset
import com.hp.hpl.jena.query.DatasetFactory
import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.rdf.model.Model

/**
 * Basic storage services for Virtuoso Triple store.
 *
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class JenaVirtuosoStoreService implements ITripleStore {
	
	def grailsApplication
	
	// -----------------------------------------------------------------------
	//    STORE
	// -----------------------------------------------------------------------
	@Override
	public String store(String apiKey, File annotationFile) {		
		log.info '[' + apiKey + '] Storing file: ' + annotationFile.getName();	
		store(apiKey, annotationFile, null);
	}
	
	@Override
	public String store(String apiKey, File annotationFile, String baseUri) {
		log.info '[' + apiKey + '] Storing file: ' + annotationFile.getName() + ' with baseUri: ' + baseUri;		
		try {
			if(annotationFile == null || !annotationFile.exists()) {
				log.error "File not found: " + annotationFile;
				throw new IllegalArgumentException("File not found: " + annotationFile);
			}
			InputStream inputStream = new FileInputStream(annotationFile);
			storeGraphs(inputStream, baseUri);	
		} catch (Exception e) {
			System.out.println( e.getMessage());
		}
	}

	@Override
	public String store(String apiKey, String content) {
		log.info '[' + apiKey + '] Loading content: ' + content;	
		store(apiKey, content, null);
	}

	@Override
	public String store(String apiKey, String content, String baseUri) {
		log.info '[' + apiKey + '] Storing content with baseUri: ' + baseUri;
		try {
			if(content == null || content.isEmpty()) {
				log.error "Content not valid: " + content;
				throw new IllegalArgumentException("Content not valid: " + content);
			}				
			InputStream inputStream = new ByteArrayInputStream(content.getBytes());
			storeGraphs(apiKey, inputStream, baseUri);		
		} catch (Exception e) {
			System.out.println( e.getMessage());
		}
	}
	
	public storeDataset(String apiKey, Dataset dataset) {
		// Default graph management
		if(dataset.getDefaultModel()!=null && dataset.getDefaultModel().size()>0) {
			log.debug '[' + apiKey + '] Storing graph: * (default)'
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
			log.debug '[' + apiKey + '] Storing graph: ' + name
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
	
	private storeGraphs(String apiKey, InputStream inputStream, String baseUri) {
		//JenaJSONLD.init(); // Only needed once
		
		Dataset dataset = DatasetFactory.createMem();
		
		// Using the RIOT reader
		if(baseUri!=null && !baseUri.isEmpty()) RDFDataMgr.read(dataset, inputStream, baseUri, RDFFormat.JSONLD);
		else RDFDataMgr.read(dataset, inputStream, RDFFormat.JSONLD);
		printDebugData(dataset);
		
		// Default graph management
		if(dataset.getDefaultModel()!=null && dataset.getDefaultModel().size()>0) {
			log.debug '[' + apiKey + '] Storing graph: * (default)'
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
			log.debug '[' + apiKey + '] Storing graph: ' + name
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
			RDFDataMgr.write(System.out, data, RDFFormat.JSONLD);
			println '\n-----END-DEVELOPMENT-DEBUG-DATA-----';
		}
	}
	
	// -----------------------------------------------------------------------
	//    UPDATE
	// -----------------------------------------------------------------------
	@Override
	public String update(String apiKey, File annotationFile) {
		log.info '[' + apiKey + '] Updating from file: ' + annotationFile.getName();
		update(apiKey, annotationFile, null);
	}

	@Override
	public String update(String apiKey, File annotationFile, String baseUri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String update(String apiKey, String content) {
		log.info '[' + apiKey + '] Updating from content: ' + content;
		// store(apiKey, content, null);
	}

	@Override
	public String update(String apiKey, String content, String baseUri) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public updateDataset(String apiKey, Dataset dataset) {
		Iterator<String> names = dataset.listNames()
		while(names.hasNext()) {
			String name = names.next();
			log.debug '[' + apiKey + '] Updating graph: ' + name
			clearGraph(apiKey, name);
		}
		storeDataset(apiKey, dataset);
	}
	
	public updateGraphMetadata(String apiKey, Model graphMetadata, String graphUri, String metadataGraph) {
		removeAllTriples(apiKey, metadataGraph, graphUri);
	}

	// -----------------------------------------------------------------------
	//    RETRIEVE
	// -----------------------------------------------------------------------
	@Override
	public Dataset retrieveGraph(String apiKey, String graphUri) {
		log.info '[' + apiKey + '] Retrieving graph: ' + graphUri;
		
		VirtGraph set = new VirtGraph (graphUri,
			grailsApplication.config.annotopia.storage.triplestore.host,
			grailsApplication.config.annotopia.storage.triplestore.user,
			grailsApplication.config.annotopia.storage.triplestore.pass);
		
		String queryString = "CONSTRUCT { ?s ?p ?o . } FROM <" + graphUri + ">" + 
			" WHERE { ?s ?p ?o . }";
		log.trace '[' + apiKey + '] ' + queryString
		
//		String queryString2 = "CONSTRUCT { <"+graphUri+"> ?p ?o . } FROM <annotopia:graphs:provenance>" + 
//			" WHERE { <"+graphUri+"> ?p ?o .}";
//		log.trace '[' + apiKey + '] ' + queryString2
		
		try {
			//Model model = ModelFactory.createMemModelMaker().createModel(graphUri);
			VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (QueryFactory.create(queryString), set);	
			Model model = vqe.execConstruct();
			
			Dataset dataset = DatasetFactory.createMem();
			dataset.addNamedModel(graphUri, model);
			
//			VirtuosoQueryExecution vqe2 = VirtuosoQueryExecutionFactory.create (QueryFactory.create(queryString2), set);
//			Model model2 = vqe2.execConstruct();
//			dataset.setDefaultModel(model2);
			return dataset;
		} catch (Exception e) {
			log.error(e.getMessage());
			return null;
		}
	}
	
	@Override
	public Model retrieveGraphMetadata(String apiKey, String graphUri, String metadataGraphUri) {
		log.info '[' + apiKey + '] Retrieving graph metadata: ' + graphUri;
		
		VirtGraph set = new VirtGraph (graphUri,
			grailsApplication.config.annotopia.storage.triplestore.host,
			grailsApplication.config.annotopia.storage.triplestore.user,
			grailsApplication.config.annotopia.storage.triplestore.pass);
		
		String queryString = "CONSTRUCT { <"+graphUri+"> ?p ?o . } FROM <" + metadataGraphUri + ">" +
			" WHERE { <"+graphUri+"> ?p ?o .}";
		log.trace '[' + apiKey + '] ' + queryString
		
		try {			
			VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (QueryFactory.create(queryString), set);
			Model model = vqe.execConstruct();
			return model;
		} catch (Exception e) {
			log.error(e.getMessage());
			return null;
		}
	}
	
	@Override
	public boolean doesGraphExists(String apiKey, String graphUri) {
		log.info '[' + apiKey + '] Checking graph existance: ' + graphUri;

		// The ASK method seems not working so I am using a more elaborate
		// methodology
		
		/*
		VirtGraph set = new VirtGraph (
			grailsApplication.config.annotopia.storage.triplestore.host,
			grailsApplication.config.annotopia.storage.triplestore.user,
			grailsApplication.config.annotopia.storage.triplestore.pass);
		
		String query = "ASK { GRAPH <" + graphUri + "> { ?s ?p ?o . } }";
		log.info 'Query: ' + query;
		
		Query sparql = QueryFactory.create(query);
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (sparql, set);
		boolean result =  vqe.execAsk();
		log.info 'Result: ' + result;
		return	result
		*/
		
		Dataset dataset = retrieveGraph(apiKey, graphUri);
		boolean existenceFlag = false;
		Iterator<String> graphNames = dataset.listNames()
		while(graphNames.hasNext()) {
			String graphName = graphNames.next();
			if(dataset.getNamedModel(graphName).size()>0)
				existenceFlag = true;
		}
		return existenceFlag;
	}
	
	// -----------------------------------------------------------------------
	//    DROP/CLEAR
	// -----------------------------------------------------------------------
	
	public void removeAllTriples(String apiKey, String graphUri, String subjectUri) {
		log.info '[' + apiKey + '] Removing all triples with subject: ' + subjectUri + ' from: ' + graphUri;
		
		try {
			String queryString = "DELETE DATA FROM <" + graphUri + ">" +
				" WHERE { <"+subjectUri+"> ?p ?o .}";
			log.trace '[' + apiKey + '] ' + queryString
			
			VirtuosoUpdateRequest vur = VirtuosoUpdateFactory.create(str, graph);
			vur.exec();
		} catch (Exception e) {
			println e.getMessage();
		}
	}
	
	@Override
	public boolean dropGraph(String apiKey, String graphUri) {
		log.info '[' + apiKey + '] Removing graph: ' + graphUri;
		
		try {
			VirtGraph graph = new VirtGraph (
				grailsApplication.config.annotopia.storage.triplestore.host,
				grailsApplication.config.annotopia.storage.triplestore.user,
				grailsApplication.config.annotopia.storage.triplestore.pass);
			String str = "DROP SILENT GRAPH <" + graphUri + ">";
			VirtuosoUpdateRequest vur = VirtuosoUpdateFactory.create(str, graph);
			vur.exec();
		} catch (Exception e) {
			println e.getMessage();
		}
	}
	
	@Override
	public boolean clearGraph(String apiKey, String graphUri) {
		log.info '[' + apiKey + '] Clearing graph: ' + graphUri;
		
		try {
			VirtGraph graph = new VirtGraph (
				grailsApplication.config.annotopia.storage.triplestore.host,
				grailsApplication.config.annotopia.storage.triplestore.user,
				grailsApplication.config.annotopia.storage.triplestore.pass);
			String str = "CLEAR GRAPH <" + graphUri + ">";
			VirtuosoUpdateRequest vur = VirtuosoUpdateFactory.create(str, graph);
			vur.exec();
		} catch (Exception e) {
			println e.getMessage();
		}
	}
}
