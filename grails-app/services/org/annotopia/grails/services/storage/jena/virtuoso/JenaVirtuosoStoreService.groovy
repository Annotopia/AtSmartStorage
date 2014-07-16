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

import org.annotopia.grails.vocabularies.Bibliographic
import org.annotopia.grails.vocabularies.RDF
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
import com.hp.hpl.jena.query.QueryExecution
import com.hp.hpl.jena.query.QueryExecutionFactory
import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.query.QuerySolution
import com.hp.hpl.jena.query.ResultSet
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.RDFNode

/**
 * Basic storage services for Virtuoso Triple store.
 *
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class JenaVirtuosoStoreService implements ITripleStore {
	
	def grailsApplication
	
	/**
	 * Returns a Virtuoso Graph for connecting to the Virtuoso
	 * triple store.
	 * @return The Virtuoso graph.
	 */
	private VirtGraph graph() {
		return new VirtGraph (
			grailsApplication.config.annotopia.storage.triplestore.host,
			grailsApplication.config.annotopia.storage.triplestore.user,
			grailsApplication.config.annotopia.storage.triplestore.pass);
	}
	
	// -----------------------------------------------------------------------
	//    COUNT
	// -----------------------------------------------------------------------
	/**
	 * Generic method for counting items in the triple store.
	 * @param apiKey		The API software key
	 * @param queryString	The query for counting.
	 * @return The count.
	 */
	public int count(apiKey, queryString, counter = "total") {
		VirtGraph graph = graph();
		log.trace('[' + apiKey + '] ' +  queryString);
			
		int totalCount = 0;
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (QueryFactory.create(queryString), graph);
		ResultSet results=vqe.execSelect();
		if(results.hasNext()) {
			totalCount = Integer.parseInt(results.next().get(counter).getString());
		}
		log.info('[' + apiKey + '] Total: ' + totalCount);
		totalCount;
	}
	
	/**
	 * Counts occurrences of grouped entities.
	 * @param apiKey		The API software key
	 * @param queryString	The query 
	 * @param counter		The label of the counter in the query
	 * @param groupBy		The grouping criteria in the query
	 * @return The map of the counter for each item of the group.
	 */
	public Map<String, Integer> countAndGroupBy(apiKey, queryString, counter, groupBy) {
		VirtGraph graph = graph();
		log.trace('[' + apiKey + '] ' + queryString);
		
		Map<String, Integer> map = new HashMap<String, Integer>();
		VirtuosoQueryExecution qGraphs = VirtuosoQueryExecutionFactory.create (QueryFactory.create(queryString), graph);
		ResultSet rGraphs = qGraphs.execSelect();
		while (rGraphs.hasNext()) {
			QuerySolution querySolution = rGraphs.nextSolution();
			map.put(
				querySolution.get(groupBy).toString(), 
				querySolution.get(counter).asLiteral().int
			);
		}
		map
	}
	
	// -----------------------------------------------------------------------
	//    QUERY
	// -----------------------------------------------------------------------
	/**
	 * This method runs all the queries that return a set of URIs. Normally
	 * this is used for graph names hence the SELECT should contain the item
	 * name 'g' which is the one that gets returned. 
	 * @param apiKey		The API software key
	 * @param queryString	The query that select 'g'
	 * @return The set of 'g' that have been selected.
	 */
	public Set<String> retrieveGraphsNames(apiKey, queryString) {
		VirtGraph graph = graph();
		log.trace('[' + apiKey + '] retrieveGraphsNames: ' + queryString);
		
		Set<String> graphNames = new HashSet<String>();
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(QueryFactory.create(queryString), graph);
		ResultSet results = vqe.execSelect();
		while (results.hasNext()) {
			QuerySolution result = results.nextSolution();
			RDFNode graph_name = result.get("g");
			graphNames.add(graph_name.toString());
		}
		graphNames
	}
	
	/**
	 * This method runs a SELECY query and returns the property values
	 * as a Set of Strings. 
	 * @param apiKey		The API software key
	 * @param queryString	The query that select a property
	 * @param variable		The property to query
	 * @return The set of property values
	 */
	public Set<String> retrievePropertyValues(apiKey, queryString, variable) {
		VirtGraph graph = graph();
		log.trace('[' + apiKey + '] retrievePropertyValues: ' + queryString);
		
		Set<String> propertyValues = new HashSet<String>();
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(QueryFactory.create(queryString), graph);
		ResultSet results = vqe.execSelect();
		while (results.hasNext()) {
			QuerySolution result = results.nextSolution();
			RDFNode property_value = result.get(variable);
			propertyValues.add(property_value.toString());
		}
		propertyValues
	}
	
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
		if (Environment.current == Environment.DEVELOPMENT && grailsApplication.config.annotopia.debug.storage.environment.trace.data &&
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
		
		VirtGraph set = graph();
		
		String queryString = "CONSTRUCT { ?s ?p ?o . } FROM <" + graphUri + ">" + 
			" WHERE { ?s ?p ?o . }";
		log.trace '[' + apiKey + '] retrieveGraph: ' + queryString
		
		try {
			VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (QueryFactory.create(queryString), set);	
			Model model = vqe.execConstruct();
			if(model!=null && !model.empty) {
				Dataset dataset = DatasetFactory.createMem();
				dataset.addNamedModel(graphUri, model);
				return dataset;
			} else {
				// TODO Raise exception?
				log.warn('[' + apiKey + '] Requested graph not found: ' + graphUri);
				return null;
			}
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
		log.trace '[' + apiKey + '] retrieveGraphMetadata: ' + queryString
		
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
	public Model retrieveGraphIdentifiersMetadata(String apiKey, Map<String,String> identifiers, String metadataIdentifiersGraphUri) {
		log.info '[' + apiKey + '] Retrieving graph identifiers metadata: ' + identifiers;

		VirtGraph set = new VirtGraph (metadataIdentifiersGraphUri,
			grailsApplication.config.annotopia.storage.triplestore.host,
			grailsApplication.config.annotopia.storage.triplestore.user,
			grailsApplication.config.annotopia.storage.triplestore.pass);

		if(identifiers.get(Bibliographic.LABEL_URL)!=null) {

			StringBuffer queryBuffer = new StringBuffer();
			queryBuffer.append(" ?expression ?ep ?eo . ");
			
			String doi = identifiers.get(Bibliographic.LABEL_DOI);
			if(doi!=null) queryBuffer.append(" OPTIONAL { ?expression <" + Bibliographic.DOI + "> ?doi . FILTER (?doi = '" + doi + "')}");

			String pmid = identifiers.get(Bibliographic.LABEL_PMID);
			if(pmid!=null) queryBuffer.append(" OPTIONAL { ?expression <" + Bibliographic.PMID + "> ?pmid . FILTER (?pmid = '" + pmid + "')}");

			String pmcid = identifiers.get(Bibliographic.LABEL_PMCID);
			if(pmcid!=null) queryBuffer.append(" OPTIONAL { ?expression <" + Bibliographic.PMCID + "> ?pmcid . FILTER (?pmcid = '" + pmcid + "')}");

			String pii = identifiers.get(Bibliographic.LABEL_PII);
			if(pii!=null) queryBuffer.append(" OPTIONAL { ?expression <" + Bibliographic.PII + "> ?pii . FILTER (?pii = '" + pii + "')}");

			String queryString = "CONSTRUCT { <"+identifiers.get(Bibliographic.LABEL_URL)+"> ?p ?o . ?expression ?ep ?eo. } FROM <" + metadataIdentifiersGraphUri + ">" +
				" WHERE { <"+identifiers.get(Bibliographic.LABEL_URL)+"> ?p ?o . <"+identifiers.get(Bibliographic.LABEL_URL)+"> <"+RDF.RDF_TYPE+"> <"+Bibliographic.WEB_PAGE+"> . " + queryBuffer.toString() + "}";
			log.trace '[' + apiKey + '] retrieveGraphIdentifiersMetadata: ' + queryString
		
			try {
				VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (QueryFactory.create(queryString), set);
				Model model = vqe.execConstruct();
				return model;
			} catch (Exception e) {
				log.error(e.getMessage());
				return null;
			}
		}
	}
	
	public List<String> retrieveAllManifestationsByIdentifiers(String apiKey, Map<String,String> identifiers, String metadataIdentifiersGraphUri) {
		log.info '[' + apiKey + '] Retrieving manifestations by identifiers: ' + identifiers;
		
		VirtGraph set = new VirtGraph (metadataIdentifiersGraphUri,
			grailsApplication.config.annotopia.storage.triplestore.host,
			grailsApplication.config.annotopia.storage.triplestore.user,
			grailsApplication.config.annotopia.storage.triplestore.pass);
		

		boolean first = false;
		StringBuffer queryBuffer = new StringBuffer();
		
		String doi = identifiers.get(Bibliographic.LABEL_DOI);
		if(doi!=null) {
			queryBuffer.append(" { ?expression <" + Bibliographic.DOI + "> ?doi . FILTER (?doi = '" + doi + "')}");
			first = true;
		}
		
		String pmid = identifiers.get(Bibliographic.LABEL_PMID);
		if(pmid!=null) {
			if(first) queryBuffer.append(" UNION ");
			queryBuffer.append(" { ?expression <" + Bibliographic.PMID + "> ?pmid . FILTER (?pmid = '" + pmid + "')}");
			first = true;
		}
		
		String pmcid = identifiers.get(Bibliographic.LABEL_PMCID);
		if(pmcid!=null) {
			if(first) queryBuffer.append(" UNION ");
			queryBuffer.append(" { ?expression <" + Bibliographic.PMCID + "> ?pmcid . FILTER (?pmcid = '" + pmcid + "')}");
			first = true;
		}
		
		String pii = identifiers.get(Bibliographic.LABEL_PII);
		if(pii!=null) {
			if(first) queryBuffer.append(" UNION ");
			queryBuffer.append(" { ?expression <" + Bibliographic.PII + "> ?pii . FILTER (?pii = '" + pii + "')}");
			first = true;
		}
		
		String QUERY = "SELECT DISTINCT ?manifestation FROM <" + metadataIdentifiersGraphUri + ">" +
			" WHERE { ?manifestation ?p ?expression . ?manifestation <"+RDF.RDF_TYPE+"> <"+Bibliographic.WEB_PAGE+"> . " + queryBuffer.toString() + "}";
		log.trace '[' + apiKey + '] retrieveAllManifestationsByIdentifiers: ' + QUERY
	
		List<String> manifestations = new ArrayList<String>();
		try {
			VirtuosoQueryExecution gIdentifiers  = VirtuosoQueryExecutionFactory.create(QueryFactory.create(QUERY), set);
			ResultSet rIdentifiers = gIdentifiers.execSelect();
			while (rIdentifiers.hasNext()) {
				QuerySolution querySolution = rIdentifiers.nextSolution();
		
				RDFNode manifestation = querySolution.get("manifestation");
				if(manifestation!=null) {
					manifestations.add(manifestation);
				}
			}
			return manifestations;
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
		if(dataset!=null) {
			Iterator<String> graphNames = dataset.listNames()
			while(graphNames.hasNext()) {
				String graphName = graphNames.next();
				if(dataset.getNamedModel(graphName).size()>0)
					existenceFlag = true;
			}
		}
		return existenceFlag;
	}
	
	// -----------------------------------------------------------------------
	//    DROP/CLEAR
	// -----------------------------------------------------------------------
	
	public void removeAllTriples(String apiKey, String graphUri, String subjectUri) {
		log.info '[' + apiKey + '] Removing all triples with subject: ' + subjectUri + ' from: ' + graphUri;
		
		try {
			VirtGraph graph = new VirtGraph (
				grailsApplication.config.annotopia.storage.triplestore.host,
				grailsApplication.config.annotopia.storage.triplestore.user,
				grailsApplication.config.annotopia.storage.triplestore.pass);
			String queryString = "DELETE WHERE { GRAPH <" + graphUri + ">" +
				" { <"+subjectUri+"> ?p ?o .} }";
			log.trace '[' + apiKey + '] removeAllTriples: ' + queryString
			
			VirtuosoUpdateRequest vur = VirtuosoUpdateFactory.create(queryString, graph);
			vur.exec();
		} catch (Exception e) {
			println "removeAllTriples: " + e.getMessage();
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
