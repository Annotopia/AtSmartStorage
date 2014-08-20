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
package org.annotopia.grails.services.storage.jena.integrated

import grails.converters.JSON

import java.text.SimpleDateFormat

import org.annotopia.grails.vocabularies.AnnotopiaVocabulary
import org.annotopia.grails.vocabularies.OA
import org.annotopia.grails.vocabularies.PAV
import org.annotopia.grails.vocabularies.RDF
import org.annotopia.groovy.service.store.StoreServiceException
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.RDFLanguages
import org.codehaus.groovy.grails.web.json.JSONArray

import virtuoso.jena.driver.VirtGraph
import virtuoso.jena.driver.VirtuosoQueryExecution
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory

import com.github.jsonldjava.utils.JsonUtils
import com.hp.hpl.jena.query.Dataset
import com.hp.hpl.jena.query.DatasetFactory
import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.query.QuerySolution
import com.hp.hpl.jena.query.ResultSet
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.rdf.model.RDFNode
import com.hp.hpl.jena.rdf.model.Resource
import com.hp.hpl.jena.rdf.model.ResourceFactory
import com.hp.hpl.jena.rdf.model.Statement
import com.hp.hpl.jena.rdf.model.StmtIterator

/**
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class AnnotationIntegratedStorageService {

	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
	
	def jenaUtilsService
	def grailsApplication
	def graphMetadataService
	def jenaVirtuosoStoreService
	def openAnnotationUtilsService
	def openAnnotationVirtuosoService
	def openAnnotationSetsVirtuosoService
	def openAnnotationStorageService
	def openAnnotationSetsUtilsService
	def graphIdentifiersMetadataService
	
	/**
	 * Counts the existing annotation sets.
	 * @param apiKey	The API key of the client issuing the request
	 * @param tgtUrls	The list of URLs identifying the targets of interest.
	 *                  If null all the available annotations will be returned.
	 *                  If empty none will be returned.
	 * @param tgtFgt	If true the results will include annotation of document fragments.
	 * 					If false, only the annotations on full resources will be returned.
	 * @return The number of existing annotation sets.
	 */
	public int countAnnotationSetGraphs(apiKey, List<String> tgtUrls, tgtFgt) {
		log.info('[' + apiKey + '] Counting Annotation Set Graphs');
		StringBuffer queryBuffer = new StringBuffer();
		if(tgtUrls==null) { // Return any annotation
			// If the tgtFgt is not true we need to filter out the
			// annotations that target fragments
			if(tgtFgt!="true") {
				queryBuffer.append("?s a oa:Annotation . FILTER NOT EXISTS { ?s oa:hasTarget ?sr. ?sr a oa:SpecificResource .}");
			}
		} else {
			// No results
			if(tgtUrls.size()==0) return 0;
			else {
				boolean first = false;
				tgtUrls.each { tgtUrl ->
					if(first) queryBuffer.append(" UNION ");
					if(tgtFgt=="false")
						queryBuffer.append("{ ?s a oa:Annotation . ?s oa:hasTarget <" + tgtUrl + "> }");
					else if(tgtFgt=="true")
						queryBuffer.append("{ ?s a oa:Annotation . ?s oa:hasTarget <" + tgtUrl + "> } UNION {?s oa:hasTarget ?t. ?t a oa:SpecificResource. ?t oa:hasSource <" + tgtUrl + ">}");
					first=true;
				}
			}
		}
		
		String queryString = "PREFIX oa: <http://www.w3.org/ns/oa#>  PREFIX at: <http://purl.org/annotopia#>" +
			"SELECT (COUNT(DISTINCT ?g1) AS ?total) WHERE { " + 
				"GRAPH ?g1 { ?set a at:AnnotationSet . ?set at:annotations ?g2 . } . " +
				"GRAPH ?g2 { " + queryBuffer.toString() + "}" +
			"}";
			
		log.info('[' + apiKey + '] Query total accessible Annotation Sets Graphs: ' + queryString);
		int totalCount = jenaVirtuosoStoreService.count(apiKey, queryString);
		log.info('[' + apiKey + '] Total accessible Annotation Set Graphs: ' + totalCount);
		totalCount;
	}
	
	/**
	 * Listing all the annotation sets according to given criteria (with pagination)
	 * @param apiKey	The API key of the client issuing the request
	 * @param max		The maximum number of results to return (pagination)
	 * @param offset	The offset for the results (pagination)
	 * @param tgtUrls	The list of URLs identifying the targets of interest. 
	 *                  If null all the available annotations will be returned. 
	 *                  If empty none will be returned.
	 * @param tgtFgt	If true the results will include annotation of document fragments.
	 * 					If false, only the annotations on full resources will be returned.
	 * @param tgtExt	(Not implemented yet)
	 * @param tgtIds	The list of IDs identifying the targets of interest.
	 * @param incGph	If true the graph accommodating the annotation metadata included
	 * 					in the annotation metadata provenance graph will be returned as well.
	 * @return  Listing all the annotation sets according to given criteria.
	 */
	public listAnnotationSets(apiKey, max, offset, List<String> tgtUrls, tgtFgt, tgtExt, tgtIds, incGph) {
		log.info '[' + apiKey + '] List integrated annotation sets ' +
			' max:' + max +
			' offset:' + offset +
			' incGph:' + incGph +
			' tgtUrl:' + tgtUrls +
			' tgtFgt:' + tgtFgt;
			
		Set<Dataset> datasets = new HashSet<Dataset>();
		Set<String> graphNames = retrieveAnnotationSetsGraphsNames(apiKey, max, offset, tgtUrls, tgtFgt);
		if(graphNames!=null) {
			graphNames.each { graphName ->
				Dataset ds = retrieveAnnotationSetGraph(apiKey, graphName);
				if(incGph=='true') {
					Model m = jenaVirtuosoStoreService.retrieveGraphMetadata(apiKey, graphName, grailsApplication.config.annotopia.storage.uri.graph.provenance);
					if(m!=null) ds.setDefaultModel(m);
				}
				if(ds!=null) datasets.add(ds);
			}
		}
		return datasets;
	}
	
	/**
	 * Retrieves all the annotation sets according to given criteria (with pagination)
	 * @param apiKey	The API key of the client issuing the request
	 * @param max		The maximum number of results to return (pagination)
	 * @param offset	The offset for the results (pagination)
	 * @param tgtUrls	The list of URLs identifying the targets of interest. 
	 *                  If null all the available annotations will be returned. 
	 *                  If empty none will be returned.
	 * @param tgtFgt	If true the results will include annotation of document fragments.
	 * 					If false, only the annotations on full resources will be returned.
	 * @return The annotation sets according to given criteria.
	 */
	public Set<String> retrieveAnnotationSetsGraphsNames(apiKey, max, offset, List<String> tgtUrls, tgtFgt) {
		log.info  '[' + apiKey + '] Retrieving annotation sets graphs names ' +
			' max:' + max +
			' offset:' + offset +
			' tgtUrls:' + tgtUrls +
			' tgtFgt:' + tgtFgt;
		
		StringBuffer queryBuffer = new StringBuffer();
		if(tgtUrls==null) { // Return any annotation
			// If the tgtFgt is not true we need to filter out the
			// annotations that target fragments
			if(tgtFgt!="true") {
				queryBuffer.append("FILTER NOT EXISTS { ?s oa:hasTarget ?sr. ?sr a oa:SpecificResource .}");
			}
		} else {
			if(tgtUrls.size()==0) return new HashSet<String>();
			// Returns annotations on the requested URLs (full resource)
			else {
				boolean first = false;
				tgtUrls.each { tgtUrl ->
					if(first) queryBuffer.append(" UNION ");
					if(tgtUrls.size()>0 && tgtFgt=="false")
						queryBuffer.append("{ ?s a oa:Annotation . ?s oa:hasTarget <" + tgtUrl + "> }");
					else if(tgtUrls.size()>0 && tgtFgt=="true")
						queryBuffer.append("{?s oa:hasTarget <" + tgtUrl + "> } UNION {?s oa:hasTarget ?t. ?t a oa:SpecificResource. ?t oa:hasSource <" + tgtUrl + ">}")
					first=true;
				}
			}
		}
	
		String subquery = "";
		if(queryBuffer.size()>0) {
			subquery = "?set at:annotations ?g2 . } GRAPH ?g2 { " + queryBuffer.toString();
		} 
		
		String queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> PREFIX at:  <http://purl.org/annotopia#> " +
		"SELECT DISTINCT ?g WHERE { GRAPH ?g { ?set a at:AnnotationSet . " +
			subquery +
		"}} LIMIT " + max + " OFFSET " + offset;
		Set<String> graphs = jenaVirtuosoStoreService.retrieveGraphsNames(apiKey, queryString);
		graphs
	}
	
	public Dataset retrieveAnnotationSet(apiKey, uri) {
		log.info '[' + apiKey + '] Retrieving annotation sets ' + uri;
		
		String queryString = "PREFIX at:  <http://purl.org/annotopia#> " +
			"SELECT DISTINCT ?g WHERE { GRAPH ?g { <" + uri + "> a at:AnnotationSet }}";
		log.trace('[' + apiKey + '] ' + queryString);
	
		VirtGraph graph = new VirtGraph (
			grailsApplication.config.annotopia.storage.triplestore.host,
			grailsApplication.config.annotopia.storage.triplestore.user,
			grailsApplication.config.annotopia.storage.triplestore.pass);
		
		Dataset graphs;
		try {
			VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (QueryFactory.create(queryString), graph);
			ResultSet results = vqe.execSelect();
			while (results.hasNext()) {
				QuerySolution result = results.nextSolution();
				RDFNode graph_name = result.get("g");
				if(graphs==null) graphs =  jenaVirtuosoStoreService.retrieveGraph(apiKey, graph_name.toString());
				else {
					Dataset ds = jenaVirtuosoStoreService.retrieveGraph(apiKey, graph_name.toString());
					ds.listNames().each { name ->
						graphs.addNamedModel(name, ds.getNamedModel(name));
					}
					if(ds.getDefaultModel()!=null) {
						println 'def model'
						graphs.setDefaultModel(ds.getDefaultModel());
					}
				}
			}
			return graphs;
		} catch (Exception e) {
			log.error(e.getMessage());
			return null;
		}
	}
	
//	public listAnnotationSets(apiKey, max, offset, tgtUrl, tgtFgt, tgtExt, tgtIds, incGph) {
//		log.info '[' + apiKey + '] List integrated annotation sets ' +
//			' max:' + max +
//			' offset:' + offset +
//			' incGph:' + incGph +
//			' tgtUrl:' + tgtUrl +
//			' tgtFgt:' + tgtFgt;
//		retrieveAnnotationSets(apiKey, max, offset, tgtUrl, tgtFgt, incGph);
//	}
//	
//	public Set<Dataset> retrieveAnnotationSets(apiKey, max, offset, tgtUrl, tgtFgt, incGph) {
//		log.info '[' + apiKey + '] Retrieving integrated annotation sets';
//	
//		Set<Dataset> datasets = new HashSet<Dataset>();
//		Set<String> graphNames = retrieveAnnotationSetsGraphsNames(apiKey, max, offset, tgtUrl, tgtFgt);
//		println '+++++ ' + graphNames.size();
//		if(graphNames!=null) {
//			graphNames.each { graphName ->
//				println '+++++ ' + graphName
//				Dataset ds = retrieveAnnotationSetGraph(apiKey, graphName);
//				if(incGph=='true') {
//					Model m = jenaVirtuosoStoreService.retrieveGraphMetadata(apiKey, graphName, grailsApplication.config.annotopia.storage.uri.graph.provenance);
//					if(m!=null) ds.setDefaultModel(m);
//				}
//				if(ds!=null) datasets.add(ds);
//			}
//		}
//		return datasets;
//	}
//	
	// TODO is this a duplicate? It seems just retrieving a graph
	public Dataset retrieveAnnotationSetGraph(String apiKey, String graphUri) {
		log.info '[' + apiKey + '] Retrieving graph: ' + graphUri;
		
		VirtGraph set = new VirtGraph (graphUri,
			grailsApplication.config.annotopia.storage.triplestore.host,
			grailsApplication.config.annotopia.storage.triplestore.user,
			grailsApplication.config.annotopia.storage.triplestore.pass);
		
		String queryString = "CONSTRUCT { ?s ?p ?o . } FROM <" + graphUri + ">" +
			" WHERE { ?s ?p ?o . }";
		log.trace '[' + apiKey + '] ' + queryString
		
		try {
			VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (QueryFactory.create(queryString), set);
			Model model = vqe.execConstruct();
			if(model!=null && !model.empty) {
				Dataset dataset = DatasetFactory.createMem();
				dataset.addNamedModel(graphUri, model);
				
				// Find the annotation graphs and insert them in the Model
				
				
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
	
	/*
	PREFIX oa:<http://www.w3.org/ns/oa#> PREFIX at:<http://purl.org/annotopia#> SELECT (COUNT(DISTINCT ?g1)) WHERE {
	GRAPH ?g1 { ?s a at:AnnotationSet . ?s at:annotations ?g2 .}
	GRAPH ?gg {
	   { ?a oa:hasTarget <http://paolociccarese.info> }
	   UNION
	   {?a <http://www.w3.org/ns/oa#hasTarget> ?t. ?t <http://www.w3.org/ns/oa#hasSource> <http://paolociccarese.info> }
	}}
	*/
	
//	public int countAnnotationSetGraphs(apiKey, tgtUrl, tgtFgt) {
//		String queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> PREFIX at:<http://purl.org/annotopia#> " +
//			"SELECT (COUNT(DISTINCT ?g1) AS ?total) WHERE { GRAPH ?g1 { ?s a at:AnnotationSet }}";
//		if(tgtUrl!=null && tgtFgt=="false") {
//			queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> PREFIX at:<http://purl.org/annotopia#> " +
//				"SELECT (COUNT(DISTINCT ?g1) AS ?total) WHERE { GRAPH ?g1 { ?s a at:AnnotationSet . ?s at:annotations ?g2 .} GRAPH ?g2 { ?a oa:hasTarget <" + tgtUrl + "> }}";
//		} else if(tgtUrl!=null && tgtFgt=="true") {
//			queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> PREFIX at:<http://purl.org/annotopia#> " +
//				"SELECT (COUNT(DISTINCT ?g1) AS ?total) WHERE { GRAPH ?g1 { ?s a at:AnnotationSet . ?s at:annotations ?g2 .} GRAPH ?g2 {{ ?a oa:hasTarget <" + tgtUrl + "> } " +
//				" UNION {?a <http://www.w3.org/ns/oa#hasTarget> ?t. ?t <http://www.w3.org/ns/oa#hasSource> <" + tgtUrl + "> }}}" ;
//		}
//			
//		int totalCount = jenaVirtuosoStoreService.count(apiKey, queryString);
//		log.info('[' + apiKey + '] Total accessible Annotation Set Graphs: ' + totalCount);
//		totalCount;
//	}
	
//	public Set<String> retrieveAnnotationSetsGraphsNames(apiKey, max, offset, tgtUrl, tgtFgt) {
//		log.info  '[' + apiKey + '] Retrieving annotation sets graphs names ' +
//			' max:' + max +
//			' offset:' + offset +
//			' tgtUrl:' + tgtUrl;
//			
//		String queryString = "PREFIX at: <http://purl.org/annotopia#> " +
//			"SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s a at:AnnotationSet }} LIMIT " + max + " OFFSET " + offset;
//		if(tgtUrl!=null) {
//			queryString = "PREFIX oa: <http://www.w3.org/ns/oa#> PREFIX at:  <http://purl.org/annotopia#> " +
//				"SELECT DISTINCT ?g  WHERE { GRAPH ?g { ?s a at:AnnotationSet . ?s at:annotations ?g2 .} GRAPH ?g2 {{ ?a oa:hasTarget <" + tgtUrl + "> } " +
//				" UNION {?a <http://www.w3.org/ns/oa#hasTarget> ?t. ?t <http://www.w3.org/ns/oa#hasSource> <" + tgtUrl + "> }}}" ;
//		}
//		
//		Set<String> graphs = jenaVirtuosoStoreService.retrieveGraphsNames(apiKey, queryString);
//		println '0000000 ' + graphs.size();
//		graphs.each { println it}
//		graphs
//	}
	
	public Dataset saveAnnotationSet(String apiKey, Long startTime, Boolean incGph, String set) {

		// Reads the inputs in a dataset
		Dataset inMemoryDataset = DatasetFactory.createMem();
		try {
			RDFDataMgr.read(inMemoryDataset, new ByteArrayInputStream(set.getBytes("UTF-8")), RDFLanguages.JSONLD);
		} catch (Exception ex) {
			log.info("[" + apiKey + "] Annotation set cannot be read... request rejected.");
			def json = JSON.parse('{"status":"invalidcontent" ,"message":"Annotation set cannot be read"' +
				',"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
			throw new StoreServiceException(500, json, "text/json", "UTF-8");
		}
		
		// Registry of the URIs of the annotations.
		// Note: The method currently supports the saving of one annotation at a time
		Set<Resource> annotationUris = new HashSet<Resource>();
		
		// Registry of all named graphs in the transaction
		Set<Resource> graphsUris = jenaUtilsService.detectNamedGraphs(apiKey, inMemoryDataset);

		// Detection of default graph
		int annotationsInDefaultGraphsCounter = openAnnotationUtilsService.detectAnnotationsInDefaultGraph(apiKey, inMemoryDataset, annotationUris, null)
		boolean defaultGraphDetected = (annotationsInDefaultGraphsCounter>0);
		
		// Query for graphs containing annotation
		// See: https://www.mail-archive.com/wikidata-l@lists.wikimedia.org/msg00370.html
		Set<Resource> annotationsGraphsUris = new HashSet<Resource>();
		int detectedAnnotationGraphsCounter = openAnnotationUtilsService.detectAnnotationsInNamedGraph(
			apiKey, inMemoryDataset, graphsUris, annotationsGraphsUris, annotationUris, null)
		
		// Enforcing the limit to one annotation per transaction
		if(defaultGraphDetected && detectedAnnotationGraphsCounter>0) {
			log.info("[" + apiKey + "] Mixed Annotation content detected... request rejected.");
			def json = JSON.parse('{"status":"nocontent" ,"message":"The request carries a mix of Annotations and Annotation Graphs"' +
				',"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
			throw new StoreServiceException(200, json, "text/json", "UTF-8");
		} else if(detectedAnnotationGraphsCounter>1 || graphsUris.size()>2) {
			// Annotation Set not found
			log.info("[" + apiKey + "] Multiple Annotation graphs detected... request rejected.");
			def json = JSON.parse('{"status":"nocontent" ,"message":"The request carries multiple Annotations or Annotation Graphs"' +
				',"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
			throw new StoreServiceException(200, json, "text/json", "UTF-8");
		}
		
		if(defaultGraphDetected) {
			log.trace("[" + apiKey + "] Default graph detected.");
			// Annotation Set
			Resource annotationSetUri = openAnnotationStorageService.persistURI(apiKey, inMemoryDataset.getDefaultModel(),
				ResourceFactory.createProperty(RDF.RDF_TYPE),
				ResourceFactory.createResource(AnnotopiaVocabulary.ANNOTATION_SET), "annotationset");

			Model annotationModel = inMemoryDataset.getDefaultModel();
			HashMap<Resource, String> oldNewAnnotationUriMapping = new HashMap<Resource, String>();
			
			persistNewAnnotation(apiKey, annotationUris, annotationModel, oldNewAnnotationUriMapping);
			
			// Identity management
			Map<String,String> identifiers = new HashMap<String,String>();
			def identifierUri = mintUri("expression");
			jenaUtilsService.getDatasetAsString(inMemoryDataset);
			openAnnotationUtilsService.detectTargetIdentifiersInDefaultGraph(apiKey, inMemoryDataset, identifiers)
			
			// identifiersModel can be null if no identifier is present.
			Model identifiersModel = jenaVirtuosoStoreService.retrieveGraphIdentifiersMetadata(apiKey, identifiers, grailsApplication.config.annotopia.storage.uri.graph.identifiers);
			if(identifiersModel!=null) jenaUtilsService.getDatasetAsString(identifiersModel);
			
			
	//		// Identity management
	//		Map<String,String> identifiers = new HashMap<String,String>();
	//		def identifierUri = mintUri("expression");
	//		openAnnotationUtilsService.detectTargetIdentifiersInDefaultGraph(apiKey, dataset, identifiers)
	//		Model identifiersModel = jenaVirtuosoStoreService.retrieveGraphIdentifiersMetadata(apiKey, identifiers, grailsApplication.config.annotopia.storage.uri.graph.identifiers);
	//		jenaUtilsService.getDatasetAsString(identifiersModel);
	//		if(identifiersModel.empty)
	//			graphIdentifiersMetadataService.getIdentifiersGraphMetadata(apiKey, creationDataset, identifierUri, identifiers);
			
//			// Annotations
//			// Specific Resource identifier
//			openAnnotationStorageService.persistURIs(apiKey, inMemoryDataset.getDefaultModel(),
//				ResourceFactory.createProperty(RDF.RDF_TYPE),
//				ResourceFactory.createResource(OA.SPECIFIC_RESOURCE), "resource");
//
//			// Embedded content (as RDF) identifier
//			openAnnotationStorageService.persistURIs(apiKey, inMemoryDataset.getDefaultModel(),
//				ResourceFactory.createProperty(RDF.RDF_TYPE),
//				ResourceFactory.createResource(OA.CONTEXT_AS_TEXT), "content");
//
//			HashMap<Resource, String> oldNewAnnotationUriMapping = new HashMap<Resource, String>();
//			Iterator<Resource> annotationUrisIterator = annotationUris.iterator();
//			while(annotationUrisIterator.hasNext()) {
//				// Bodies graphs identifiers
//				Resource annotation = annotationUrisIterator.next();
//				String newAnnotationUri = openAnnotationStorageService.mintAnnotationUri();
//				oldNewAnnotationUriMapping.put(annotation, newAnnotationUri);
//			}
//			
//			// Update Bodies graphs URIs
//			Model annotationModel = inMemoryDataset.getDefaultModel();
//			oldNewAnnotationUriMapping.keySet().each { oldAnnotation ->
//				// Update annotation URi when subject
//				StmtIterator statements = annotationModel.listStatements(oldAnnotation, null, null);
//				List<Statement> statementsToRemove = new ArrayList<Statement>();
//				statements.each { statementsToRemove.add(it)}
//				statementsToRemove.each { statement ->
//					annotationModel.remove(statement);
//					annotationModel.add(ResourceFactory.createResource(oldNewAnnotationUriMapping.get(oldAnnotation)),
//						statement.getPredicate(), statement.getObject());
//				}
//				
//				annotationModel.removeAll(ResourceFactory.createResource(oldNewAnnotationUriMapping.get(oldAnnotation)), ResourceFactory.createProperty(PAV.PAV_PREVIOUS_VERSION), null);
//				annotationModel.add(
//					ResourceFactory.createResource(oldNewAnnotationUriMapping.get(oldAnnotation)),
//					ResourceFactory.createProperty(PAV.PAV_PREVIOUS_VERSION),
//					ResourceFactory.createPlainLiteral(oldAnnotation.toString()));
//				
//				annotationModel.removeAll(ResourceFactory.createResource(oldNewAnnotationUriMapping.get(oldAnnotation)), ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_ON), null);
//				annotationModel.add(
//					ResourceFactory.createResource(oldNewAnnotationUriMapping.get(oldAnnotation)),
//					ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_ON),
//					ResourceFactory.createPlainLiteral(dateFormat.format(new Date())));
//			}
			
			// TODO make sure there is only one set
			
			// Update annotation URi when objects
			List<Statement> statementsToRemove = new ArrayList<Statement>();
			StmtIterator statements = annotationModel.listStatements(null,
				ResourceFactory.createProperty(RDF.RDF_TYPE),
				ResourceFactory.createResource(AnnotopiaVocabulary.ANNOTATION_SET));
			if(statements.hasNext()) {
				Statement annotationSetStatement = statements.nextStatement();
				Resource annotationSet = annotationSetStatement.getSubject();
				// Getting all the annotations of the set
				StmtIterator stats = annotationModel.listStatements(annotationSet,
					ResourceFactory.createProperty(AnnotopiaVocabulary.ANNOTATIONS),
					null);
				while(stats.hasNext()) {
					Statement s = stats.nextStatement();
					statementsToRemove.add(s);
				}
			}		
			statementsToRemove.each { s ->
				annotationModel.remove(s);
				annotationModel.add(s.getSubject(), ResourceFactory.createProperty(AnnotopiaVocabulary.ANNOTATIONS),
					ResourceFactory.createProperty(oldNewAnnotationUriMapping.get(s.getObject())));
			}
			
			// Last saved on
			annotationModel.removeAll(annotationSetUri, ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_ON), null);
			annotationModel.add(annotationSetUri, ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_ON),
				ResourceFactory.createPlainLiteral(dateFormat.format(new Date())));
			
			// Version
			annotationModel.removeAll(annotationSetUri, ResourceFactory.createProperty(PAV.PAV_VERSION), null);
			annotationModel.add(annotationSetUri, ResourceFactory.createProperty(PAV.PAV_VERSION),
				ResourceFactory.createPlainLiteral("1"));
			
			// Minting of the URI for the Named Graph that will wrap the
			// default graph
			def graphUri = openAnnotationStorageService.mintGraphUri();
			Dataset creationDataset = DatasetFactory.createMem();
			creationDataset.addNamedModel(graphUri, inMemoryDataset.getDefaultModel());
			
			Dataset storedDataset = DatasetFactory.createMem();
			storedDataset.addNamedModel(graphUri, inMemoryDataset.getDefaultModel());
			
			// Creation of the metadata for the Graph wrapper
			def graphResource = ResourceFactory.createResource(graphUri);
			Model metaModel = graphMetadataService.getAnnotationSetGraphCreationMetadata(apiKey, creationDataset, graphUri);
			oldNewAnnotationUriMapping.values().each { annotationUri ->
				metaModel.add(graphResource, ResourceFactory.createProperty(AnnotopiaVocabulary.GRAPH_ANNOTATION),
					 ResourceFactory.createPlainLiteral(annotationUri));
			}
			if(oldNewAnnotationUriMapping.values().size()>0) {
				metaModel.add(graphResource, ResourceFactory.createProperty(AnnotopiaVocabulary.GRAPH_ANNOTATION_COUNT),
					ResourceFactory.createPlainLiteral(""+oldNewAnnotationUriMapping.values().size()));
			}
			
			// split the annotations into graphs
			Dataset annotationGraphs = openAnnotationSetsUtilsService.splitAnnotationGraphs(apiKey, storedDataset);
			
			// If no identifiers are found for this resource we create the identifiers metadata.
			if(identifiersModel!=null) {
				if(identifiersModel.empty) {
					graphIdentifiersMetadataService.getIdentifiersGraphMetadata(apiKey, annotationGraphs, identifierUri, identifiers);
				} else {
					graphIdentifiersMetadataService.updateIdentifiersGraphMetadata(apiKey, creationDataset, identifiersModel, identifiers);
				}
			}
			
			jenaVirtuosoStoreService.storeDataset(apiKey, annotationGraphs);
			storedDataset
		}
	}
	
	private void persistNewAnnotation(def apiKey, def annotationUris, Model annotationModel, HashMap<Resource, String> oldNewAnnotationUriMapping) {
		// Annotations
		// Specific Resource identifier
		openAnnotationStorageService.persistURIs(apiKey, annotationModel,
			ResourceFactory.createProperty(RDF.RDF_TYPE),
			ResourceFactory.createResource(OA.SPECIFIC_RESOURCE), "resource");

		// Embedded content (as RDF) identifier
		openAnnotationStorageService.persistURIs(apiKey, annotationModel,
			ResourceFactory.createProperty(RDF.RDF_TYPE),
			ResourceFactory.createResource(OA.CONTEXT_AS_TEXT), "content");

		Iterator<Resource> annotationUrisIterator = annotationUris.iterator();
		while(annotationUrisIterator.hasNext()) {
			// Annotations identifiers
			Resource annotation = annotationUrisIterator.next();
			String newAnnotationUri = openAnnotationStorageService.mintAnnotationUri();
			oldNewAnnotationUriMapping.put(annotation, newAnnotationUri);
		}
		
		// Update Bodies graphs URIs
		oldNewAnnotationUriMapping.keySet().each { oldAnnotation ->
			// Update annotation URi when subject
			StmtIterator statements = annotationModel.listStatements(oldAnnotation, null, null);
			List<Statement> statementsToRemove = new ArrayList<Statement>();
			statements.each { statementsToRemove.add(it)}
			statementsToRemove.each { statement ->
				annotationModel.remove(statement);
				annotationModel.add(ResourceFactory.createResource(oldNewAnnotationUriMapping.get(oldAnnotation)),
					statement.getPredicate(), statement.getObject());
			}
			
			annotationModel.removeAll(ResourceFactory.createResource(oldNewAnnotationUriMapping.get(oldAnnotation)), ResourceFactory.createProperty(PAV.PAV_PREVIOUS_VERSION), null);
			annotationModel.add(
				ResourceFactory.createResource(oldNewAnnotationUriMapping.get(oldAnnotation)),
				ResourceFactory.createProperty(PAV.PAV_PREVIOUS_VERSION),
				ResourceFactory.createPlainLiteral(oldAnnotation.toString()));
			
			annotationModel.removeAll(ResourceFactory.createResource(oldNewAnnotationUriMapping.get(oldAnnotation)), ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_ON), null);
			annotationModel.add(
				ResourceFactory.createResource(oldNewAnnotationUriMapping.get(oldAnnotation)),
				ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_ON),
				ResourceFactory.createPlainLiteral(dateFormat.format(new Date())));
		}
	}
	
	public Dataset updateAnnotationSet(String apiKey, Long startTime, Boolean incGph, String set) {
		
		// Reads the inputs in a dataset
		Dataset inMemoryDataset = DatasetFactory.createMem();
		try {
			RDFDataMgr.read(inMemoryDataset, new ByteArrayInputStream(set.getBytes("UTF-8")), RDFLanguages.JSONLD);
		} catch (Exception ex) {
			log.info("[" + apiKey + "] Annotation set cannot be read... request rejected.");
			def json = JSON.parse('{"status":"invalidcontent" ,"message":"Annotation set cannot be read"' +
				',"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
			throw new StoreServiceException(500, json, "text/json", "UTF-8");
		}
		
		// Registry of the URIs of the annotations.
		// Note: The method currently supports the saving of one annotation at a time
		Set<Resource> annotationUris = new HashSet<Resource>();
		
		// Registry of all named graphs in the transaction
		Set<Resource> graphsUris = jenaUtilsService.detectNamedGraphs(apiKey, inMemoryDataset);
		
		// Detection of default graph
		int annotationsInDefaultGraphsCounter = openAnnotationUtilsService.detectAnnotationsInDefaultGraph(apiKey, inMemoryDataset, annotationUris, null)
		boolean defaultGraphDetected = (annotationsInDefaultGraphsCounter>0);

		// Query for graphs containing annotation
		// See: https://www.mail-archive.com/wikidata-l@lists.wikimedia.org/msg00370.html
		Set<Resource> annotationsGraphsUris = new HashSet<Resource>();
		int detectedAnnotationGraphsCounter = openAnnotationUtilsService.detectAnnotationsInNamedGraph(
			apiKey, inMemoryDataset, graphsUris, annotationsGraphsUris, annotationUris, null)
		
		// Enforcing the limit to one annotation per transaction
		if(defaultGraphDetected && detectedAnnotationGraphsCounter>0) {
			log.info("[" + apiKey + "] Mixed Annotation content detected... request rejected.");
			def json = JSON.parse('{"status":"nocontent" ,"message":"The request carries a mix of Annotations and Annotation Graphs"' +
				',"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
			throw new StoreServiceException(200, json, "text/json", "UTF-8");
		} else if(detectedAnnotationGraphsCounter>1 || graphsUris.size()>2) {
			// Annotation Set not found
			log.info("[" + apiKey + "] Multiple Annotation graphs detected... request rejected.");
			def json = JSON.parse('{"status":"nocontent" ,"message":"The request carries multiple Annotations or Annotation Graphs"' +
				',"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
			throw new StoreServiceException(200, json, "text/json", "UTF-8");
		}
		
		if(defaultGraphDetected) {
			log.trace("[" + apiKey + "] Default graph detected.");
			
			Model annotationModel = inMemoryDataset.getDefaultModel();
			HashMap<Resource, String> oldNewAnnotationUriMapping = new HashMap<Resource, String>();
			
			Set<Resource> annotationSetUris = new HashSet<Resource>();
			int annotationsSetsUrisInDefaultGraphsCounter =
				openAnnotationSetsUtilsService.detectAnnotationSetUriInDefaultGraph(apiKey, inMemoryDataset, annotationSetUris, null);

			if(annotationsSetsUrisInDefaultGraphsCounter==0) {
				log.info("[" + apiKey + "] No Annotation set detected... request rejected.");
				def json = JSON.parse('{"status":"nocontent" ,"message":"The request does not carry Annotation Sets."' +
					',"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
				throw new StoreServiceException(200, json, "text/json", "UTF-8");
			} else if(annotationsSetsUrisInDefaultGraphsCounter==1) {
				
				Dataset datasetToRender = DatasetFactory.createMem();
			
				String annotationSetUri = annotationSetUris.iterator().next();
				
				// Retrieve the graph name from the storage
				String QUERY = "PREFIX at: <http://purl.org/annotopia#> SELECT DISTINCT ?s ?g WHERE { GRAPH ?g { <" + annotationSetUri + "> a at:AnnotationSet . }}"
				Set<String> annotationSetsGraphNames = jenaVirtuosoStoreService.retrieveGraphsNames(apiKey, QUERY);
				
				if(annotationSetsGraphNames.size()==1) {
					log.info("[" + apiKey + "] found Annotation Set graph " + annotationSetsGraphNames);
					def gName = annotationSetsGraphNames.iterator().next();
				
					def annotationGraphs = [] as List;
					Dataset annotationSetGraph = jenaVirtuosoStoreService.retrieveGraph(apiKey, gName);
					Model defModel = annotationSetGraph.getNamedModel(gName);
					StmtIterator sIt = defModel.listStatements(null, ResourceFactory.createProperty(AnnotopiaVocabulary.ANNOTATIONS), null);
					while(sIt.hasNext()) {
						annotationGraphs.add(sIt.nextStatement().getObject().asResource().getURI());
					}
					
					def annotationGraphToModelMap = [:]
					def annotationToAnnotationGraphMap = [:]
					annotationGraphs.each { nGraph ->
						def gg = jenaVirtuosoStoreService.retrieveGraph(apiKey, nGraph);
						StmtIterator sIt1 = gg.getNamedModel(nGraph).listStatements(null, 
							ResourceFactory.createProperty(RDF.RDF_TYPE), ResourceFactory.createResource(OA.ANNOTATION));
						if(sIt1.hasNext()) {
							def uri = sIt1.nextStatement().getSubject().getURI();
							annotationToAnnotationGraphMap.put(uri, nGraph);
							annotationGraphToModelMap.put(nGraph, gg.getNamedModel(nGraph));
						}
					}
					
					// Split annotations out
					def unchangedAnnotations = []
					def annotationToModelMap = [:]

					//Set<Model> annotationsModels = new HashSet<Model>();
					Object json = JsonUtils.fromString(set);
					JSONArray array = json.getAt("annotations");
					
					Model bareSetModel = ModelFactory.createDefaultModel()
					Object bareSetJson = json;
					bareSetJson.getAt("annotations").clear();
					String annotationSetAsString = JsonUtils.toString(bareSetJson);
					RDFDataMgr.read(bareSetModel, new ByteArrayInputStream(annotationSetAsString.getBytes("UTF-8")), RDFLanguages.JSONLD);
					
					for(int j=0; j<array.size(); j++) {
						Model m = ModelFactory.createDefaultModel()
			
						def annotation = array.get(j);
						annotation.put("@context", grailsApplication.config.annotopia.jsonld.annotopia.context);
						
						String annotationAsString = JsonUtils.toString(annotation);
						
						println annotationAsString
						
						RDFDataMgr.read(m, new ByteArrayInputStream(annotationAsString.getBytes("UTF-8")), RDFLanguages.JSONLD);
						//annotationsModels.add(m)

						def annotationUri;
						StmtIterator annotationUriIterator = m.listStatements(null, 
							ResourceFactory.createProperty(RDF.RDF_TYPE), ResourceFactory.createResource(OA.ANNOTATION));
						if(annotationUriIterator.hasNext()) {
							annotationUri = annotationUriIterator.nextStatement().getSubject().getURI();
							annotationToModelMap.put(annotationUri, m);
						}

						StmtIterator annotationChangedIterator = m.listStatements(ResourceFactory.createResource(annotationUri),
							ResourceFactory.createProperty(AnnotopiaVocabulary.HAS_CHANGED), null);
						if(annotationChangedIterator.hasNext()) {
							if(annotationChangedIterator.nextStatement().getObject().asLiteral().toString()=="true") {
								// Annotation to save
								println 'CHANGED ' + annotationUri
								
								m.removeAll(ResourceFactory.createResource(annotationUri), ResourceFactory.createProperty(AnnotopiaVocabulary.HAS_CHANGED), null);
								
								// Updating last saved on
								m.removeAll(ResourceFactory.createResource(annotationUri), ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_ON), null);
								m.add(
									ResourceFactory.createResource(annotationUri),
									ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_ON),
									ResourceFactory.createPlainLiteral(dateFormat.format(new Date())));
								
								m.removeAll(ResourceFactory.createResource(annotationSetUri), ResourceFactory.createProperty(PAV.PAV_PREVIOUS_VERSION), null);
								
								if(!annotationToAnnotationGraphMap.containsKey(annotationUri)) {
									// New annotation
									println 'NEW ' + annotationUri
									persistNewAnnotation(apiKey, annotationUris, m, oldNewAnnotationUriMapping);							
									datasetToRender.addNamedModel(mintGraphUri(), m);
									
									//ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
									//RDFDataMgr.write(outputStream, m, RDFLanguages.JSONLD);
									//println outputStream.toString();
								} else {
									println 'UPDATE ' + annotationUri
									// Updated last saved on	
									def lGraph = annotationToAnnotationGraphMap.get(annotationUri);
									datasetToRender.addNamedModel(annotationUri, annotationGraphToModelMap.get(lGraph));
								}		
							} else {
								// Annotation to ignore (graph is already in the storage, does not change)
								println 'UNCHANGED ' + annotationUri
								unchangedAnnotations.add(annotationUri);
								m.removeAll(ResourceFactory.createResource(annotationUri), ResourceFactory.createProperty(AnnotopiaVocabulary.HAS_CHANGED), null);
								m.removeAll(ResourceFactory.createResource(annotationSetUri), ResourceFactory.createProperty(PAV.PAV_PREVIOUS_VERSION), null);
								
								datasetToRender.addNamedModel(annotationToAnnotationGraphMap.get(annotationUri), m);
							}
						} 
					}
					
					// Set Last saved on
					bareSetModel.removeAll(ResourceFactory.createResource(annotationSetUri), ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_ON), null);
					bareSetModel.add(ResourceFactory.createResource(annotationSetUri), ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_ON),
						ResourceFactory.createPlainLiteral(dateFormat.format(new Date())));
					
					// Set Version
					bareSetModel.removeAll(ResourceFactory.createResource(annotationSetUri), ResourceFactory.createProperty(PAV.PAV_VERSION), null);
					bareSetModel.add(ResourceFactory.createResource(annotationSetUri), ResourceFactory.createProperty(PAV.PAV_VERSION),
						ResourceFactory.createPlainLiteral("1"));
					
					Iterator<String> iterator = datasetToRender.listNames();
					while(iterator.hasNext()) {
						bareSetModel.add(ResourceFactory.createResource(annotationSetUri), ResourceFactory.createProperty(AnnotopiaVocabulary.ANNOTATIONS),
							ResourceFactory.createProperty(iterator.next()));
					}
					
					datasetToRender.addNamedModel(gName, bareSetModel);
					
					jenaVirtuosoStoreService.updateDataset(apiKey, datasetToRender);
					
//					ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
//					RDFDataMgr.write(outputStream, datasetToRender, RDFLanguages.JSONLD);
//					println outputStream.toString();
					
					Dataset graphs =  openAnnotationSetsVirtuosoService.retrieveAnnotationSet(apiKey, annotationSetUri);
					
					if(graphs!=null && graphs.listNames().hasNext()) {
						Set<Model> toAdd = new HashSet<Model>();
						Set<Statement> statsToAdd = new HashSet<Statement>();
						Set<Statement> toRemove = new HashSet<Statement>();
						Model setModel = graphs.getNamedModel(graphs.listNames().next());
						StmtIterator  iter = setModel.listStatements(null, ResourceFactory.createProperty(AnnotopiaVocabulary.ANNOTATIONS), null);
						while(iter.hasNext()) {
							Statement s = iter.next();
							toRemove.add(s);
							Dataset ds = jenaVirtuosoStoreService.retrieveGraph(apiKey, s.getObject().asResource().getURI());
							if(ds!=null && ds.listNames().hasNext()) {
								Model aModel = ds.getNamedModel(ds.listNames().next());
								StmtIterator  iter2 = aModel.listStatements(null, ResourceFactory.createProperty(RDF.RDF_TYPE), ResourceFactory.createResource(OA.ANNOTATION));
								if(iter2.hasNext()) {
									toAdd.add(aModel);
									//setModel.add(annotationModel);
									statsToAdd.add(ResourceFactory.createStatement(s.getSubject(), ResourceFactory.createProperty(AnnotopiaVocabulary.ANNOTATIONS), iter2.next().getSubject()));
								}
							}
						}
						toRemove.each {
							setModel.remove(it);
						}
						statsToAdd.each {
							setModel.add(it);
						}
						toAdd.each {
							setModel.add(it);
						}
					}
					
					return graphs
					
					// TODO response message
				} else {
					log.info("[" + apiKey + "] Multiple Annotation sets graphs detected... request rejected.");
					def json = JSON.parse('{"status":"rejected" ,"message":"The request carries multiple Annotation Sets graphs."' +
						',"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
					throw new StoreServiceException(200, json, "text/json", "UTF-8");
				}	
			} else {
				log.info("[" + apiKey + "] Multiple Annotation sets detected... request rejected.");
				def json = JSON.parse('{"status":"rejected" ,"message":"The request carries multiple Annotation Sets."' +
					',"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
				throw new StoreServiceException(200, json, "text/json", "UTF-8");
			}
		}
		
		inMemoryDataset
	}
	
	/**
	 * Mints a URI that is shaped according to the passed type.
	 * @param uriType	The type of the URI (graph, annotation, ...)
	 * @return The minted URI
	 */
	public String mintUri(uriType) {
		return grailsApplication.config.grails.server.protocol + '://' +
			grailsApplication.config.grails.server.host + ':' +
			grailsApplication.config.grails.server.port + '/s/' + uriType + '/' +
			org.annotopia.grails.services.storage.utils.UUID.uuid();
	}
	
	/**
	 * Mints a URI of type graph
	 * @return The newly minted graph URI
	 */
	public String mintGraphUri() {
		return mintUri("graph");
	}
}
