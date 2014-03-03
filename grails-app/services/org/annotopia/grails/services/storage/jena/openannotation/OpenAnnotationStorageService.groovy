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

import grails.converters.JSON

import java.text.SimpleDateFormat
import java.util.regex.Pattern

import org.annotopia.groovy.service.store.StoreServiceException
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.RDFLanguages

import com.hp.hpl.jena.query.Dataset
import com.hp.hpl.jena.query.DatasetFactory
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.Property
import com.hp.hpl.jena.rdf.model.Resource
import com.hp.hpl.jena.rdf.model.ResourceFactory
import com.hp.hpl.jena.rdf.model.Statement
import com.hp.hpl.jena.rdf.model.StmtIterator


/**
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class OpenAnnotationStorageService {

	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSz")
	
	def grailsApplication
	def jenaUtilsService
	def jenaVirtuosoStoreService
	def graphMetadataService
	def openAnnotationUtilsService
	def openAnnotationVirtuosoService
	
	/**
	 * Lists the ann...
	 * @param apiKey
	 * @param max
	 * @param offset
	 * @param tgtUrl
	 * @param tgtFgt
	 * @param tgtExt
	 * @param tgtIds
	 * @return
	 */
	public listAnnotation(apiKey, max, offset, tgtUrl, tgtFgt, tgtExt, tgtIds, incGph) {
		log.info '[' + apiKey + '] List annotations' +
			' max:' + max +
			' offset:' + offset +
			' incGph:' + incGph +
			' tgtUrl:' + tgtUrl +
			' tgtFgt:' + tgtFgt;
		retrieveAnnotationGraphs(apiKey, max, offset, tgtUrl, tgtFgt, incGph);
	}
	
	public Set<Dataset> retrieveAnnotationGraphs(apiKey, max, offset, tgtUrl, tgtFgt, incGph) {
		log.info '[' + apiKey + '] Retrieving annotation graphs';
	
		Set<Dataset> datasets = new HashSet<Dataset>();
		List<String> graphNames = openAnnotationVirtuosoService.retrieveAnnotationGraphsNames(apiKey, max, offset, tgtUrl, tgtFgt);
		if(graphNames!=null) {
			graphNames.each { graphName ->
				Dataset ds = jenaVirtuosoStoreService.retrieveGraph(apiKey, graphName);
				if(incGph=='true') {
					Model m = jenaVirtuosoStoreService.retrieveGraphMetadata(apiKey, graphName, "annotopia:graphs:provenance");
					if(m!=null) ds.setDefaultModel(m);
				}
				if(ds!=null) datasets.add(ds);
			}
		}
		return datasets;
	}
	
	/**
	 * Saves the annotation Dataset. The service now accept one single item
	 * for each given request. However, the request can include multiple 
	 * graphs. The method uses Dataset for supporting multiple graphs. 
	 * @param apiKey		The API key of the client issuing the request
	 * @param startTime		The start time used for calculating the total elapsed time
	 * @param dataset		The Dataset with the annotation content
	 * @return The Dataset with the saved (persisted) content. 
	 */
	public Dataset saveAnnotationDataset(apiKey, startTime, Dataset dataset) {
		
//		def addCreationDetails = { model, resource ->
//			model.add(resource, ResourceFactory.createProperty("http://purl.org/pav/createdAt"), ResourceFactory.createPlainLiteral(dateFormat.format(new Date())));
//			model.add(resource, ResourceFactory.createProperty("http://purl.org/pav/lastUpdatedOn"), ResourceFactory.createPlainLiteral(dateFormat.format(new Date())));
//		}
		
		// Registry of the URIs of the annotations.
		// Note: The method currently supports the saving of one annotation at a time
		Set<Resource> annotationUris = new HashSet<Resource>();
		
		// Registry of all named graphs in the transaction
		Set<Resource> graphsUris = jenaUtilsService.detectNamedGraphs(apiKey, dataset);

		// Detection of default graph
		int annotationsInDefaultGraphsCounter = openAnnotationUtilsService.detectAnnotationsInDefaultGraph(apiKey, dataset, annotationUris, null)
		boolean defaultGraphDetected = (annotationsInDefaultGraphsCounter>0);
		
		// Query for graphs containing annotation
		// See: https://www.mail-archive.com/wikidata-l@lists.wikimedia.org/msg00370.html
		Set<Resource> annotationsGraphsUris = new HashSet<Resource>();
		int detectedAnnotationGraphsCounter = openAnnotationUtilsService.detectAnnotationsInNamedGraph(
			apiKey, dataset, graphsUris, annotationsGraphsUris, annotationUris, null)
		
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
		
		String content;
		if(detectedAnnotationGraphsCounter>0) {
			// Query Specific Resources
			Set<Resource> specificResourcesUris = new HashSet<Resource>();
			int totalDetectedSpecificResources = openAnnotationUtilsService
				.detectSpecificResourcesAsNamedGraphs(apiKey, dataset,specificResourcesUris)
			
			// Query Content As Text
			Set<Resource> embeddedTextualBodiesUris = new HashSet<Resource>();
			int totalEmbeddedTextualBodies = openAnnotationUtilsService
				.detectContextAsTextInNamedGraphs(apiKey, dataset, embeddedTextualBodiesUris);
				
			// Query Bodies as Named Graphs
			Set<Resource> bodiesGraphsUris = new HashSet<Resource>();
			int totalBodiesAsGraphs = openAnnotationUtilsService
				.detectBodiesAsNamedGraphs(apiKey, dataset, bodiesGraphsUris);
				
			Dataset workingDataset = dataset;
			Dataset creationDataset = DatasetFactory.createMem();
			
			// Swap annotation id
			// Swap resource id
			// Swap content id
			Iterator<Resource> annotationsGraphsUrisIterator = annotationsGraphsUris.iterator();
			if(annotationsGraphsUrisIterator.hasNext()) {
				Resource annotationGraphUri = annotationsGraphsUrisIterator.next();
				Resource annotation = identifiableURIs(apiKey, workingDataset.getNamedModel(annotationGraphUri.toString()),
					ResourceFactory.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
					ResourceFactory.createResource("http://www.w3.org/ns/oa#Annotation"), "annotation");
				
				identifiableURIs(apiKey, workingDataset.getNamedModel(annotationGraphUri.toString()),
					ResourceFactory.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
					ResourceFactory.createResource("http://www.w3.org/ns/oa#SpecificResource"), "resource");
				
				identifiableURIs(apiKey, workingDataset.getNamedModel(annotationGraphUri.toString()),
					ResourceFactory.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
					ResourceFactory.createResource("http://www.w3.org/2011/content#ContentAsText"), "content");
				
				// Swap annotation graph id and create metadata
				def newAnnotationGraphUri = getGraphUri();
				creationDataset.addNamedModel(newAnnotationGraphUri, workingDataset.getNamedModel(annotationGraphUri.toString()));
				Model newAnnotationGraphMetadataModel = 
					graphMetadataService.getAnnotationGraphCreationMetadata(apiKey, creationDataset, newAnnotationGraphUri);
				newAnnotationGraphMetadataModel.add(
					ResourceFactory.createResource(newAnnotationGraphUri), 
					ResourceFactory.createProperty("http://purl.org/pav/previousVersion"), 
					ResourceFactory.createResource(annotationGraphUri.toString()));
				
				// Swap body graph id and create metadata
				Iterator<Resource> bodiesGraphsUrisIterator = bodiesGraphsUris.iterator();
				if(bodiesGraphsUrisIterator.hasNext()) {
					Resource bodyGraphUri = bodiesGraphsUrisIterator.next();
					def newBodyGraphUri = getGraphUri();
					creationDataset.addNamedModel(newBodyGraphUri, workingDataset.getNamedModel(bodyGraphUri.toString()));
					Model newBodyGraphMetadataModel = 
						graphMetadataService.getBodyGraphCreationMetadata(apiKey, creationDataset, newBodyGraphUri);
					newBodyGraphMetadataModel.add(
						ResourceFactory.createResource(newBodyGraphUri),
						ResourceFactory.createProperty("http://purl.org/pav/previousVersion"),
						ResourceFactory.createResource(bodyGraphUri.toString()));
					
					Model annotationModel = workingDataset.getNamedModel(annotationGraphUri.toString());
					StmtIterator statements = annotationModel.listStatements(annotation, 
						ResourceFactory.createProperty("http://www.w3.org/ns/oa#hasBody"), null);
					List<Statement> statementsToRemove = new ArrayList<Statement>();
					statementsToRemove.addAll(statements);
					statementsToRemove.each { statement ->
						annotationModel.remove(statement);
						annotationModel.add(annotation, ResourceFactory.createProperty("http://www.w3.org/ns/oa#hasBody"), 
							ResourceFactory.createResource(newBodyGraphUri));
					}
					// Update the main annotation Graph
					newAnnotationGraphMetadataModel.add(
						ResourceFactory.createResource(newAnnotationGraphUri), 
						ResourceFactory.createProperty("http://purl.org/annotopia#body"),
						ResourceFactory.createResource(newBodyGraphUri));
				}
			}			
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			RDFDataMgr.write(outputStream, creationDataset, RDFLanguages.JSONLD);
			println outputStream.toString();
			
			jenaVirtuosoStoreService.storeDataset(apiKey, creationDataset);
			return creationDataset;
		} else if(defaultGraphDetected) {
			Dataset workingDataset = dataset;

			identifiableURIs(apiKey, workingDataset.getDefaultModel(),
				ResourceFactory.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
				ResourceFactory.createResource("http://www.w3.org/ns/oa#Annotation"), "annotation");
			
			identifiableURIs(apiKey, workingDataset.getDefaultModel(),
				ResourceFactory.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
				ResourceFactory.createResource("http://www.w3.org/ns/oa#SpecificResource"), "resource");

			identifiableURIs(apiKey, workingDataset.getDefaultModel(),
				ResourceFactory.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
				ResourceFactory.createResource("http://www.w3.org/2011/content#ContentAsText"), "content");
			
			def graphUri = getGraphUri();
			Dataset creationDataset = DatasetFactory.createMem();
			creationDataset.addNamedModel(graphUri, workingDataset.getDefaultModel());
			
			graphMetadataService.getAnnotationGraphCreationMetadata(apiKey, creationDataset, graphUri);			
			jenaVirtuosoStoreService.storeDataset(apiKey, creationDataset);
			return creationDataset;
		} else {
			// Annotation Set not found
			log.info("[" + apiKey + "] Annotation not found " + content);
			def json = JSON.parse('{"status":"nocontent" ,"message":"The request does not carry acceptable payload or payload cannot be read"' +
				',"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
			throw new StoreServiceException(200, json, "text/json", "UTF-8");
		}
	}
	
	public Dataset updateAnnotationDataset(apiKey, startTime, Dataset dataset) {
		
//		def addUpdateDetails = { model, resource ->
//			model.add(resource, ResourceFactory.createProperty("http://purl.org/pav/lastUpdatedOn"),
//				ResourceFactory.createPlainLiteral(dateFormat.format(new Date())));
//		}
		
		// Detect all named graphs
		Set<Resource> graphsUris = jenaUtilsService.detectNamedGraphs(apiKey, dataset);

		// Detection of default graph
		Set<Resource> annotationUris = new HashSet<Resource>();
		int annotationsInDefaultGraphsCounter = openAnnotationUtilsService.detectAnnotationsInDefaultGraph(apiKey, dataset, annotationUris, null)
		boolean defaultGraphDetected = (annotationsInDefaultGraphsCounter>0);
		
		// Query for graphs containing annotation
		// See: https://www.mail-archive.com/wikidata-l@lists.wikimedia.org/msg00370.html
		Set<Resource> annotationsGraphsUris = new HashSet<Resource>();
		int detectedAnnotationGraphsCounter = openAnnotationUtilsService.detectAnnotationsInNamedGraph(
			apiKey, dataset, graphsUris, annotationsGraphsUris, annotationUris, null)

		if(defaultGraphDetected && detectedAnnotationGraphsCounter>0) {
			log.info("[" + apiKey + "] Mixed Annotation content detected... request rejected.");
			def json = JSON.parse('{"status":"nocontent" ,"message":"The request carries a mix of Annotations and Annotation Graphs"' +
				',"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
			throw new StoreServiceException(200, json, "text/json", "UTF-8");
		} else if(annotationUris.size()>1) {
			// Annotation Set not found
			log.info("[" + apiKey + "] Multiple Annotation graphs detected... request rejected.");
			def json = JSON.parse('{"status":"nocontent" ,"message":"The request carries multiple Annotations or Annotation Graphs"' +
				',"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
			throw new StoreServiceException(200, json, "text/json", "UTF-8");
		}
		
		String content;
		if(detectedAnnotationGraphsCounter>0) {
			// Query Specific Resources
			Set<Resource> specificResourcesUris = new HashSet<Resource>();
			int totalDetectedSpecificResources = openAnnotationUtilsService
				.detectSpecificResourcesAsNamedGraphs(apiKey, dataset,specificResourcesUris)

			// Query Content As Text
			Set<Resource> embeddedTextualBodiesUris = new HashSet<Resource>();
			int totalEmbeddedTextualBodies = openAnnotationUtilsService
				.detectContextAsTextInNamedGraphs(apiKey, dataset, embeddedTextualBodiesUris);
				
			
			
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			RDFDataMgr.write(outputStream, dataset, RDFLanguages.JSONLD);
			content = outputStream.toString();
			
			Dataset workingDataset = DatasetFactory.createMem();
			RDFDataMgr.read(workingDataset, new ByteArrayInputStream(content.toString().getBytes("UTF-8")), RDFLanguages.JSONLD);
			
			jenaVirtuosoStoreService.updateDataset(apiKey, workingDataset);
			
			return workingDataset;
		} else if(defaultGraphDetected) {
			String annotationUri;
			annotationUris.each{ annotationUri = it }
			
			if(annotationUri!=null) {
				String graphName;
				Set<String> names = openAnnotationVirtuosoService.retrieveAnnotationGraphNames(apiKey, annotationUri)
				names.each { graphName = it }
				if(graphName!=null) {
					Dataset storedAnnotationDataset = openAnnotationVirtuosoService.retrieveAnnotation(apiKey, annotationUri);
					
					// TODO Bodies management has to be perfected to make sure the URIs of modified entities
					// are mantained with a precise criteria. For now, when updating the URIs are kept while 
					// URNs and blanks are replaced.
					
					ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
					RDFDataMgr.write(outputStream, dataset, RDFLanguages.JSONLD);
					println outputStream.toString();
					
					// Find body URI
					// Query Content As Text
					Set<Resource> embeddedTextualBodiesUris = new HashSet<Resource>();
					int totalEmbeddedTextualBodies = openAnnotationUtilsService
						.detectContextAsTextInDefaultGraph(apiKey, dataset, embeddedTextualBodiesUris);
						
					Set<Resource> storedEmbeddedTextualBodiesUris = new HashSet<Resource>();
					int totalStoredEmbeddedTextualBodies = openAnnotationUtilsService
						.detectContextAsTextInNamedGraphs(apiKey, storedAnnotationDataset, storedEmbeddedTextualBodiesUris);
					
					if(embeddedTextualBodiesUris.equals(storedEmbeddedTextualBodiesUris)) {
						println 'same bodies yay'
					} else {
						println 'different bodies'
						println 'update: ' + embeddedTextualBodiesUris
						println 'stored: ' + storedEmbeddedTextualBodiesUris
					}					
					
					Model newDefaultModel = dataset.getDefaultModel();
					Dataset updateDataset = DatasetFactory.createMem();
					updateDataset.addNamedModel(graphName, newDefaultModel);

					// Find metadata graph			
					def annotationGraphUri;
					storedAnnotationDataset.listNames().each { annotationGraphUri = it }
					
					Model metaModel = jenaVirtuosoStoreService.retrieveGraphMetadata(apiKey, annotationGraphUri, "annotopia:graphs:provenance");
					graphMetadataService.getAnnotationGraphUpdateMetadata(apiKey, metaModel, annotationGraphUri);					
					updateDataset.addNamedModel("annotopia:graphs:provenance", metaModel);
					
					ByteArrayOutputStream outputStream3 = new ByteArrayOutputStream();
					RDFDataMgr.write(outputStream3, updateDataset, RDFLanguages.JSONLD);
					println outputStream3.toString();
					
					jenaVirtuosoStoreService.updateGraphMetadata(apiKey, metaModel, annotationGraphUri, "annotopia:graphs:provenance")
					
					jenaVirtuosoStoreService.updateDataset(apiKey, updateDataset);
					
					return updateDataset;
				} else {
					// Annotation not found
					log.info("[" + apiKey + "] Annotation not found " + content);
					def json = JSON.parse('{"status":"nocontent" ,"message":"The requested Annotation cannot be updated as it has not been found"' +
						',"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
					throw new StoreServiceException(200, json, "text/json", "UTF-8");
				}
			} else {
				// Annotation not found
				log.info("[" + apiKey + "] Annotation not found " + content);
				def json = JSON.parse('{"status":"nocontent" ,"message":"The requested Annotation cannot be updated as it has not been found"' +
					',"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
				throw new StoreServiceException(200, json, "text/json", "UTF-8");
			}
		} else {
			// Annotation Set not found
			log.info("[" + apiKey + "] Annotation to be updated not found " + content);
			def json = JSON.parse('{"status":"nocontent" ,"message":"The requested Annotation cannot be updated as it has not been found"' +
				',"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
			throw new StoreServiceException(200, json, "text/json", "UTF-8");
		}
	}
	
	public String getGraphUri() {
		return 'http://' + grailsApplication.config.grails.server.host + ':' +
			grailsApplication.config.grails.server.port.http + '/s/graph/' +
			org.annotopia.grails.services.storage.utils.UUID.uuid();
	}
	
	private String identifiableURIs(String apiKey, String content, Set<Resource> uris, String uriType) {
		String toReturn = content;
		uris.each { uri ->
			def uuid = org.annotopia.grails.services.storage.utils.UUID.uuid();
			def nUri = 'http://' + grailsApplication.config.grails.server.host + ':' +
							grailsApplication.config.grails.server.port.http + '/s/' + uriType + '/' + uuid;

			log.info("[" + apiKey + "] Minting URIs (1) " + uriType + " " + uri.toString() + " -> " + nUri);
			if(uri.isAnon()) {
				println 'blank ' + uri.getId();
				toReturn = content.replaceAll(Pattern.quote("\"@id\" : \"" + uri + "\""),
					"\"@id\" : \"" + nUri + "\"" + ",\"http://purl.org/pav/previousVersion\" : \"blank\"");
			} else
			toReturn = content.replaceAll(Pattern.quote("\"@id\" : \"" + uri + "\""),
				"\"@id\" : \"" + nUri + "\"" + ",\"http://purl.org/pav/previousVersion\" : \"" + uri.toString() + "\"");
		}
		return toReturn;
	}
	
	private Resource identifiableURIs(String apiKey, Model model, Property property, Resource resource, String uriType) {
		
		log.info("[" + apiKey + "] Minting URIs (2) " + uriType + " " + resource.toString());
		
		def uuid = org.annotopia.grails.services.storage.utils.UUID.uuid();
		def nUri = 'http://' + grailsApplication.config.grails.server.host + ':' +
						grailsApplication.config.grails.server.port.http + '/s/' + uriType + '/' + uuid;
		def rUri = ResourceFactory.createResource(nUri);

		def originalSubject;
		List<Statement> s = new ArrayList<Statement>();
		StmtIterator statements = model.listStatements(null, property, resource);
		statements.each {
			originalSubject =  it.getSubject()
			StmtIterator statements2 = model.listStatements(it.getSubject(), null, null);
			statements2 .each { its ->
				s.add(model.createStatement(rUri, its.getPredicate(), its.getObject()));
			}
		}
		if(originalSubject==null) {
			log.info("[" + apiKey + "] Skipping URI " + uriType + " " + resource.toString());
			return;
		}
		model.removeAll(originalSubject, null, null);
	
		StmtIterator statements3 = model.listStatements(null, null, originalSubject);
		statements3.each { its2 ->
			s.add(model.createStatement(its2.getSubject(), its2.getPredicate(), rUri));
		}
		model.removeAll(null, null, originalSubject);
			
		s.each { model.add(it); }
		
		if(!originalSubject.isAnon())
			model.add(model.createStatement(rUri,
				ResourceFactory.createProperty("http://purl.org/pav/previousVersion"),
				ResourceFactory.createPlainLiteral(originalSubject.toString())
				));
		else
			model.add(model.createStatement(rUri,
				ResourceFactory.createProperty("http://purl.org/pav/previousVersion"),
				ResourceFactory.createPlainLiteral("blank")
				));
			
		rUri;
	}
}
