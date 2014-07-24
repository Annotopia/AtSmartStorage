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

import org.annotopia.grails.vocabularies.AnnotopiaVocabulary
import org.annotopia.grails.vocabularies.OA
import org.annotopia.grails.vocabularies.PAV
import org.annotopia.grails.vocabularies.RDF
import org.annotopia.groovy.service.store.StoreServiceException
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.RDFLanguages

import com.hp.hpl.jena.query.Dataset
import com.hp.hpl.jena.query.DatasetFactory
import com.hp.hpl.jena.query.QueryExecution
import com.hp.hpl.jena.query.QueryExecutionFactory
import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.query.QuerySolution
import com.hp.hpl.jena.query.ResultSet
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

	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
	
	// incGph (Include graph) constants
	private final INCGPH_YES = "true";
	private final INCGPH_NO = "false";
	
	def jenaUtilsService
	def grailsApplication
	def graphMetadataService
	def jenaVirtuosoStoreService
	def openAnnotationUtilsService
	def openAnnotationVirtuosoService
	def graphIdentifiersMetadataService
	
	
	/**
	 * Retrieves the available annotations given the specified parameters.
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
	 * @return The list of annotations meeting the given criteria
	 */
	public listAnnotation(apiKey, max, offset, List<String> tgtUrls, tgtFgt, tgtExt, tgtIds, incGph) {
		log.info '[' + apiKey + '] Listing annotations' +
			' max:' + max +
			' offset:' + offset +
			' incGph:' + incGph +
			' tgtUrl:' + tgtUrls +
			' tgtFgt:' + tgtFgt;
			
		Set<Dataset> datasets = new HashSet<Dataset>();
		Set<String> graphNames = openAnnotationVirtuosoService.retrieveAnnotationGraphsNames(apiKey, max, offset, tgtUrls, tgtFgt);
		if(graphNames!=null) {
			graphNames.each { graphName ->
				Dataset ds = jenaVirtuosoStoreService.retrieveGraph(apiKey, graphName);
				
				if(ds!=null) {
					List<Statement> statementsToRemove = new ArrayList<Statement>();
					Set<Resource> subjectsToRemove = new HashSet<Resource>();
					Iterator<String> names = ds.listNames();
					names.each { name ->
						Model m = ds.getNamedModel(name);
						// Remove AnnotationSets data and leave oa:Annotation
						StmtIterator statements = m.listStatements(null, ResourceFactory.createProperty(RDF.RDF_TYPE), ResourceFactory.createResource(AnnotopiaVocabulary.ANNOTATION_SET));
						statements.each { statement ->
							subjectsToRemove.add(statement.getSubject())
						}
						
						subjectsToRemove.each { subjectToRemove ->
							m.removeAll(subjectToRemove, null, null);
						}
					}
				}
				
				if(incGph==INCGPH_YES) {
					Model m = jenaVirtuosoStoreService.retrieveGraphMetadata(apiKey, graphName, grailsApplication.config.annotopia.storage.uri.graph.provenance);
					if(m!=null) ds.setDefaultModel(m);
				}
				if(ds!=null) datasets.add(ds);
			}
		}
		return datasets;
	}
	
//	/**
//	 * Lists the ann...
//	 * @param apiKey
//	 * @param max
//	 * @param offset
//	 * @param tgtUrl
//	 * @param tgtFgt
//	 * @param tgtExt
//	 * @param tgtIds
//	 * @return
//	 */
//	public listAnnotation(apiKey, max, offset, tgtUrl, tgtFgt, tgtExt, tgtIds, incGph) {
//		log.info '[' + apiKey + '] List annotations' +
//			' max:' + max +
//			' offset:' + offset +
//			' incGph:' + incGph +
//			' tgtUrl:' + tgtUrl +
//			' tgtFgt:' + tgtFgt;
//		retrieveAnnotationGraphs(apiKey, max, offset, tgtUrl, tgtFgt, incGph);
//	}
//	
//	public Set<Dataset> retrieveAnnotationGraphs(apiKey, max, offset, tgtUrl, tgtFgt, incGph) {
//		log.info '[' + apiKey + '] Retrieving annotation graphs';
//	
//		Set<Dataset> datasets = new HashSet<Dataset>();
//		Set<String> graphNames = openAnnotationVirtuosoService.retrieveAnnotationGraphsNames(apiKey, max, offset, tgtUrl, tgtFgt);
//		if(graphNames!=null) {
//			graphNames.each { graphName ->
//				Dataset ds = jenaVirtuosoStoreService.retrieveGraph(apiKey, graphName);
//				
//				if(ds!=null) {
//					List<Statement> statementsToRemove = new ArrayList<Statement>();
//					Set<Resource> subjectsToRemove = new HashSet<Resource>();
//					Iterator<String> names = ds.listNames();
//					names.each { name ->
//						Model m = ds.getNamedModel(name);
//						// Remove AnnotationSets data and leave oa:Annotation
//						StmtIterator statements = m.listStatements(null, ResourceFactory.createProperty(RDF.RDF_TYPE), ResourceFactory.createResource(AnnotopiaVocabulary.ANNOTATION_SET));
//						statements.each { statement ->
//							subjectsToRemove.add(statement.getSubject())
//						}		
//						
//						subjectsToRemove.each { subjectToRemove ->
//							m.removeAll(subjectToRemove, null, null);
//						}
//					}
//				}
//				
//				if(incGph==INCGPH_YES) {
//					Model m = jenaVirtuosoStoreService.retrieveGraphMetadata(apiKey, graphName, grailsApplication.config.annotopia.storage.uri.graph.provenance);
//					if(m!=null) ds.setDefaultModel(m);
//				}
//				if(ds!=null) datasets.add(ds);
//			}
//		}
//		return datasets;
//	}
	
	/**
	 * Saves the annotation Dataset. The service now accept one single item
	 * for each given request. However, the request can include multiple 
	 * graphs. The method uses Dataset for supporting multiple graphs. 
	 * @param apiKey		The API key of the client issuing the request
	 * @param startTime		The start time used for calculating the total elapsed time
	 * @param dataset		The Dataset with the annotation content
	 * @return The Dataset with the saved (persisted) content. 
	 */
	public Dataset saveAnnotationDataset(String apiKey, Long startTime, Boolean incGph, Dataset dataset) {
		
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
		
		// Verifies the existence of an annotation target
		if(true) {
			int detectedAnnotationTarget = 
				openAnnotationUtilsService.detectAnnotationTarget(apiKey, dataset);
			if(detectedAnnotationTarget==0) {
				ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
				RDFDataMgr.write(outputStream, dataset, RDFLanguages.JSONLD);
				log.info("[" + apiKey + "] Annotation target not found " + outputStream.toString());
				def json = JSON.parse('{"statusCode":"nocontent" ,"statusMessage":"oa:hasTarget not found"' +
					',"message":"The request does not carry acceptable payload or payload cannot be read"' +
					',"duration":"' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
				throw new StoreServiceException(200, json, "text/json", "UTF-8");
			}
		}
		
		// Verifies the existence of the annotator 
		// Currently disabled
		if(false) {
			int detectedAnnotatorInfo =
				openAnnotationUtilsService.detectAnnotatorInfo(apiKey, dataset);
			if(detectedAnnotatorInfo==0) {
				ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
				RDFDataMgr.write(outputStream, dataset, RDFLanguages.JSONLD);
				log.info("[" + apiKey + "] Annotator info not found " + outputStream.toString());
				def json = JSON.parse('{"statusCode":"nocontent" ,"statusMessage":"oa:annotatedBy not found"' +
					',"message":"The request does not carry acceptable payload or payload cannot be read"' +
					',"duration":"' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
				throw new StoreServiceException(200, json, "text/json", "UTF-8");
			}
		}
		
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
		

		Map<String,String> identifiers = new HashMap<String,String>();
		
		String content; 
		if(defaultGraphDetected) {
			log.trace("[" + apiKey + "] Default graph detected.");
			// If the content is expressed in the default graph, it is necessary 
			// to swap blank nodes and temporary URIs with persistent URIs
			// NOTE: This is supporting right now a single body			
			
			// Annotation identifier
			Resource annotation = persistURI(apiKey, dataset.getDefaultModel(),
				ResourceFactory.createProperty(RDF.RDF_TYPE),
				ResourceFactory.createResource(OA.ANNOTATION), "annotation");
			
			// Specific Resource identifier
			persistURIs(apiKey, dataset.getDefaultModel(),
				ResourceFactory.createProperty(RDF.RDF_TYPE),
				ResourceFactory.createResource(OA.SPECIFIC_RESOURCE), "resource");

			// Embedded content (as RDF) identifier
			persistURIs(apiKey, dataset.getDefaultModel(),
				ResourceFactory.createProperty(RDF.RDF_TYPE),
				ResourceFactory.createResource(OA.CONTEXT_AS_TEXT), "content");
			
			// TODO Tags management
			// TODO Extension points
			
			// Minting of the URI for the Named Graph that will wrap the
			// default graph
			def graphUri = mintGraphUri();
			Dataset creationDataset = DatasetFactory.createMem();
			creationDataset.addNamedModel(graphUri, dataset.getDefaultModel());
			
			// Identity management
			def identifierUri = mintUri("expression");
			openAnnotationUtilsService.detectTargetIdentifiersInDefaultGraph(apiKey, dataset, identifiers)		
			Model identifiersModel = jenaVirtuosoStoreService.retrieveGraphIdentifiersMetadata(apiKey, identifiers, grailsApplication.config.annotopia.storage.uri.graph.identifiers);
			//jenaUtilsService.getDatasetAsString(identifiersModel);
			// If no identifiers are found for this resource we create the identifiers metadata.
			if(identifiersModel!=null) {
				if(identifiersModel.empty) {
					graphIdentifiersMetadataService.getIdentifiersGraphMetadata(apiKey, creationDataset, identifierUri, identifiers);
				} else {
					graphIdentifiersMetadataService.updateIdentifiersGraphMetadata(apiKey, creationDataset, identifiersModel, identifiers);
				}
			}
			
			// Creation of the metadata for the Graph wrapper
			def graphResource = ResourceFactory.createResource(graphUri);
			Model metaModel = graphMetadataService.getAnnotationGraphCreationMetadata(apiKey, creationDataset, graphUri);
			metaModel.add(graphResource, ResourceFactory.createProperty(AnnotopiaVocabulary.ANNOTATION), ResourceFactory.createPlainLiteral(annotation.toString()));
			metaModel.add(graphResource, ResourceFactory.createProperty(AnnotopiaVocabulary.ANNOTATION_COUNT), ResourceFactory.createPlainLiteral("1"));
			
			jenaVirtuosoStoreService.storeDataset(apiKey, creationDataset);
			
			Dataset storedDataset = DatasetFactory.createMem();
			storedDataset.addNamedModel(graphUri, dataset.getDefaultModel());
			
			if(incGph) return creationDataset;
			else return storedDataset;
		} else if(detectedAnnotationGraphsCounter>0) {
			// If multiple graphs are detected, the logic is different as there can be one 
		    // or more graphs as bodies. 
		    // NOTE: This is tested right now only for a single body
		
			openAnnotationUtilsService.detectTargetIdentifiers(apiKey, dataset, identifiers);
		
			// Detecting Specific Resources
			Set<Resource> specificResourcesUris = new HashSet<Resource>();
			int totalDetectedSpecificResources = openAnnotationUtilsService
				.detectSpecificResourcesAsNamedGraphs(apiKey, dataset,specificResourcesUris)
			
			// Detecting Content As Text
			Set<Resource> embeddedTextualBodiesUris = new HashSet<Resource>();
			int totalEmbeddedTextualBodies = openAnnotationUtilsService
				.detectContextAsTextInNamedGraphs(apiKey, dataset, embeddedTextualBodiesUris);
				
			// Detecting Bodies as Named Graphs
			Set<Resource> bodiesGraphsUris = new HashSet<Resource>();
			int totalBodiesAsGraphs = openAnnotationUtilsService
				.detectBodiesAsNamedGraphs(apiKey, dataset, bodiesGraphsUris);
				
			Dataset workingDataset = dataset;
			Dataset creationDataset = DatasetFactory.createMem();
			Dataset storedDataset = DatasetFactory.createMem();
			
			// This is iterating over multiple annotation graphs.
			// NOTE: only one iteration as only one annotation graph is currently accepted
			Iterator<Resource> annotationsGraphsUrisIterator = annotationsGraphsUris.iterator();
			if(annotationsGraphsUrisIterator.hasNext()) {
				Resource annotationGraphUri = annotationsGraphsUrisIterator.next();
				
				// Annotation identifier
				Resource annotation = persistURI(apiKey, workingDataset.getNamedModel(annotationGraphUri.toString()),
					ResourceFactory.createProperty(RDF.RDF_TYPE),
					ResourceFactory.createResource(OA.ANNOTATION), "annotation");
				
				// Specific Resource identifier
				persistURIs(apiKey, workingDataset.getNamedModel(annotationGraphUri.toString()),
					ResourceFactory.createProperty(RDF.RDF_TYPE),
					ResourceFactory.createResource(OA.SPECIFIC_RESOURCE), "resource");
				
				// Embedded content (as RDF) identifier
				persistURIs(apiKey, workingDataset.getNamedModel(annotationGraphUri.toString()),
					ResourceFactory.createProperty(RDF.RDF_TYPE),
					ResourceFactory.createResource(OA.CONTEXT_AS_TEXT), "content");
				
				// Annotation graphs identifier
				def newAnnotationGraphUri = mintGraphUri();
				creationDataset.addNamedModel(newAnnotationGraphUri, workingDataset.getNamedModel(annotationGraphUri.toString()));
				storedDataset.addNamedModel(newAnnotationGraphUri, workingDataset.getNamedModel(annotationGraphUri.toString()));
				
				// Annotation graphs metadata
				Model newAnnotationGraphMetadataModel = 
					graphMetadataService.getAnnotationGraphCreationMetadata(apiKey, creationDataset, newAnnotationGraphUri);
				newAnnotationGraphMetadataModel.add(ResourceFactory.createResource(newAnnotationGraphUri), ResourceFactory.createProperty(AnnotopiaVocabulary.ANNOTATION), ResourceFactory.createPlainLiteral(annotation.toString()));
				newAnnotationGraphMetadataModel.add(ResourceFactory.createResource(newAnnotationGraphUri), ResourceFactory.createProperty(AnnotopiaVocabulary.ANNOTATION_COUNT), ResourceFactory.createPlainLiteral("1"));
				newAnnotationGraphMetadataModel.add(
					ResourceFactory.createResource(newAnnotationGraphUri), 
					ResourceFactory.createProperty(PAV.PAV_PREVIOUS_VERSION), 
					ResourceFactory.createResource(annotationGraphUri.toString()));
				
				if(bodiesGraphsUris.size()>0) {
					HashMap<String, String> oldNewBodyUriMapping = new HashMap<String, String>();
					Iterator<Resource> bodiesGraphsUrisIterator = bodiesGraphsUris.iterator();
					while(bodiesGraphsUrisIterator.hasNext()) {						
						// Bodies graphs identifiers
						Resource bodyGraphUri = bodiesGraphsUrisIterator.next();
						def newBodyGraphUri = mintGraphUri();
						creationDataset.addNamedModel(newBodyGraphUri, workingDataset.getNamedModel(bodyGraphUri.toString()));
						storedDataset.addNamedModel(newBodyGraphUri, workingDataset.getNamedModel(bodyGraphUri.toString()));
						oldNewBodyUriMapping.put(bodyGraphUri.toString(), newBodyGraphUri);
						
						// Bodies graphs metadata
						Model newBodyGraphMetadataModel = 
							graphMetadataService.getBodyGraphCreationMetadata(apiKey, creationDataset, newBodyGraphUri);
						newBodyGraphMetadataModel.add(
							ResourceFactory.createResource(newBodyGraphUri),
							ResourceFactory.createProperty(PAV.PAV_PREVIOUS_VERSION),
							ResourceFactory.createResource(bodyGraphUri.toString()));
					}
						
					// Update Bodies graphs URIs
					oldNewBodyUriMapping.keySet().each { oldUri ->
						Model annotationModel = workingDataset.getNamedModel(annotationGraphUri.toString());
						StmtIterator statements = annotationModel.listStatements(annotation, 
							ResourceFactory.createProperty(OA.HAS_BODY), ResourceFactory.createResource(oldUri));
						List<Statement> statementsToRemove = new ArrayList<Statement>();
						statementsToRemove.addAll(statements);
						statementsToRemove.each { statement ->
							annotationModel.remove(statement);
							annotationModel.add(annotation, ResourceFactory.createProperty(OA.HAS_BODY), 
								ResourceFactory.createResource(oldNewBodyUriMapping.get(oldUri)));
							// Adding trig:Graph type
							annotationModel.add(ResourceFactory.createResource(oldNewBodyUriMapping.get(oldUri)), 
								ResourceFactory.createProperty(RDF.RDF_TYPE),
								ResourceFactory.createResource(OA.GRAPH));
						}
						// Graphs metadata linkage (Annotation Graph to Bodies Graphs)
						newAnnotationGraphMetadataModel.add(
							ResourceFactory.createResource(newAnnotationGraphUri), 
							ResourceFactory.createProperty(AnnotopiaVocabulary.BODY),
							ResourceFactory.createResource(oldNewBodyUriMapping.get(oldUri)));
					}
					if(oldNewBodyUriMapping.values().size()>0) {
						newAnnotationGraphMetadataModel.add(ResourceFactory.createResource(newAnnotationGraphUri),
							ResourceFactory.createProperty(AnnotopiaVocabulary.BODIES_COUNT), ResourceFactory.createPlainLiteral(""+oldNewBodyUriMapping.values().size()));
					}
					
				} else {
					if(graphsUris.size()>0) {
						log.info("[" + apiKey + "] Anonymous body Graph detected... request rejected.");
						def json = JSON.parse('{"status":"nocontent" ,"message":"The request carries anonymous body Graphs"' +
							',"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
						throw new StoreServiceException(501, json, "text/json", "UTF-8");
					}
				}
			}
			jenaVirtuosoStoreService.storeDataset(apiKey, creationDataset);
			
			if(incGph) return creationDataset;
			else return storedDataset;
		} else {
			// Annotation Set not found
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			RDFDataMgr.write(outputStream, dataset, RDFLanguages.JSONLD);
			log.info("[" + apiKey + "] Annotation not found " + outputStream.toString());
			def json = JSON.parse('{"statusCode":"nocontent" ,"statusMessage":"oa:Annotation type not found"' +
				',"message":"The request does not carry acceptable payload or payload cannot be read"' +
				',"duration":"' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
			throw new StoreServiceException(200, json, "text/json", "UTF-8");
		}
	}
	
	public Dataset updateAnnotationDataset(apiKey, startTime, Boolean incGph,  Dataset dataset) {
		
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
			log.trace("[" + apiKey + "] Named graphs detected.");
			
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
			
			if(incGph) return workingDataset;
			else return workingDataset;
		} else if(defaultGraphDetected) {
			log.trace("[" + apiKey + "] Default graph detected.");
			
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
					
					Dataset storedDataset = DatasetFactory.createMem();
					storedDataset.addNamedModel(graphName, newDefaultModel);
					
					// Find metadata graph			
					def annotationGraphUri;
					storedAnnotationDataset.listNames().each { annotationGraphUri = it }
					
					Model metaModel = jenaVirtuosoStoreService.retrieveGraphMetadata(apiKey, annotationGraphUri, grailsApplication.config.annotopia.storage.uri.graph.provenance);
					graphMetadataService.getAnnotationGraphUpdateMetadata(apiKey, metaModel, annotationGraphUri);					
					updateDataset.addNamedModel(grailsApplication.config.annotopia.storage.uri.graph.provenance, metaModel);

					//println jenaUtilsService.getDatasetAsString(updateDataset);
					
					jenaVirtuosoStoreService.updateGraphMetadata(apiKey, metaModel, annotationGraphUri, grailsApplication.config.annotopia.storage.uri.graph.provenance)
					
					jenaVirtuosoStoreService.updateDataset(apiKey, updateDataset);
					
					if(incGph) return updateDataset;
					else return storedDataset;
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
	
	/**
	 * Mints a URI of type annotation
	 * @return The newly minted annotation URI
	 */
	public String mintAnnotationUri() {
		return mintUri("annotation");
	}

//	private String identifiableURIs(String apiKey, String content, Set<Resource> uris, String uriType) {
//		String toReturn = content;
//		uris.each { uri ->
//			def newUri = mintUri(uriType);			
//							
//			log.info("[" + apiKey + "] Minting multiple URIs (1) " + uriType + " " + uri.toString() + " -> " + newUri);
//			if(uri.isAnon()) {
//				println 'blank ' + uri.getId();
//				toReturn = content.replaceAll(Pattern.quote("\"@id\" : \"" + uri + "\""),
//					"\"@id\" : \"" + newUri + "\"" + ",\"http://purl.org/pav/previousVersion\" : \"blank\"");
//			} else
//			toReturn = content.replaceAll(Pattern.quote("\"@id\" : \"" + uri + "\""),
//				"\"@id\" : \"" + newUri + "\"" + ",\"http://purl.org/pav/previousVersion\" : \"" + uri.toString() + "\"");
//		}
//		return toReturn;
//	}
	
	/**
	 * Persist a URI (when only one resource is allowed)
	 * @param apiKey	The apiKey of who issued the request
	 * @param model		The model to act upon
	 * @param property	The property for matching statements property
	 * @param resource	The resource for matching statements object
	 * @param uriType	The type of the resource the URI is identifying
	 * @return The new resource (persisted)
	 */
	private Resource persistURI(String apiKey, Model model, Property property, Resource resource, String uriType) {
		
		log.info("[" + apiKey + "] Minting single URI (2) " + uriType + " " + resource.toString());
		
		def newResource = ResourceFactory.createResource(mintUri(uriType));

		List<Statement> updatedStatements = new ArrayList<Statement>();
		
		int counter = 0;
		def originalSubject;
		StmtIterator statements = model.listStatements(null, property, resource);
		for(def statement : statements) {
			counter++;
			originalSubject =  statement.getSubject()
		}
		if(counter>1)  log.warn("[" + apiKey + "] Attempt to mint multiple URI on a single URI request")
		
		if(originalSubject==null) {
			log.info("[" + apiKey + "] No " + uriType + " " + resource.toString() + " found.");
			return;
		} else {		
			// Update all the statements with the resource as subject
			updateStatementsWithResourceAsSubject(model, updatedStatements, newResource, originalSubject)	
			// Update all the statements with the resource as object
			updateStatementsWithResourceAsObject(model, updatedStatements, newResource, originalSubject)
			
			// Add the updated statements to the model
			updatedStatements.each { model.add(it); }
			
			// Updating the 'previous version' info
			updatePreviousVersionInfo(model, newResource, originalSubject);
				
			newResource;
		}
	}
	
	/**
	 * Persists multiple URIs.
	 * @param apiKey	The apiKey of who issued the request
	 * @param model		The model to act upon
	 * @param property	The property for matching statements property
	 * @param resource	The resource for matching statements object
	 * @param uriType	The type of the resource the URI is identifying
	 */
	private void persistURIs(String apiKey, Model model, Property property, Resource resource, String uriType) {
		
		log.info("[" + apiKey + "] Minting multiple URIs (3) " + uriType + " " + resource.toString());
		
		// Minting of URIs
		HashMap<Resource, Resource> originalToNewSubjects = new HashSet<Resource, Resource>();
		StmtIterator statements = model.listStatements(null, property, resource);
		statements.each {
			if(!originalToNewSubjects.containsKey(it.getSubject())) {
				log.info("[" + apiKey + "] Minting URI for content " + it.getSubject());
				def newResource = ResourceFactory.createResource(mintUri(uriType));
				originalToNewSubjects.put(it.getSubject(), newResource)
			}
		}
		
		if(originalToNewSubjects.size()==0) {
			log.info("[" + apiKey + "] No " + uriType + " " + resource.toString() + " found.");
			return;
		} else {
			List<Statement> updatedStatements = new ArrayList<Statement>();
			
			originalToNewSubjects.keySet().each { originalSubject ->
				// Update all the statements with the resource as subject
				updateStatementsWithResourceAsSubject(model, updatedStatements, originalToNewSubjects.get(originalSubject), originalSubject)
				// Update all the statements with the resource as object
				updateStatementsWithResourceAsObject(model, updatedStatements, originalToNewSubjects.get(originalSubject), originalSubject)
			}
			
			// Add the updated statements to the model
			updatedStatements.each { model.add(it); }
	
			// Updating the 'previous version' info
			originalToNewSubjects.keySet().each { originalSubject ->
				updatePreviousVersionInfo(model, originalToNewSubjects.get(originalSubject), originalSubject);
			}
		} 
	}
	
	/**
	 * Updates the "previous version" info. If the original resource did not have a URI, the
	 * previous version is marked as 'blank'.
	 * @param model				The model to act upon
	 * @param newSubject		The new URI identifying the resource
	 * @param originalSubject	The previous URI identifying the resource
	 */
	private void updatePreviousVersionInfo(Model model, Resource newSubject, Resource originalSubject) {
		model.removeAll(newSubject, ResourceFactory.createProperty(PAV.PAV_PREVIOUS_VERSION), null);
		if(!originalSubject.isAnon()) {
			model.add(model.createStatement(newSubject,
				ResourceFactory.createProperty(PAV.PAV_PREVIOUS_VERSION),
				ResourceFactory.createPlainLiteral(originalSubject.toString())
				));
		} else {
			model.add(model.createStatement(newSubject,
				ResourceFactory.createProperty(PAV.PAV_PREVIOUS_VERSION),
				ResourceFactory.createPlainLiteral("blank")
				));
		}
	}
	
	/**
	 * Updates of all the statements that have a resource (originalSubject) as object.
	 * @param model				The model to act upon
	 * @param updatedStatements The list of updated statements
	 * @param newSubject		The new URI identifying the object resource
	 * @param originalSubject	The previous URI identifying the resource
	 */
	private void updateStatementsWithResourceAsObject(Model model, List<Statement> updatedStatements, Resource newSubject, Resource originalSubject) {
		StmtIterator statements = model.listStatements(null, null, originalSubject);
		statements.each { statement ->
			updatedStatements.add(model.createStatement(statement.getSubject(), statement.getPredicate(), newSubject));
		}
		model.removeAll(null, null, originalSubject);
	}
	
	/**
	 * Updates of all the statements that have a resource (originalSubject) as subject.
	 * @param model				The model to act upon
	 * @param updatedStatements The list of updated statements
	 * @param newSubject		The new URI identifying the subject resource
	 * @param originalSubject	The previous URI identifying the resource
	 */
	private void updateStatementsWithResourceAsSubject(Model model, List<Statement> updatedStatements, Resource newSubject, Resource originalSubject) {	
		StmtIterator statements = model.listStatements(originalSubject, null, null);
		statements.each { statement ->
			updatedStatements.add(model.createStatement(newSubject, statement.getPredicate(), statement.getObject()));
		}		
		model.removeAll(originalSubject, null, null);
	}
}
