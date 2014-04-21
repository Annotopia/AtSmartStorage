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
package org.annotopia.grails.services.storage.openannotation.jena

import grails.converters.JSON
import groovy.lang.Closure;

import java.text.SimpleDateFormat
import java.util.Set;

import org.annotopia.grails.vocabularies.AnnotopiaVocabulary
import org.annotopia.grails.vocabularies.OA
import org.annotopia.grails.vocabularies.PAV
import org.annotopia.grails.vocabularies.RDF
import org.annotopia.groovy.service.store.StoreServiceException
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.RDFLanguages

import com.hp.hpl.jena.query.Dataset
import com.hp.hpl.jena.query.DatasetFactory
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.Resource
import com.hp.hpl.jena.rdf.model.ResourceFactory
import com.hp.hpl.jena.rdf.model.Statement
import com.hp.hpl.jena.rdf.model.StmtIterator

/**
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class OpenAnnotationSetStorageService {

	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
	
	def jenaUtilsService
	def grailsApplication
	def graphMetadataService
	def jenaVirtuosoStoreService
	def openAnnotationUtilsService
	def openAnnotationVirtuosoService
	def openAnnotationStorageService
	def openAnnotationSetsUtilsService
	
	public listAnnotationSets(apiKey, max, offset, tgtUrl, tgtFgt, tgtExt, tgtIds, incGph) {
		log.info '[' + apiKey + '] List annotation sets' +
			' max:' + max +
			' offset:' + offset +
			' incGph:' + incGph +
			' tgtUrl:' + tgtUrl +
			' tgtFgt:' + tgtFgt;
		retrieveAnnotationSets(apiKey, max, offset, tgtUrl, tgtFgt, incGph);
	}
	
	public Set<Dataset> retrieveAnnotationSets(apiKey, max, offset, tgtUrl, tgtFgt, incGph) {
		log.info '[' + apiKey + '] Retrieving annotation sets';
	
		Set<Dataset> datasets = new HashSet<Dataset>();
		Set<String> graphNames = openAnnotationVirtuosoService.retrieveAnnotationSetsGraphsNames(apiKey, max, offset, tgtUrl, tgtFgt);
		if(graphNames!=null) {
			graphNames.each { graphName ->
				Dataset ds = jenaVirtuosoStoreService.retrieveGraph(apiKey, graphName);
				if(incGph=='true') {
					Model m = jenaVirtuosoStoreService.retrieveGraphMetadata(apiKey, graphName, grailsApplication.config.annotopia.storage.uri.graph.provenance);
					if(m!=null) ds.setDefaultModel(m);
				}
				if(ds!=null) datasets.add(ds);
			}
		}
		return datasets;
	}
	
	public Dataset saveAnnotationSet(String apiKey, Long startTime, String set) {
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
			Resource annotationSetUri = openAnnotationStorageService.identifiableURI(apiKey, inMemoryDataset.getDefaultModel(),
				ResourceFactory.createProperty(RDF.RDF_TYPE),
				ResourceFactory.createResource(AnnotopiaVocabulary.ANNOTATION_SET), "annotationset");
			
			// Specific Resource identifier
			openAnnotationStorageService.identifiableURIs(apiKey, inMemoryDataset.getDefaultModel(),
				ResourceFactory.createProperty(RDF.RDF_TYPE),
				ResourceFactory.createResource(OA.SPECIFIC_RESOURCE), "resource");

			// Embedded content (as RDF) identifier
			openAnnotationStorageService.identifiableURIs(apiKey, inMemoryDataset.getDefaultModel(),
				ResourceFactory.createProperty(RDF.RDF_TYPE),
				ResourceFactory.createResource(OA.CONTEXT_AS_TEXT), "content");
			
			HashMap<Resource, String> oldNewAnnotationUriMapping = new HashMap<Resource, String>();
			Iterator<Resource> annotationUrisIterator = annotationUris.iterator();
			while(annotationUrisIterator.hasNext()) {
				// Bodies graphs identifiers
				Resource annotation = annotationUrisIterator.next();
				String newAnnotationUri = openAnnotationStorageService.getAnnotationUri();
				oldNewAnnotationUriMapping.put(annotation, newAnnotationUri);
			}
			
			// Update Bodies graphs URIs
			Model annotationModel = inMemoryDataset.getDefaultModel();
			oldNewAnnotationUriMapping.keySet().each { oldAnnotation ->
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
			
			// TODO make sure there is only one set
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
			def graphUri = openAnnotationStorageService.getGraphUri();
			Dataset creationDataset = DatasetFactory.createMem();
			creationDataset.addNamedModel(graphUri, inMemoryDataset.getDefaultModel());
			// Creation of the metadata for the Graph wrapper
			def graphResource = ResourceFactory.createResource(graphUri);
			Model metaModel = graphMetadataService.getAnnotationSetGraphCreationMetadata(apiKey, creationDataset, graphUri);
			oldNewAnnotationUriMapping.values().each { annotationUri ->
				metaModel.add(graphResource, ResourceFactory.createProperty(AnnotopiaVocabulary.ANNOTATION),
					 ResourceFactory.createPlainLiteral(annotationUri));
			}
			if(oldNewAnnotationUriMapping.values().size()>0) {
				metaModel.add(graphResource, ResourceFactory.createProperty(AnnotopiaVocabulary.ANNOTATION_COUNT),
					ResourceFactory.createPlainLiteral(""+oldNewAnnotationUriMapping.values().size()));
			}
				
			// TODO remove before release
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			RDFDataMgr.write(outputStream, creationDataset, RDFLanguages.JSONLD);
			println outputStream.toString();
			
			jenaVirtuosoStoreService.storeDataset(apiKey, creationDataset);
			return creationDataset;
		}
	}
	
	public Dataset updateAnnotationSet(String apiKey, Long startTime, String set) {
				
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
		Set<Resource> newAnnotationUris = new HashSet<Resource>();
		
		// Registry of all named graphs in the transaction
		Set<Resource> newGraphsUris = jenaUtilsService.detectNamedGraphs(apiKey, inMemoryDataset);

		// Detection of default graph
		int annotationsInDefaultGraphsCounter = openAnnotationUtilsService.detectAnnotationsInDefaultGraph(apiKey, inMemoryDataset, newAnnotationUris, null)
		boolean defaultGraphDetected = (annotationsInDefaultGraphsCounter>0);
		
		// Query for graphs containing annotation
		// See: https://www.mail-archive.com/wikidata-l@lists.wikimedia.org/msg00370.html
		Set<Resource> annotationsGraphsUris = new HashSet<Resource>();
		int detectedAnnotationGraphsCounter = openAnnotationUtilsService.detectAnnotationsInNamedGraph(
			apiKey, inMemoryDataset, newGraphsUris, annotationsGraphsUris, newAnnotationUris, null)
		
		// Enforcing the limit to one annotation per transaction
		if(defaultGraphDetected && detectedAnnotationGraphsCounter>0) {
			log.info("[" + apiKey + "] Mixed Annotation content detected... request rejected.");
			def json = JSON.parse('{"status":"nocontent" ,"message":"The request carries a mix of Annotations and Annotation Graphs"' +
				',"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
			throw new StoreServiceException(200, json, "text/json", "UTF-8");
		} else if(detectedAnnotationGraphsCounter>1 || newGraphsUris.size()>2) {
			// Annotation Set not found
			log.info("[" + apiKey + "] Multiple Annotation graphs detected... request rejected.");
			def json = JSON.parse('{"status":"rejected" ,"message":"The request carries multiple Annotations or Annotation Graphs"' +
				',"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
			throw new StoreServiceException(200, json, "text/json", "UTF-8");
		}
		
		if(defaultGraphDetected) {
			log.trace("[" + apiKey + "] Default graph detected.");
			
			Set<Resource> annotationSetUris = new HashSet<Resource>();
			int annotationsSetsUrisInDefaultGraphsCounter = 
				openAnnotationSetsUtilsService.detectAnnotationSetUriInDefaultGraph(apiKey, inMemoryDataset, annotationSetUris, null);
			
			if(annotationsSetsUrisInDefaultGraphsCounter==0) {
				log.info("[" + apiKey + "] No Annotation set detected... request rejected.");
				def json = JSON.parse('{"status":"nocontent" ,"message":"The request does not carry Annotation Sets."' +
					',"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
				throw new StoreServiceException(200, json, "text/json", "UTF-8");
			} else if(annotationsSetsUrisInDefaultGraphsCounter==1) {
				String annotationSetUri = annotationSetUris.iterator().next();
				
				// Retrieve the graph name from the storage
				String QUERY = "PREFIX at: <http://purl.org/annotopia#> SELECT DISTINCT ?s ?g WHERE { GRAPH ?g { <" + annotationSetUri + "> a at:AnnotationSet . }}"
				Set<String> annotationSetsGraphNames = jenaVirtuosoStoreService.retrieveGraphsNames(apiKey, QUERY);
				
				if(annotationSetsGraphNames.size()==1) {
					String annotationSetGraphName = annotationSetsGraphNames.iterator().next();
					
					Dataset existingSet = jenaVirtuosoStoreService.retrieveGraph(apiKey, annotationSetGraphName);
					if(existingSet!=null) {
						log.info("[" + apiKey + "] Old Annotaiton Set version detected.");
						
						Set<Resource> existingAnnotationUris = new HashSet<Resource>();
						int existingAnnotationCounter = openAnnotationUtilsService.detectAnnotationsInNamedGraph(apiKey, existingSet, new HashSet<Resource>(), new HashSet<Resource>(), existingAnnotationUris, null)
						boolean existingAnnotationDetected = (existingAnnotationCounter>0);
						
						Model annotationModel = inMemoryDataset.getDefaultModel();
						HashMap<Resource, String> oldNewAnnotationUriMapping = new HashMap<Resource, String>();
						for(Resource newAnnotationResource: newAnnotationUris) {
							boolean found = false;
							for(Resource existingAnnotationUri: existingAnnotationUris) {
								if(existingAnnotationUri.getURI().equals(newAnnotationResource.getURI())) {
									found = true;
									log.info("[" + apiKey + "] Checking if existing annotation changed.");
									
									boolean hasChanged = openAnnotationSetsUtilsService.isAnnotationChanged(apiKey, inMemoryDataset, newAnnotationResource.getURI());
									if(hasChanged) {
										annotationModel.removeAll(newAnnotationResource, ResourceFactory.createProperty(PAV.PAV_PREVIOUS_VERSION), null);
										annotationModel.add(
											newAnnotationResource,
											ResourceFactory.createProperty(PAV.PAV_PREVIOUS_VERSION),
											ResourceFactory.createPlainLiteral(newAnnotationResource.toString()));
										
										annotationModel.removeAll(newAnnotationResource, ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_ON), null);
										annotationModel.add(
											newAnnotationResource,
											ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_ON),
											ResourceFactory.createPlainLiteral(dateFormat.format(new Date())));
									}							
									break;
								}
							}
							if(!found) {
								log.info("[" + apiKey + "] Found new annotation " + newAnnotationResource.getURI());
								oldNewAnnotationUriMapping.put(newAnnotationResource, openAnnotationStorageService.getAnnotationUri());
								
								StmtIterator statements = annotationModel.listStatements(newAnnotationResource, null, null);
								List<Statement> statementsToRemove = new ArrayList<Statement>();
								statements.each { statementsToRemove.add(it)}
								statementsToRemove.each { statement ->
									annotationModel.remove(statement);
									annotationModel.add(ResourceFactory.createResource(oldNewAnnotationUriMapping.get(newAnnotationResource)),
										statement.getPredicate(), statement.getObject());
								}
								
								annotationModel.removeAll(ResourceFactory.createResource(oldNewAnnotationUriMapping.get(newAnnotationResource)), ResourceFactory.createProperty(PAV.PAV_PREVIOUS_VERSION), null);
								annotationModel.add(
									ResourceFactory.createResource(oldNewAnnotationUriMapping.get(newAnnotationResource)),
									ResourceFactory.createProperty(PAV.PAV_PREVIOUS_VERSION),
									ResourceFactory.createPlainLiteral(newAnnotationResource.toString()));
								
								annotationModel.removeAll(ResourceFactory.createResource(oldNewAnnotationUriMapping.get(newAnnotationResource)), ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_ON), null);
								annotationModel.add(
									ResourceFactory.createResource(oldNewAnnotationUriMapping.get(newAnnotationResource)),
									ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_ON),
									ResourceFactory.createPlainLiteral(dateFormat.format(new Date())));
							} 
						}
						
						// TODO make sure there is only one set
						List<Statement> statementsToRemove = new ArrayList<Statement>();
						StmtIterator stats = annotationModel.listStatements(ResourceFactory.createResource(annotationSetUri),
							ResourceFactory.createProperty(AnnotopiaVocabulary.ANNOTATIONS),
							null);
						while(stats.hasNext()) {
							Statement s = stats.nextStatement();
							if(oldNewAnnotationUriMapping.get(s.getObject())!=null)  statementsToRemove.add(s);
						}
						
						statementsToRemove.each { s ->
							annotationModel.remove(s);
							annotationModel.add(s.getSubject(), ResourceFactory.createProperty(AnnotopiaVocabulary.ANNOTATIONS),
								ResourceFactory.createResource(oldNewAnnotationUriMapping.get(s.getObject())));
						}
						
						
						// Last saved on
						annotationModel.removeAll(ResourceFactory.createResource(annotationSetUri), ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_ON), null);
						annotationModel.add(ResourceFactory.createResource(annotationSetUri), ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_ON),
							ResourceFactory.createPlainLiteral(dateFormat.format(new Date())));
						
						// Version
						annotationModel.removeAll(ResourceFactory.createResource(annotationSetUri), ResourceFactory.createProperty(PAV.PAV_VERSION), null);
						annotationModel.add(ResourceFactory.createResource(annotationSetUri), ResourceFactory.createProperty(PAV.PAV_VERSION),
							ResourceFactory.createPlainLiteral("1"));

						
//						// Update Bodies graphs URIs
//						Model annotationModel = inMemoryDataset.getDefaultModel();
//						oldNewAnnotationUriMapping.keySet().each { oldAnnotation ->
//							StmtIterator statements = annotationModel.listStatements(oldAnnotation, null, null);
//							List<Statement> statementsToRemove = new ArrayList<Statement>();
//							statements.each { statementsToRemove.add(it)}
//							statementsToRemove.each { statement ->
//								annotationModel.remove(statement);
//								annotationModel.add(ResourceFactory.createResource(oldNewAnnotationUriMapping.get(oldAnnotation)),
//									statement.getPredicate(), statement.getObject());
//							}
//							
//							annotationModel.removeAll(ResourceFactory.createResource(oldNewAnnotationUriMapping.get(oldAnnotation)), ResourceFactory.createProperty(PAV.PAV_PREVIOUS_VERSION), null);
//							annotationModel.add(
//								ResourceFactory.createResource(oldNewAnnotationUriMapping.get(oldAnnotation)),
//								ResourceFactory.createProperty(PAV.PAV_PREVIOUS_VERSION),
//								ResourceFactory.createPlainLiteral(oldAnnotation.toString()));
//							
//							annotationModel.removeAll(ResourceFactory.createResource(oldNewAnnotationUriMapping.get(oldAnnotation)), ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_ON), null);
//							annotationModel.add(
//								ResourceFactory.createResource(oldNewAnnotationUriMapping.get(oldAnnotation)),
//								ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_ON),
//								ResourceFactory.createPlainLiteral(dateFormat.format(new Date())));
//						}
						
					
//						println "----------1 " + annotationSetGraphName
//						ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
//						RDFDataMgr.write(outputStream, existingSet, RDFLanguages.JSONLD);
//						println outputStream.toString();
						
	//					Model newDefaultModel = existingSet.getNamedModel(annotationSetGraphName);
	//					
	//					println "----------2 " + annotationSetGraphName
	//					ByteArrayOutputStream outputStream2 = new ByteArrayOutputStream();
	//					RDFDataMgr.write(outputStream2, newDefaultModel, RDFLanguages.JSONLD);
	//					println outputStream2.toString();
						
						Dataset updateDataset = DatasetFactory.createMem();
						
						
						
						updateDataset.addNamedModel(annotationSetGraphName, annotationModel);
						//updateDataset.addNamedModel(annotationSetGraphName, inMemoryDataset.getDefaultModel());
						
//						println "----------3 " + annotationSetGraphName
//						ByteArrayOutputStream outputStream3 = new ByteArrayOutputStream();
//						RDFDataMgr.write(outputStream3, updateDataset, RDFLanguages.JSONLD);
//						println outputStream3.toString();
						
						//jenaVirtuosoStoreService.updateDataset(apiKey, updateDataset);
						
						return updateDataset;
					} else {
						log.info("[" + apiKey + "] Old version of Annotation sets graphs not found... request rejected (should be a POST?).");
						def json = JSON.parse('{"status":"rejected" ,"message":"The requested update cannot be performed.  Old version of Annotation sets graphs not found... request rejected (should be a POST?)."' +
							',"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
						throw new StoreServiceException(200, json, "text/json", "UTF-8");
					}
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
			
			
//			String QUERY = "PREFIX at: <http://purl.org/annotopia#> SELECT ?s ?g WHERE { GRAPH ?g { ?s a at:AnnotationSet . }}";
//			Set<String> graphNames = jenaVirtuosoStoreService.retrieveGraphsNames(apiKey, QUERY)
//				
//			if(graphNames.size()==1) {
//				Iterator itr = graphNames.iterator();
//				String annotationSetGraphName = itr.next();
//				
//				Dataset existingSet = jenaVirtuosoStoreService.retrieveGraph(apiKey, annotationSetGraphName);
//				
//				println "----------1 " + annotationSetGraphName
//				ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
//				RDFDataMgr.write(outputStream, existingSet, RDFLanguages.JSONLD);
//				println outputStream.toString();
//				
//				//???
//				//Model newDefaultModel = dataset.getDefaultModel();
//				//jenaVirtuosoStoreService.updateDataset(apiKey, workingDataset);
//				
//				//Dataset workingDataset = DatasetFactory.createMem();
//				//RDFDataMgr.read(workingDataset, new ByteArrayInputStream(set.toString().getBytes("UTF-8")), RDFLanguages.JSONLD);
//				
//				Model newDefaultModel = existingSet.getDefaultModel();
//				
//				println "----------2 " + annotationSetGraphName
//				ByteArrayOutputStream outputStream2 = new ByteArrayOutputStream();
//				RDFDataMgr.write(outputStream2, newDefaultModel, RDFLanguages.JSONLD);
//				println outputStream2.toString();
//				
//				Dataset updateDataset = DatasetFactory.createMem();
//				updateDataset.addNamedModel(annotationSetGraphName, newDefaultModel);
//				
//				println "----------3 " + annotationSetGraphName
//				ByteArrayOutputStream outputStream3 = new ByteArrayOutputStream();
//				RDFDataMgr.write(outputStream3, updateDataset, RDFLanguages.JSONLD);
//				println outputStream3.toString();
//				
//				jenaVirtuosoStoreService.updateDataset(apiKey, updateDataset);
//				
//				return updateDataset;
//			} else {
//				
//			}
//			
//		}
//		println "end";
	}
}
