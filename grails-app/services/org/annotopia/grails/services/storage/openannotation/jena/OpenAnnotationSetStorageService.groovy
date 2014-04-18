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

import java.text.SimpleDateFormat

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
			log.error(ex.getMessage());
//			def message = "Annotation cannot be read";
//			render(status: 500, text: returnMessage(apiKey, "invalidcontent", message, startTime), contentType: "text/json", encoding: "UTF-8");
			return;
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
			log.error(ex.getMessage());
//			def message = "Annotation cannot be read";
//			render(status: 500, text: returnMessage(apiKey, "invalidcontent", message, startTime), contentType: "text/json", encoding: "UTF-8");
			return;
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
			
			Set<Resource> annotationSetUris = new HashSet<Resource>();
			Set<Resource> annotationSetGraphsUris = new HashSet<Resource>();
			
			String QUERY = "PREFIX at: <http://purl.org/annotopia#> SELECT ?s ?g WHERE { GRAPH ?g { ?s a at:AnnotationSet . }}";
			Set<String> graphNames = jenaVirtuosoStoreService.retrieveGraphsNames(apiKey, QUERY)
				
			if(graphNames.size()==1) {
				Iterator itr = graphNames.iterator();
				String annotationSetGraphName = itr.next();
				
				Dataset existingSet = jenaVirtuosoStoreService.retrieveGraph(apiKey, annotationSetGraphName);
				
				//???
				//Model newDefaultModel = dataset.getDefaultModel();
				//jenaVirtuosoStoreService.updateDataset(apiKey, workingDataset);
				
				Dataset workingDataset = DatasetFactory.createMem();
				RDFDataMgr.read(workingDataset, new ByteArrayInputStream(set.toString().getBytes("UTF-8")), RDFLanguages.JSONLD);
				
				jenaVirtuosoStoreService.updateDataset(apiKey, workingDataset);
				
				return workingDataset;
			} else {
				
			}
			
		}
		println "end";
	}
}
