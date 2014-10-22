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
package org.annotopia.grails.services.storage.utils.jena

import java.text.SimpleDateFormat

import org.annotopia.grails.vocabularies.AnnotopiaVocabulary
import org.annotopia.grails.vocabularies.PAV
import org.annotopia.grails.vocabularies.RDF

import com.hp.hpl.jena.query.Dataset
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.rdf.model.ResourceFactory

/**
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class GraphMetadataService {
	
	def grailsApplication
	def configAccessService
	
	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSz")

	public Model getAnnotationSetGraphCreationMetadata(String apiKey, Dataset dataset, def graphUri) {
		//def metaGraphUri = getGraphUri();
		def graphRes = ResourceFactory.createResource(graphUri);
		Model metaModel
		
		if(dataset.getNamedModel(configAccessService.getAsString("annotopia.storage.uri.graph.provenance"))!=null) {
			 metaModel = dataset.getNamedModel(configAccessService.getAsString("annotopia.storage.uri.graph.provenance"));
		} else {
			 metaModel = ModelFactory.createDefaultModel();
			 dataset.addNamedModel(configAccessService.getAsString("annotopia.storage.uri.graph.provenance, metaModel"));
		}
		metaModel.add(graphRes, ResourceFactory.createProperty(RDF.RDF_TYPE), ResourceFactory.createResource(AnnotopiaVocabulary.ANNOTATION_SET_GRAPH));
		metaModel.add(graphRes, ResourceFactory.createProperty(PAV.PAV_CREATED_BY), ResourceFactory.createResource("annotopia:client:" + apiKey));
		metaModel.add(graphRes, ResourceFactory.createProperty(PAV.PAV_CREATED_AT), ResourceFactory.createPlainLiteral(dateFormat.format(new Date())));
		metaModel.add(graphRes, ResourceFactory.createProperty(PAV.PAV_CREATED_WITH), ResourceFactory.createResource("annotopia:test:001"));
		metaModel.add(graphRes, ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_ON), ResourceFactory.createPlainLiteral(dateFormat.format(new Date())));
		metaModel.add(graphRes, ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_BY), ResourceFactory.createResource("annotopia:client:" + apiKey));	
		metaModel.add(graphRes, ResourceFactory.createProperty(AnnotopiaVocabulary.AT_STATUS), ResourceFactory.createPlainLiteral("current"));
		dataset.addNamedModel(configAccessService.getAsString("annotopia.storage.uri.graph.provenance"), metaModel);
		metaModel
	}
	
	public Model getAnnotationGraphCreationMetadata(String apiKey, Dataset dataset, def graphUri) {
		//def metaGraphUri = getGraphUri();
		def graphRes = ResourceFactory.createResource(graphUri);
		Model metaModel 
		if(dataset.getNamedModel(configAccessService.getAsString("annotopia.storage.uri.graph.provenance"))!=null) {
			 metaModel = dataset.getNamedModel(configAccessService.getAsString("annotopia.storage.uri.graph.provenance"));
		} else {
		 	 metaModel = ModelFactory.createDefaultModel();
			 dataset.addNamedModel(configAccessService.getAsString("annotopia.storage.uri.graph.provenance"), metaModel);
		}
		metaModel.add(graphRes, ResourceFactory.createProperty(RDF.RDF_TYPE), ResourceFactory.createResource(AnnotopiaVocabulary.ANNOTATION_GRAPH));
		metaModel.add(graphRes, ResourceFactory.createProperty(PAV.PAV_CREATED_BY), ResourceFactory.createResource("annotopia:client:" + apiKey));
		metaModel.add(graphRes, ResourceFactory.createProperty(PAV.PAV_CREATED_AT), ResourceFactory.createPlainLiteral(dateFormat.format(new Date())));
		metaModel.add(graphRes, ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_ON), ResourceFactory.createPlainLiteral(dateFormat.format(new Date())));
		metaModel.add(graphRes, ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_BY), ResourceFactory.createResource("annotopia:client:" + apiKey));
		metaModel.add(graphRes, ResourceFactory.createProperty(PAV.PAV_CREATED_WITH), ResourceFactory.createResource("annotopia:test:001"));
		metaModel.add(graphRes, ResourceFactory.createProperty(AnnotopiaVocabulary.AT_STATUS), ResourceFactory.createPlainLiteral("current"));
		dataset.addNamedModel(configAccessService.getAsString("annotopia.storage.uri.graph.provenance"), metaModel);
		metaModel
	}
	
	public Model getBodyGraphCreationMetadata(String apiKey, Dataset dataset, def graphUri) {
		//xwdef metaGraphUri = getGraphUri();
		def graphRes = ResourceFactory.createResource(graphUri);
		Model metaModel 
		if(dataset.getNamedModel(configAccessService.getAsString("annotopia.storage.uri.graph.provenance"))!=null) {
			 metaModel = dataset.getNamedModel(configAccessService.getAsString("annotopia.storage.uri.graph.provenance"));
		} else {
		 	 metaModel = ModelFactory.createDefaultModel();
			 dataset.addNamedModel(configAccessService.getAsString("annotopia.storage.uri.graph.provenance"), metaModel);
		}
		metaModel.add(graphRes, ResourceFactory.createProperty(RDF.RDF_TYPE), ResourceFactory.createResource(AnnotopiaVocabulary.BODY_GRAPH));
		metaModel.add(graphRes, ResourceFactory.createProperty(PAV.PAV_CREATED_BY), ResourceFactory.createResource("annotopia:client:" + apiKey));
		metaModel.add(graphRes, ResourceFactory.createProperty(PAV.PAV_CREATED_AT), ResourceFactory.createPlainLiteral(dateFormat.format(new Date())));
		metaModel.add(graphRes, ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_ON), ResourceFactory.createPlainLiteral(dateFormat.format(new Date())));
		metaModel.add(graphRes, ResourceFactory.createProperty(PAV.PAV_CREATED_WITH), ResourceFactory.createResource("annotopia:test:001"));
		metaModel.add(graphRes, ResourceFactory.createProperty(AnnotopiaVocabulary.AT_STATUS), ResourceFactory.createPlainLiteral("current"));
		metaModel
	}
	
	public Model getAnnotationGraphUpdateMetadata(String apiKey, Model metaModel, def graphUri) {
		def graphRes = ResourceFactory.createResource(graphUri);
		metaModel.removeAll(graphRes, ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_ON), null);
		metaModel.add(graphRes, ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_ON), ResourceFactory.createPlainLiteral(dateFormat.format(new Date())));
		metaModel.removeAll(graphRes, ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_BY), null);
		metaModel.add(graphRes, ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_BY), ResourceFactory.createResource("annotopia:client:" + apiKey));
		metaModel.removeAll(graphRes, ResourceFactory.createProperty(AnnotopiaVocabulary.AT_CURRENT), null);
		metaModel.add(graphRes, ResourceFactory.createProperty(AnnotopiaVocabulary.AT_STATUS), ResourceFactory.createPlainLiteral("current"));
		metaModel
	}
	
	private String getGraphUri() {
		return configAccessService.getAsString("grails.server.protocol") + '://' + 
			configAccessService.getAsString("grails.server.host") + ':' +
			configAccessService.getAsString("grails.server.port") + '/s/graph/' +
			org.annotopia.grails.services.storage.utils.UUID.uuid();
	}
}
