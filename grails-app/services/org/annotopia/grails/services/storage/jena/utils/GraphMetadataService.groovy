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
package org.annotopia.grails.services.storage.jena.utils

import java.text.SimpleDateFormat

import com.hp.hpl.jena.query.Dataset
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.rdf.model.ResourceFactory

/**
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class GraphMetadataService {
	
	def grailsApplication
	
	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSz")

	public Model getAnnotationSetGraphCreationMetadata(String apiKey, Dataset dataset, def graphUri) {
		//def metaGraphUri = getGraphUri();
		def graphRes = ResourceFactory.createResource(graphUri);
		Model metaModel
		if(dataset.getNamedModel("annotopia:graphs:provenance")!=null) {
			 metaModel = dataset.getNamedModel("annotopia:graphs:provenance");
		} else {
			  metaModel = ModelFactory.createDefaultModel();
			 dataset.addNamedModel("annotopia:graphs:provenance", metaModel);
		}
		metaModel.add(graphRes, ResourceFactory.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), ResourceFactory.createResource("http://purl.org/annotopia#AnnotationSetGraph"));
		metaModel.add(graphRes, ResourceFactory.createProperty("http://purl.org/pav/createdBy"), ResourceFactory.createResource("annotopia:client:" + apiKey));
		metaModel.add(graphRes, ResourceFactory.createProperty("http://purl.org/pav/createdAt"), ResourceFactory.createPlainLiteral(dateFormat.format(new Date())));
		metaModel.add(graphRes, ResourceFactory.createProperty("http://purl.org/pav/lastUpdatedOn"), ResourceFactory.createPlainLiteral(dateFormat.format(new Date())));
		metaModel.add(graphRes, ResourceFactory.createProperty("http://purl.org/pav/lastUpdatedBy"), ResourceFactory.createResource("annotopia:client:" + apiKey));
		metaModel.add(graphRes, ResourceFactory.createProperty("http://purl.org/pav/createdWith"), ResourceFactory.createResource("annotopia:test:001"));
		metaModel.add(graphRes, ResourceFactory.createProperty("http://purl.org/annotopia#status"), ResourceFactory.createPlainLiteral("current"));
		dataset.addNamedModel("annotopia:graphs:provenance", metaModel);
		metaModel
	}
	
	public Model getAnnotationGraphCreationMetadata(String apiKey, Dataset dataset, def graphUri) {
		//def metaGraphUri = getGraphUri();
		def graphRes = ResourceFactory.createResource(graphUri);
		Model metaModel 
		if(dataset.getNamedModel("annotopia:graphs:provenance")!=null) {
			 metaModel = dataset.getNamedModel("annotopia:graphs:provenance");
		} else {
		 	 metaModel = ModelFactory.createDefaultModel();
			 dataset.addNamedModel("annotopia:graphs:provenance", metaModel);
		}
		metaModel.add(graphRes, ResourceFactory.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), ResourceFactory.createResource("http://purl.org/annotopia#AnnotationGraph"));
		metaModel.add(graphRes, ResourceFactory.createProperty("http://purl.org/pav/createdBy"), ResourceFactory.createResource("annotopia:client:" + apiKey));
		metaModel.add(graphRes, ResourceFactory.createProperty("http://purl.org/pav/createdAt"), ResourceFactory.createPlainLiteral(dateFormat.format(new Date())));
		metaModel.add(graphRes, ResourceFactory.createProperty("http://purl.org/pav/lastUpdatedOn"), ResourceFactory.createPlainLiteral(dateFormat.format(new Date())));
		metaModel.add(graphRes, ResourceFactory.createProperty("http://purl.org/pav/lastUpdatedBy"), ResourceFactory.createResource("annotopia:client:" + apiKey));
		metaModel.add(graphRes, ResourceFactory.createProperty("http://purl.org/pav/createdWith"), ResourceFactory.createResource("annotopia:test:001"));
		metaModel.add(graphRes, ResourceFactory.createProperty("http://purl.org/annotopia#status"), ResourceFactory.createPlainLiteral("current"));
		dataset.addNamedModel("annotopia:graphs:provenance", metaModel);
		metaModel
	}
	
	public Model getBodyGraphCreationMetadata(String apiKey, Dataset dataset, def graphUri) {
		//xwdef metaGraphUri = getGraphUri();
		def graphRes = ResourceFactory.createResource(graphUri);
		Model metaModel 
		if(dataset.getNamedModel("annotopia:graphs:provenance")!=null) {
			 metaModel = dataset.getNamedModel("annotopia:graphs:provenance");
		} else {
		 	 metaModel = ModelFactory.createDefaultModel();
			 dataset.addNamedModel("annotopia:graphs:provenance", metaModel);
		}
		metaModel.add(graphRes, ResourceFactory.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), ResourceFactory.createResource("http://purl.org/annotopia#BodyGraph"));
		metaModel.add(graphRes, ResourceFactory.createProperty("http://purl.org/pav/createdBy"), ResourceFactory.createResource("annotopia:client:" + apiKey));
		metaModel.add(graphRes, ResourceFactory.createProperty("http://purl.org/pav/createdAt"), ResourceFactory.createPlainLiteral(dateFormat.format(new Date())));
		metaModel.add(graphRes, ResourceFactory.createProperty("http://purl.org/pav/lastUpdatedOn"), ResourceFactory.createPlainLiteral(dateFormat.format(new Date())));
		metaModel.add(graphRes, ResourceFactory.createProperty("http://purl.org/pav/createdWith"), ResourceFactory.createResource("annotopia:test:001"));
		metaModel.add(graphRes, ResourceFactory.createProperty("http://purl.org/annotopia#status"), ResourceFactory.createPlainLiteral("current"));
		metaModel
	}
	
	public Model getAnnotationGraphUpdateMetadata(String apiKey, Model metaModel, def graphUri) {
		def graphRes = ResourceFactory.createResource(graphUri);
		metaModel.removeAll(graphRes, ResourceFactory.createProperty("http://purl.org/pav/lastUpdatedOn"), null);
		metaModel.add(graphRes, ResourceFactory.createProperty("http://purl.org/pav/lastUpdatedOn"), ResourceFactory.createPlainLiteral(dateFormat.format(new Date())));
		metaModel.removeAll(graphRes, ResourceFactory.createProperty("http://purl.org/pav/lastUpdatedBy"), null);
		metaModel.add(graphRes, ResourceFactory.createProperty("http://purl.org/pav/lastUpdatedBy"), ResourceFactory.createResource("annotopia:client:" + apiKey));
		metaModel.removeAll(graphRes, ResourceFactory.createProperty("http://purl.org/annotopia#current"), null);
		metaModel.add(graphRes, ResourceFactory.createProperty("http://purl.org/annotopia#status"), ResourceFactory.createPlainLiteral("current"));
		metaModel
	}
	
	private String getGraphUri() {
		return 'http://' + grailsApplication.config.grails.server.host + ':' +
			grailsApplication.config.grails.server.port.http + '/s/graph/' +
			org.annotopia.grails.services.storage.utils.UUID.uuid();
	}
}
