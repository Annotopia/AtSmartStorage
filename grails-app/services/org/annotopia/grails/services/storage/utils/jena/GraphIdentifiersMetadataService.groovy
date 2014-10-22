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

import org.annotopia.grails.vocabularies.Bibliographic
import org.annotopia.grails.vocabularies.RDF

import com.hp.hpl.jena.query.Dataset
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.rdf.model.Resource
import com.hp.hpl.jena.rdf.model.ResourceFactory
import com.hp.hpl.jena.rdf.model.Statement
import com.hp.hpl.jena.rdf.model.StmtIterator


/**
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class GraphIdentifiersMetadataService {

	def grailsApplication
	def configAccessService
	
	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSz")
	
	/**
	 * Creates, persists and returns a Model with all the bibliographic metadata.
	 * @param apiKey		The API key of the client issuing the request
	 * @param dataset		The Dataset to which to add the bibliographic metadata model
	 * @param graphUri		The URI to be used for the Expression entity
	 * @param identifiers	The list of bibliographic identifiers.
	 * @return The Model with all the Expression bibliographic metadata.
	 */
	public Model getIdentifiersGraphMetadata(String apiKey, Dataset dataset, def graphUri, Map<String, String> identifiers) {
		log.trace("Creating bilbliographic identifiers " + identifiers);

		Resource expression = ResourceFactory.createResource(graphUri);
		Model metaModel;
		if(dataset.getNamedModel(configAccessService.getAsString("annotopia.storage.uri.graph.identifiers"))!=null) {
			 metaModel = dataset.getNamedModel(configAccessService.getAsString("annotopia.storage.uri.graph.identifiers"));
		} else {
			 metaModel = ModelFactory.createDefaultModel();
			 dataset.addNamedModel(configAccessService.getAsString("annotopia.storage.uri.graph.identifiers"), metaModel);
		}
		
		if(identifiers.get(Bibliographic.LABEL_URL)!=null) {
			// Definition of the expression
			metaModel.add(expression, 
				ResourceFactory.createProperty(RDF.RDF_TYPE), 
				ResourceFactory.createResource(Bibliographic.EXPRESSION));
			// Definition of the manifestation
			addManifestation(expression, metaModel, identifiers);
			// Update of the identifiers list
			updateModelWithIdentifiers(expression, metaModel, identifiers);
			dataset.addNamedModel(configAccessService.getAsString("annotopia.storage.uri.graph.identifiers"), metaModel);
		}
		metaModel
	}
	
	/**
	 * Adds the manifestation to the model.
	 * @param expression	The expression of the content
	 * @param model			The model with the bibliographic metadata
	 * @param identifiers	The collection of new bibliographic identifiers
	 */
	private void addManifestation(Resource expression, Model model, Map<String, String> identifiers) {
		log.trace("Adding alternative manifestation: " + identifiers.get(Bibliographic.LABEL_URL +
			" for expression " + expression.toString()));
		
		model.add(ResourceFactory.createResource(identifiers.get(Bibliographic.LABEL_URL)),
			ResourceFactory.createProperty(Bibliographic.EMBODIMENT_OF),
			expression);
		
		model.add(ResourceFactory.createResource(identifiers.get(Bibliographic.LABEL_URL)),
			ResourceFactory.createProperty(RDF.RDF_TYPE),
			ResourceFactory.createResource(Bibliographic.WEB_PAGE));
	}
	
	/**
	 * Updates the expression with the newly available identifiers.
	 * @param expression	The expression resource
	 * @param model			The model to update
	 * @param identifiers	The list of new identifiers
	 */
	private void updateModelWithIdentifiers(Resource expression, Model model, Map<String, String> identifiers) {
		log.trace("Updating expression.");
		if(identifiers.get(Bibliographic.LABEL_DOI)!=null) {
			model.add(expression, ResourceFactory.createProperty(Bibliographic.DOI),
				ResourceFactory.createPlainLiteral(identifiers.get(Bibliographic.LABEL_DOI)));
		}
		
		if(identifiers.get(Bibliographic.LABEL_PMID)!=null) {
			model.add(expression, ResourceFactory.createProperty(Bibliographic.PMID),
				ResourceFactory.createPlainLiteral(identifiers.get(Bibliographic.LABEL_PMID)));
		}
		
		if(identifiers.get(Bibliographic.LABEL_PMCID)!=null) {
			model.add(expression, ResourceFactory.createProperty(Bibliographic.PMCID),
				ResourceFactory.createPlainLiteral(identifiers.get(Bibliographic.LABEL_PMCID)));
		}
		
		if(identifiers.get(Bibliographic.LABEL_PII)!=null) {
			model.add(expression, ResourceFactory.createProperty(Bibliographic.PII),
				ResourceFactory.createPlainLiteral(identifiers.get(Bibliographic.LABEL_PII)));
		}
		
		if(identifiers.get(Bibliographic.LABEL_TITLE)!=null) {
			model.add(expression, ResourceFactory.createProperty(Bibliographic.TITLE),
				ResourceFactory.createPlainLiteral(identifiers.get(Bibliographic.LABEL_TITLE)));
		}
	}
	
	/**
	 * Updates and returns a copy of the model with the Expression bibliographic metadata
	 * @param apiKey		The API key of the client issuing the request
	 * @param metaModel		The Model with the already persisted bibliographic metadata.
	 * @param identifiers	The list of bibliographic identifiers in the update.
	 * @return The updated Model with all the Expression bibliographic metadata
	 */
	public Model updateIdentifiersGraphMetadata(String apiKey, Dataset dataset, Model metaModel, Map<String, String> identifiers) {
		log.trace("Updating bilbliographic identifiers " + identifiers);
		
		Resource expression;
		Set<String> resources = new HashSet<String>();
		StmtIterator resourcesIterator = metaModel.listStatements(null, ResourceFactory.createProperty(Bibliographic.EMBODIMENT_OF), null);
		while(resourcesIterator.hasNext()) {
			def statement = resourcesIterator.next();
			if(expression==null) expression = statement.getObject();
			resources.add(statement.getSubject().toString());
		}

		if(!resources.contains(identifiers.get(Bibliographic.LABEL_URL))) {
			addManifestation(expression, metaModel, identifiers);
		}
		
		StmtIterator identifiersData = metaModel.listStatements(expression, null, null);
		while(identifiersData.hasNext()) {
			def statement = identifiersData.next();
			def label = statement.getPredicate().toString();
			
			if(label==Bibliographic.DOI) {
				if(identifiers.get(Bibliographic.LABEL_DOI)!=null) {
					if(identifiers.get(Bibliographic.LABEL_DOI)!=statement.getObject().toString()) {
						log.warn("DOI inconsistency detected [old:" + statement.getObject().toString() + 
							", new:" + identifiers.get(Bibliographic.LABEL_DOI) + "]");
					}
					identifiers.remove(Bibliographic.LABEL_DOI);
				}
			}		
			if(label==Bibliographic.PMID) {
				if(identifiers.get(Bibliographic.LABEL_PMID)!=null) {
					if(identifiers.get(Bibliographic.LABEL_PMID)!=statement.getObject().toString()) {
						log.warn("PMID inconsistency detected [old:" + statement.getObject().toString() + 
							", new:" + identifiers.get(Bibliographic.LABEL_PMID) + "]");
					}
					identifiers.remove(Bibliographic.LABEL_PMID);
				}
			}
			if(label==Bibliographic.PMCID) {
				if(identifiers.get(Bibliographic.LABEL_PMCID)!=null) {
					if(identifiers.get(Bibliographic.LABEL_PMCID)!=statement.getObject().toString()) {
						log.warn("PMCID inconsistency detected [old:" + statement.getObject().toString() + 
							", new:" + identifiers.get(Bibliographic.LABEL_PMCID) + "]");
					}
					identifiers.remove(Bibliographic.LABEL_PMCID);
				}
			}
			if(label==Bibliographic.PII) {
				if(identifiers.get(Bibliographic.LABEL_PII)!=null) {
					if(identifiers.get(Bibliographic.LABEL_PII)!=statement.getObject().toString()) {
						log.warn("PII inconsistency detected [old:" + statement.getObject().toString() + 
							", new:" + identifiers.get(Bibliographic.LABEL_PII) + "]");
					}
					identifiers.remove(Bibliographic.LABEL_PII);
				}
			}
			if(label==Bibliographic.TITLE) {
				if(identifiers.get(Bibliographic.LABEL_TITLE)!=null) {
					if(identifiers.get(Bibliographic.LABEL_TITLE)!=statement.getObject().toString()) {
						log.warn("Title inconsistency detected [old:" + statement.getObject().toString() +
							", new:" + identifiers.get(Bibliographic.LABEL_TITLE) + "]");
					}
					//identifiers.remove(Bibliographic.LABEL_TITLE);
				} 
			}
		}
		
		updateModelWithIdentifiers(expression, metaModel, identifiers)
		println metaModel;
		dataset.addNamedModel(configAccessService.getAsString("annotopia.storage.uri.graph.identifiers"), metaModel);
	}
}
