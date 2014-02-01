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
package org.annotopia.grails.services.storage.jena.validation.oa

import org.apache.jena.riot.RDFDataMgr
import org.json.simple.JSONValue

import com.github.jsonldjava.jena.JenaJSONLD
import com.github.jsonldjava.utils.JSONUtils
import com.hp.hpl.jena.query.Dataset
import com.hp.hpl.jena.query.DatasetFactory
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.query.QueryExecution
import com.hp.hpl.jena.query.QueryExecutionFactory
import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.query.QuerySolution
import com.hp.hpl.jena.query.ResultSet
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.RDFNode

/**
 * This service allows to validate RDF content against the
 * Open Annotation Model http://www.openannotation.org/spec/core/
 * 
 * This class is a rewritten adaptation of the one originally written 
 * by Anna Gerber for the lorestore project. This new version is in Groovy 
 * and works with Jena/ARQ APIs.
 *
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class OpenAnnotationValidationService {

	/*
	 * This file has been created by Anna Gerber for the lorestore
	 * project 
	 */
	private final VALIDATION_RULES_FILE = "OAConstraintsSPARQL.json";
	
	/**
	 * This method validates the input agains the Open Annotation data model
	 * specifications http://www.openannotation.org/spec/core/
	 * 
	 * TODO: Validate annotations in graphs
	 * 
	 * @param inputStream	The inputStream of the resource to validate
	 * @param contentType	The content type of the resource to validate (Accepted 
	 *                      JSON-LD and therefore 'application/json'
	 * @return The map with a summary and all the tests and results
	 */
	public HashMap<String,Object> validate(InputStream inputStream, String contentType) {

		// Validation rules (later loaded from the external json file)
		List<Map<String, Object>> validationRules = null;

		// Results
		HashMap<String,Object> graphResult = new HashMap<String, Object>();

		try {
			if (contentType.contains("application/json")) {
				try {
					
					Dataset dataset = DatasetFactory.createMem();
					
					try {
						// Using the RIOT reader
						RDFDataMgr.read(dataset, inputStream, "http://localhost/jsonld/", JenaJSONLD.JSONLD);
					} catch (Exception ex) {
						graphResult.put("exception", createException("Content parsing failed", "Failure while: loading of the content to validate " + ex.toString()));
						return graphResult;
					} 
					
					// Load validation rules
					log.info("Loading validation rules...");
					try {
						if (validationRules == null) {
							String currentDir = new File(".").getCanonicalPath()
							File file = new File(currentDir + '/web-app/data/' + VALIDATION_RULES_FILE)
							
							log.info("From file " + file.getAbsoluteFile());
							InputStream inp = new FileInputStream(file);
							validationRules = (List<Map<String, Object>>) JSONUtils.fromInputStream(inp, "UTF-8");
						}
					} catch (IOException ex) {
						graphResult.put("exception", createException("Validation rules loading failed", "Failure while: loading the validation rules " + ex.toString()));
						return graphResult;
					}
					
					// Query for graphs containing annotation
					// See: https://www.mail-archive.com/wikidata-l@lists.wikimedia.org/msg00370.html
					HashMap<String, Model> models = new HashMap<String, Model>();
					boolean graphRepresentationDetected = false;
					List<String> annotationsGraphs = new ArrayList<String>();
					Query  sparqlAnnotationGraphs = QueryFactory.create("PREFIX oa: <http://www.w3.org/ns/oa#> SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s a oa:Annotation . }}");
					QueryExecution qAnnotationGraphs = QueryExecutionFactory.create (sparqlAnnotationGraphs, dataset);
					ResultSet rAnnotationGraphs = qAnnotationGraphs.execSelect();
					while (rAnnotationGraphs.hasNext()) {
						QuerySolution querySolution = rAnnotationGraphs.nextSolution();
						annotationsGraphs.add(querySolution.get("g").toString());
						graphRepresentationDetected = true;
					}
					// TODO Graphs
					// For each graph containing annotations run the validation code 
					// The summary will have to reflect the analyzed graphs
					if(graphRepresentationDetected) {
						annotationsGraphs.each {
							println '*********** ' + it
							println '** validate ' + dataset.getNamedModel(it);
							models.put(it, dataset.getNamedModel(it));
						}
					} else {
						println '--- DEFAULT GRAPH CASE'
						models.put("default", dataset.getDefaultModel());
					}
					
					
					models.keySet().each { modelName ->
						Model firstModel = models.get(modelName);
						
						log.info("Applying validation rules...");
						int totalPass = 0, totalError = 0, totalWarn = 0, totalSkip = 0, totalTotal = 0;
						ArrayList<Map<String, Object>> result = new ArrayList<Map<String, Object>>(validationRules);
						for (Map<String, Object> section : result) {
							// for each section
							int sectionPass = 0, sectionError = 0, sectionWarn = 0, sectionSkip = 0;
							for (Map<String, Object> rule : ((List<Map<String, Object>>) section.get("constraints"))) {
								log.debug("Number of loaded constraints " + rule.size());
								
								totalTotal++;
								// process each rule
								String queryString = (String) rule.get("query");
								String preconditionQueryString = (String) rule.get("precondition");
								log.debug("Preconditions:  " + preconditionQueryString);
								
								if (queryString == null || "".equals(queryString)) {
									totalSkip++;
									sectionSkip++;
									rule.put("status", "skip");
									rule.put("result", "Validation rule not implemented");
								} else {
									log.debug("Query: " + queryString);
								
									int count = 0;
									try {
										boolean preconditionOK = true;
										
										// Checking pre-conditions (ASK)
										if (preconditionQueryString != null && !"".equals(preconditionQueryString)) {
											Query  sparql = QueryFactory.create(preconditionQueryString);
											QueryExecution qe = QueryExecutionFactory.create (sparql, firstModel);
											boolean preconditionSatisfied = qe.execAsk();
											if (!preconditionSatisfied) {
												log.debug("Precondition not satisfied");
												// if precondition did not produce any matches,
												// set status to skip
												rule.put("status", "skip");
												rule.put("result", "Rule does not apply to supplied data: "
													+ rule.get("preconditionMessage"));
												totalSkip++;
												sectionSkip++;
												preconditionOK = false;
											}
										}
										
										// If precondition satisfied, checking rule
										if (preconditionOK) {
											log.info("Running query... " + queryString);
											// run the query and store the result back into the
											// constraint object
											Query  sparql = QueryFactory.create(queryString);
											QueryExecution qe = QueryExecutionFactory.create (sparql, firstModel);
											ResultSet rs = qe.execSelect();
											List<String> vars = rs.getResultVars();
											List<Map<String, String>> matches = new ArrayList<Map<String, String>>();
											while (rs.hasNext()) {
												QuerySolution querySolution = rs.nextSolution();
												boolean nullValues = true;
												for (String var : vars) {
													RDFNode val = querySolution.get(var);
													if (val != null && !val.toString().equals("0")) {
														nullValues = false;
														HashMap<String, String> r = new HashMap<String, String>();
														r.put(var, querySolution.get(var).toString());
														matches.add(r);
													}
												}
												if (!nullValues) {
													count++;
												}
											}
											if (count == 0) {
												rule.put("status", "pass");
												rule.put("result", "");
												totalPass++;
												sectionPass++;
											} else {
												// if there are results, the validation failed,
												// so set the status from the severity
												// add results to the result so that they can be
												// displayed
												rule.put("result", matches);
												String severity = (String) rule.get("severity");
												rule.put("status", severity);
												if ("error".equals(severity)) {
													totalError++;
													sectionError++;
												} else {
													totalWarn++;
													sectionWarn++;
												}
											}	
										}						
									} catch (Exception e) {
										// if there were any errors running queries, set status
										// to skip
										log.warn("error validating: "
												+ rule.get("description") + " "
												+ e.getMessage());
										rule.put("status", "skip");
										rule.put("result", "Error evaluating validation rule: "
												+ e.getMessage());
										totalSkip++;
										sectionSkip++;
									}
								}
								
								// section summaries for validation report
								section.put("pass", sectionPass);
								section.put("error", sectionError);
								section.put("warn", sectionWarn);
								section.put("skip", sectionSkip);
								if (sectionError > 0) {
									section.put("status", "error");
								} else if (sectionWarn > 0) {
									section.put("status", "warn");
								} else if (sectionPass == 0) {
									section.put("status", "skip");
								} else {
									section.put("status", "pass");
								}
							}
						}
						
						graphResult.put("model", modelName);
						graphResult.put("result", result);
						graphResult.put("error", totalError);
						graphResult.put("warn", totalWarn);
						graphResult.put("pass", totalPass);
						graphResult.put("skip", totalSkip);
						graphResult.put("total", totalTotal);
					}
					return graphResult;
				} catch (Exception ex) {
					graphResult.put("exception", createException("OA JSON validation failed", "Failure while: validating the JSON " + ex.toString()));
					return graphResult;
				}
			} else {
				graphResult.put("exception", createException("Format not supported", "Format not supported:" + contentType));
				return graphResult;
			}
		} catch (Exception ex) {
			graphResult.put("exception", createException("OA validation failed", "Failure while: validating the OA content " + ex.toString()));
			return graphResult;
		}
	}
	
	private Map<String, Object> createException(String label, String message) {
		log.error("{\"label\": \"" + label + "\", \"message\": \"" + message + "\"}");
		Map<String, Object> exception = new HashMap<String, Object>();
		exception.put("label", label);
		exception.put("message", message);
		return exception;
	}
}
