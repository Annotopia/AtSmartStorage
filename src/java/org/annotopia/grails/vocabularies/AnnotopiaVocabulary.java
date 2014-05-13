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
package org.annotopia.grails.vocabularies;

/**
 * This is a class collecting the properties and classes that are 
 * proper of Annotopia. 
 * 
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
public interface AnnotopiaVocabulary {

	public static final String AT_STATUS 	= "http://purl.org/annotopia#status";
	public static final String AT_CURRENT 	= "http://purl.org/annotopia#current";
	
	public static final String ANNOTATION_SET_GRAPH    	= "http://purl.org/annotopia#AnnotationSetGraph";
	public static final String ANNOTATION_GRAPH   	   	= "http://purl.org/annotopia#AnnotationGraph";
	public static final String BODY_GRAPH   			= "http://purl.org/annotopia#BodyGraph"; 
	public static final String BODY   					= "http://purl.org/annotopia#graphbody"; 
	public static final String BODIES_COUNT   			= "http://purl.org/annotopia#graphbodycount"; 
	
	public static final String ANNOTATION_SET          	= "http://purl.org/annotopia#AnnotationSet";
	public static final String ANNOTATIONS             	= "http://purl.org/annotopia#annotations";
	
	public static final String ANNOTATION         		= "http://purl.org/annotopia#annotation";
	public static final String ANNOTATION_COUNT  		= "http://purl.org/annotopia#annotationcount";
	
	public static final String GRAPH_ANNOTATION         = "http://purl.org/annotopia#graphannotation";
	public static final String GRAPH_ANNOTATION_COUNT   = "http://purl.org/annotopia#graphannotationcount";
	
	public static final String HAS_CHANGED         		= "http://purl.org/annotopia#hasChanged";
}
