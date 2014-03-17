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
 * This is a class collecting the properties defined by the Provenance
 * Authoring and Versioning (PAV) ontology. 
 * 
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
public interface PavVocabulary {

	/**
	 * pav:createdBy
	 * See: http://pav-ontology.googlecode.com/svn/trunk/pav.html#d4e213
	 */
	public static final String PAV_CREATED_BY 		= "http://purl.org/pav/createdBy";
	/**
	 * pav:createdAt
	 * See: http://pav-ontology.googlecode.com/svn/trunk/pav.html#d4e197
	 */
	public static final String PAV_CREATED_AT 		= "http://purl.org/pav/createdAt";
	/**
	 * pav:createdWith
	 * See: http://pav-ontology.googlecode.com/svn/trunk/pav.html#d4e241
	 */
	public static final String PAV_CREATED_WITH 	= "http://purl.org/pav/createdWith";
	
	/**
	 * pav:lastUpdatedBy
	 * See: (not in PAV yet)
	 */
	public static final String PAV_LAST_UPDATED_BY 	= "http://purl.org/pav/lastUpdatedBy";	
	/**
	 * pav:
	 * See: http://pav-ontology.googlecode.com/svn/trunk/pav.html#d4e641
	 */
	public static final String PAV_LAST_UPDATED_ON 	= "http://purl.org/pav/lastUpdatedOn";
	
	/**
	 * pav:previousVersion
	 * See: http://pav-ontology.googlecode.com/svn/trunk/pav.html#d4e366
	 */
	public static final String PAV_PREVIOUS_VERSION = "http://purl.org/pav/previousVersion";
}
