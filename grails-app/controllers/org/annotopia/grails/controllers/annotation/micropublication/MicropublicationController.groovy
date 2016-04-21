package org.annotopia.grails.controllers.annotation.micropublication;

import org.annotopia.grails.connectors.BaseConnectorController

import grails.converters.JSON

import javax.servlet.http.HttpServletResponse

import org.annotopia.groovy.service.store.BaseController
import org.annotopia.groovy.service.store.StoreServiceException
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.RDFLanguages
import org.codehaus.groovy.grails.web.json.JSONArray
import org.codehaus.groovy.grails.web.json.JSONObject

import org.annotopia.grails.vocabularies.AnnotopiaVocabulary
import org.annotopia.grails.vocabularies.OA
import org.annotopia.grails.vocabularies.PAV
import org.annotopia.grails.vocabularies.RDF

import com.github.jsonldjava.core.JsonLdOptions
import com.github.jsonldjava.core.JsonLdProcessor
import com.github.jsonldjava.utils.JsonUtils
import com.hp.hpl.jena.query.Dataset
import com.hp.hpl.jena.query.DatasetFactory
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.query.QuerySolution
import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.query.ResultSet
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.RDFNode
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.rdf.model.Resource
import com.hp.hpl.jena.rdf.model.ResourceFactory
import com.hp.hpl.jena.rdf.model.Statement
import com.hp.hpl.jena.rdf.model.StmtIterator

import virtuoso.jena.driver.VirtGraph
import virtuoso.jena.driver.VirtuosoQueryExecution
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory


class MicropublicationController extends BaseConnectorController {

	def apiKeyAuthenticationService
	def openAnnotationStorageService
	def jenaVirtuosoStoreService
	def configAccessService

	// Shared variables/functionality
	def startTime
	def apiKey
	def beforeInterceptor = {
		startTime = System.currentTimeMillis()

		// Authenticate with OAuth or Annotopia API Key
		def oauthToken = apiKeyAuthenticationService.getOauthToken(request)
		if(oauthToken != null) {
			apiKey = oauthToken.system.apikey
		} else {
			apiKey = apiKeyAuthenticationService.getApiKey(request)

			if(!apiKeyAuthenticationService.isApiKeyValid(request.getRemoteAddr(), apiKey)) {
				invalidApiKey(request.getRemoteAddr())
				return false // Returning false stops the actual controller action from being called
			}
		}
		log.info("API key [" + apiKey + "]")
	}

    def index() {
		// Target filters
		def tgtUrl = request.JSON.tgtUrl
		if(params.tgtUrl!=null) tgtUrl = params.tgtUrl;
		def tgtUrls
		if(tgtUrl != null)
			tgtUrls = [tgtUrl]
		def tgtFgt = (request.JSON.tgtFgt!=null)?request.JSON.tgtFgt:"true";
		if(params.tgtFgt!=null) tgtFgt = params.tgtFgt;

		// Target IDs
		Map<String,String> identifiers = new HashMap<String,String>();
		def tgtIds = (request.JSON.tgtIds!=null)?request.JSON.tgtIds:null;
		if(params.tgtIds!=null) tgtIds = params.tgtIds
		if(tgtIds!=null) {
			JSONObject ids = JSON.parse(tgtIds);
			ids.keys().each { key ->
				identifiers.put(key, ids.get(key));
			}
		}

		// Facets
		def sources = request.JSON.sources
		if(params.sources!=null) sources = params.sources;
		def sourcesFacet = []
		if(sources) sourcesFacet = sources.split(",");
		def motivations = request.JSON.motivations
		if(params.motivations!=null) motivations = params.motivations;
		def motivationsFacet = []
		if(motivations) motivationsFacet = motivations.split(",");

		// Currently unusued, planned
		def tgtExt = request.JSON.tgtExt
		def flavor = request.JSON.flavor

		log.info("[" + apiKey + "] List >>" +
			((tgtUrl!=null) ? (" tgtUrl:" + tgtUrl):"") +
			((tgtFgt!=null) ? (" tgtFgt:" + tgtFgt):"") +
			((tgtExt!=null) ? (" tgtExt:" + tgtExt):"") +
			((tgtIds!=null) ? (" tgtIds:" + tgtIds):"") +
			((flavor!=null) ? (" flavor:" + flavor):""));

		def annotationGraphs = openAnnotationStorageService.listAnnotation(apiKey, null, null, tgtUrls,
			tgtFgt, tgtExt, tgtIds, "false", sourcesFacet, motivationsFacet)
		def contextJson = JsonUtils.fromInputStream(
			callExternalUrl(apiKey, configAccessService.getAsString("annotopia.jsonld.micropublication.framing")))
		def list = []

		if(annotationGraphs!=null) {
			annotationGraphs.each { annotationGraph ->
				def baos = new ByteArrayOutputStream();
				if(annotationGraph.listNames().hasNext()) {
					Model m = annotationGraph.getNamedModel(annotationGraph.listNames().next());
					RDFDataMgr.write(baos, m.getGraph(), RDFLanguages.JSONLD);
				}
				def json = JsonUtils.fromString(baos.toString().replace('"@id" : "urn:x-arq:DefaultGraphNode",',''))
				def framedJson =  JsonLdProcessor.frame(json, contextJson, new JsonLdOptions());
				def outputJson = framedJson.get("@graph").getAt(0) // Don't need wrapping graph
				if(outputJson != null) {
					outputJson.put('@context', framedJson.get('@context')) // Need the context thought
					list.add(JsonUtils.toPrettyString(outputJson))
				}
			}
		}

		response.contentType = "application/json;charset=UTF-8"
		response.outputStream << "["+list.join(",")+"]"
		response.outputStream.flush()
	}
	
	private VirtGraph getGraph() {
		return new VirtGraph (
			configAccessService.getAsString("annotopia.storage.triplestore.host"),
			configAccessService.getAsString("annotopia.storage.triplestore.user"),
			configAccessService.getAsString("annotopia.storage.triplestore.pass"));
	}

	
	def search = {
		log.info 'search micropubs ' + params
		long startTime = System.currentTimeMillis();

		// Pagination
		def max = (request.JSON.max!=null)?request.JSON.max:"10";
		if(params.max!=null) max = params.max;
		def offset = (request.JSON.offset!=null)?request.JSON.offset:"0";
		if(params.offset!=null) offset = params.offset;
		
		// retrieve the return format
		def format = retrieveValue(request.JSON.format, params.format, "annotopia");
		
		// retrieve the query
		def query = retrieveValue(request.JSON.q, params.q, "q", startTime);
		if(!query) { return; }
		
		if(query!=null && !query.empty) {
			try {
				JSONObject results = new JSONObject();
				
				Set<String> graphNames = new HashSet<>();
				StringBuffer queryBuffer = new StringBuffer()
				queryBuffer << "PREFIX rdfs: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " + 
					"PREFIX ao: <http://www.w3.org/ns/oa#> " + 
					"PREFIX mp: <http://purl.org/mp/> " + 
					"SELECT DISTINCT ?g { " + 
					" GRAPH ?g" +
					" {" +
					"	 ?s ?p ?o ." + 
					"	 ?a rdfs:type ao:Annotation ." + 
					"	 ?a ao:hasBody ?body ." + 
					"	 ?body rdfs:type mp:Micropublication  ." + 
					"  }" + 
					"} " +
					"LIMIT " + max +
					"OFFSET " + offset;
				
				def resultingDataSet = DatasetFactory.createMem();
				def model = ModelFactory.createDefaultModel();
				VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(QueryFactory.create(queryBuffer.toString()), getGraph());
				ResultSet filterMatches = vqe.execSelect();
				while (filterMatches.hasNext()) {
					QuerySolution result = filterMatches.nextSolution();
					graphNames.add(result.get("g").toString());
				}
				
				println 'hello'
				Set<Dataset> datasets = new HashSet<Dataset>();
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
						
//						if(incGph==INCGPH_YES) {
//							Model m = jenaVirtuosoStoreService.retrieveGraphMetadata(apiKey, graphName, configAccessService.getAsString("annotopia.storage.uri.graph.provenance"));
//							if(m!=null) ds.setDefaultModel(m);
//						}
						if(ds!=null) datasets.add(ds);
					}
				}
				
				println datasets;
				
				response.outputStream << results.toString()
				response.outputStream.flush()
			} catch(Exception e) {
				log.error("Exception: " + e.getMessage() + " " + e.getClass().getName());
				render(status: 500, text: returnMessage(apiKey, "error", e.getMessage(), startTime), contentType: "text/json", encoding: "UTF-8");
				return;
			}
		} else {
			def message = 'Query text is null';
			log.error("Exception: " + message);
			render(status: 400, text: returnMessage(apiKey, "nocontent", message, startTime), contentType: "text/json", encoding: "UTF-8");
			return;
		}	
	}
}
