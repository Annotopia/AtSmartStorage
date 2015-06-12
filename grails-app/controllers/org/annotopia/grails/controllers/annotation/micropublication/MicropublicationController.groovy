package org.annotopia.grails.controllers.annotation.micropublication;

import org.annotopia.groovy.service.store.BaseController;

import grails.converters.JSON

import javax.servlet.http.HttpServletResponse

import org.annotopia.groovy.service.store.BaseController
import org.annotopia.groovy.service.store.StoreServiceException
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.RDFLanguages
import org.codehaus.groovy.grails.web.json.JSONArray
import org.codehaus.groovy.grails.web.json.JSONObject

import com.github.jsonldjava.core.JsonLdOptions
import com.github.jsonldjava.core.JsonLdProcessor
import com.github.jsonldjava.utils.JsonUtils
import com.hp.hpl.jena.query.Dataset
import com.hp.hpl.jena.query.DatasetFactory
import com.hp.hpl.jena.rdf.model.Model


class MicropublicationController extends BaseController {

	def apiKeyAuthenticationService
	def openAnnotationStorageService
	def configAccessService

	// Shared variables/functionality
	def startTime
	def apiKey
	def outCmd
	def incGph
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
		outCmd = (request.JSON.outCmd!=null)?request.JSON.outCmd:"none";
		if(params.outCmd!=null) outCmd = params.outCmd;

		incGph = (request.JSON.incGph!=null)?request.JSON.incGph:"false";
		if(params.incGph!=null) incGph = params.incGph;

		// Pagination
		def max = (request.JSON.max!=null)?request.JSON.max:"100000000";
		if(params.max!=null) max = params.max;
		def offset = (request.JSON.offset!=null)?request.JSON.offset:"0";
		if(params.offset!=null) offset = params.offset;

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
			" max:" + max + " offset:" + offset +
			((tgtUrl!=null) ? (" tgtUrl:" + tgtUrl):"") +
			((tgtFgt!=null) ? (" tgtFgt:" + tgtFgt):"") +
			((tgtExt!=null) ? (" tgtExt:" + tgtExt):"") +
			((tgtIds!=null) ? (" tgtIds:" + tgtIds):"") +
			((flavor!=null) ? (" flavor:" + flavor):"") +
			((outCmd!=null) ? (" outCmd:" + outCmd):"") +
			((incGph!=null) ? (" incGph:" + incGph):""));

		def annotationGraphs = openAnnotationStorageService.listAnnotation(apiKey, max, offset, tgtUrls,
			tgtFgt, tgtExt, tgtIds, incGph, sourcesFacet, motivationsFacet)
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
					list.add(JsonUtils.toPrettyString(content))
				}
			}
		}

		response.contentType = "application/json;charset=UTF-8"
		response.outputStream << "["+list.join(",")+"]"
		response.outputStream.flush()
	}
}
