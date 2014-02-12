class UrlMappings {

	static mappings = {
		"/annotationset/$id?"{
			controller = "annotationSet"
			action = [GET:"show", POST:"save"]
		}
		"/annotation/$id?"{
			controller = "annotation"
			action = [GET:"show", POST:"save"]
		}
		
		"/$controller/$action?/$id?"{
			constraints {
				// apply constraints here
			}
		}

		"/"(view:"/index")
		"500"(view:'/error')
	}
}
