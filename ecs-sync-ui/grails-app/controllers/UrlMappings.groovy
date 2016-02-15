import com.emc.object.s3.S3Exception

class UrlMappings {

    static mappings = {
        "/$controller/$action?/$id?(.$format)?"{
            constraints {
                // apply constraints here
            }
        }

        "/"(controller: 'status')
        "500"(controller: 'uiConfig', action: 'index', exception: S3Exception)
        "500"(view:'/error')
        "404"(view:'/notFound')
    }
}
