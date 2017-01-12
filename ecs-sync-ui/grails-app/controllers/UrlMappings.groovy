import sync.ui.ConfigException

class UrlMappings {
    static mappings = {
        "/storage/$path**"(controller: 'storage', action: 'get')

        "/$controller/$action?/$id?(.$format)?"{
            constraints {
                // apply constraints here
            }
        }

        "/"(controller: 'status')
        "500"(controller: 'uiConfig', action: 'index', exception: ConfigException)
        "500"(view: '/notRunning', exception: ConnectException)
        "500"(view: '/error')
        "404"(view: '/notFound')
    }
}
