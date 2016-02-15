import groovy.time.TimeCategory

class BootStrap {

    def init = { servletContext ->
        Integer.metaClass.mixin TimeCategory
        Date.metaClass.mixin TimeCategory
    }
    def destroy = {
    }
}
