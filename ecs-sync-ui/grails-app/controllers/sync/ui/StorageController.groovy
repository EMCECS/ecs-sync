package sync.ui

class StorageController implements ConfigAccessor {
    static allowedMethods = [get: "GET"]

    def grailsMimeUtility

    def get(String path) {
        if (configService.configObjectExists(path)) {
            def file = new File(path)
            def filename = file.name
            response.setHeader('Content-Disposition', "attachment; filename=${filename}")
            if (filename.contains('.'))
                response.contentType = grailsMimeUtility.getMimeTypeForExtension(filename.substring(filename.lastIndexOf('.') + 1))
            response.outputStream << configService.readConfigObject(path, InputStream.class)
            response.outputStream.flush()
        } else {
            render status: 404
        }
    }
}