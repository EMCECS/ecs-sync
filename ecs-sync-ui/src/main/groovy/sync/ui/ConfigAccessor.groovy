package sync.ui

trait ConfigAccessor {
    def ecsConfigService
    def fileConfigService

    ConfigService getConfigService() {
        getConfigService(ConfigService.getConfig())
    }

    ConfigService getConfigService(uiConfig) {
        if (uiConfig.configStorageType == ConfigStorageType.ECS) return ecsConfigService
        else if (uiConfig.configStorageType == ConfigStorageType.LocalDisk) return fileConfigService
    }
}
