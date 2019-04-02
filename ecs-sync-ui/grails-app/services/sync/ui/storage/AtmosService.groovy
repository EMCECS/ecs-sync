package sync.ui.storage

import com.emc.atmos.api.AtmosConfig
import com.emc.atmos.api.ObjectPath
import com.emc.atmos.api.bean.DirectoryEntry
import com.emc.atmos.api.jersey.AtmosApiClient
import com.emc.atmos.api.request.ListDirectoryRequest
import com.emc.atmos.mgmt.TenantMgmtApi
import com.emc.atmos.mgmt.TenantMgmtConfig
import com.emc.atmos.mgmt.bean.ObjectUserStatus
import com.emc.atmos.mgmt.jersey.TenantMgmtClient
import com.emc.object.s3.S3Config
import com.emc.object.s3.jersey.S3JerseyClient

class AtmosService {
    private clientMap = [:]

    def testConfig(AtmosStorage atmosStorage) {
        try {
            // check mgmt access
            getClient(atmosStorage).listPolicies() // least expensive (quickest) method I could find to test auth
            // check Atmos API
            HttpURLConnection con = "${atmosStorage.atmosApiEndpoint}/rest".toURL().openConnection() as HttpURLConnection
            if (con.responseCode != 200)
                throw new RuntimeException("Atmos API is not valid (${con.responseCode} status code received)")
            // check S3 API
            con = atmosStorage.s3ApiEndpoint.toURL().openConnection() as HttpURLConnection
            if (con.responseCode != 400)
                throw new RuntimeException("S3 API is not valid (expected 400 response, but got ${con.responseCode})")
            if (!con.errorStream.getText("UTF-8").find(/<Code>MissingSecurityHeader<\/Code>/))
                throw new RuntimeException("S3 API is not valid (expected MissingSecurityHeader error)")
            ''
        } catch (e) {
            return e.getMessage()
        }
    }

    def getTenantInfo(AtmosStorage atmosStorage) {
        getClient(atmosStorage).getTenantInfo()
    }

    def getSubtenantDetails(AtmosStorage atmosStorage, subtenantName) {
        getClient(atmosStorage).getSubtenant(subtenantName)
    }

    def getUserSecretKey(AtmosStorage atmosStorage, subtenantName, uid) {
        getClient(atmosStorage).getSharedSecret(subtenantName, uid)
    }

    def listAllBuckets(AtmosStorage atmosStorage, subtenantName) {
        // need to grab subtenantId
        def subtenant = getSubtenantDetails(atmosStorage, subtenantName)

        // gather Atmos API endpoint details
        def endpoint = atmosStorage.atmosApiEndpoint

        // grab first active user (other users will not have access)
        def objectUser = subtenant.subtenant.objectUsers.find { it.status == ObjectUserStatus.Operational }

        // all users should have read access to the s3/ folder by design, so we just need an active user
        def uid = subtenant.subtenant.id + '/' + objectUser.uid
        def secret = objectUser.sharedSecret
        def atmosConfig = new AtmosConfig(uid, secret, endpoint.toURI())
        atmosConfig.disableSslValidation = true
        def atmosClient = new AtmosApiClient(atmosConfig)
        def buckets = []
        def request = new ListDirectoryRequest().path(new ObjectPath("/s3/"))
        while (true) {
            def response = atmosClient.listDirectory(request)
            buckets += response.entries.findAll { it.fileType == DirectoryEntry.FileType.directory }.collect {
                it.filename
            }
            if (!request.token) break
        }
        buckets
    }

    def listBucketsForUser(AtmosStorage atmosStorage, subtenantName, uid) {
        // need to grab subtenantId
        def subtenant = getSubtenantDetails(atmosStorage, subtenantName)

        // gather S3 API endpoint details
        def endpoint = atmosStorage.s3ApiEndpoint

        // find user
        def user = subtenant.subtenant.objectUsers.find { it.uid == uid }
        if (!user) throw new IllegalArgumentException("Cannot find uid ${uid} in subtenant ${subtenantName} (id: ${subtenant.subtenant.id})")

        // find secret key for user
        def secret = user.sharedSecret

        def uidToken = subtenant.subtenant.id + '/' + uid
        def s3Config = new S3Config(endpoint.toURI()).withIdentity(uidToken).withSecretKey(secret)
        def s3Client = new S3JerseyClient(s3Config)
        s3Client.listBuckets().buckets.collect { it.name }
    }

    private synchronized TenantMgmtApi getClient(AtmosStorage atmosStorage) {
        def client = clientMap[atmosStorage]
        if (!client) {
            def config = new TenantMgmtConfig(atmosStorage.tenantName, atmosStorage.tenantAdminUser, atmosStorage.tenantAdminPassword, atmosStorage.managementEndpoint.toURI())
            config.setDisableSslValidation(true)
            client = new TenantMgmtClient(config)
            clientMap[atmosStorage] = client
        }
        return client
    }
}
