package com.emc.ecs.sync.config.storage;

import com.emc.ecs.sync.config.*;
import com.emc.ecs.sync.config.annotation.*;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import static com.emc.ecs.sync.config.storage.EcsS3Config.URI_PREFIX;

@XmlRootElement
@StorageConfig(uriPrefix = URI_PREFIX)
@Label("Azure Blob")
@Documentation("Reads content from an Azure Blob Storage. This " +
        "plugin is triggered by the pattern:\n" +
        "DefaultEndpointsProtocol=https;AccountName=[containerName];AccountKey=[accountKey];EndpointSuffix=core.windows.net" + "\n" +
        "Note that this plugin only used as target which need to be sync." + "\n" +
        "Please run the sync without mpu when target is Azure blob storage.")

@Role(RoleType.Source)
public class AzureBlobConfig extends AbstractConfig {
    public static final String URI_PREFIX = "azure-blob:";

    private String connectionString;
    private String containerName;
    private String blobPrefix;
    private boolean isIncludeSnapShots;

    @XmlTransient
    @UriGenerator
    public String getUri() {
        return URI_PREFIX + bin(connectionString);
    }

    @UriParser
    public void setUri(String uri) {
        assert uri.startsWith(URI_PREFIX) : "invalid uri " + uri;
        connectionString = uri.substring(URI_PREFIX.length());
    }

    @Option(orderIndex = 10, locations = Option.Location.Form, required = true, description = "The connection string passed to the Azure blob SDK. Should be of the form DefaultEndpointsProtocol=https;AccountName=quickstart;AccountKey=fsbegyIZ2pknO4JO7rhh38fXPqkwVRfliw2SFQWhHrF2pL4TNuz3B40TfeBz77gZ9RtPbsS9JppwD0pDqrCRiw==;EndpointSuffix=core.windows.net\n")
    public String getConnectionString() {
        return connectionString;
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    @Option(orderIndex = 20, locations = Option.Location.Form, required = true, description = "The container name which need to specified, only sync the blobs which belongs to this container.")
    public String getContainerName() {
        return containerName;
    }

    public void setContainerName(String containerName) {
        this.containerName = containerName;
    }

    @Option(orderIndex = 40, locations = Option.Location.Form, advanced = true, description = "The prefix of blobs to use when enumerating to the bucket.")
    public String getBlobPrefix() {
        return blobPrefix;
    }

    public void setBlobPrefix(String blobPrefix) {
        this.blobPrefix = blobPrefix;
    }

    @Option(orderIndex = 50, advanced = true, description = "Enable to transfer all snapshots of every blob object")
    public boolean isIncludeSnapShots() {
        return isIncludeSnapShots;
    }

    public void setIncludeSnapShots(boolean includeSnapShots) {
        this.isIncludeSnapShots = includeSnapShots;
    }
}
