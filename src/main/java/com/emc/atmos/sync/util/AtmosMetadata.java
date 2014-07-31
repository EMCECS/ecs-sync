/*
 * Copyright 2013 EMC Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.emc.atmos.sync.util;

import com.emc.atmos.api.Acl;
import com.emc.atmos.api.bean.Metadata;
import com.emc.atmos.api.bean.ObjectMetadata;
import com.emc.atmos.api.bean.Permission;
import com.google.gson.*;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

/**
 * Similar to the Atmos API's ObjectMetadata, but it splits the system
 * metadata out into a separate collection and supports serializing to and 
 * from a standard JSON format.
 * @author cwikj
 */
public class AtmosMetadata {
	private static final String VALUE_PROP = "value";
	private static final String LISTABLE_PROP = "listable";
	private static final String PERMISSION_PROP = "permission";
	private static final String TYPE_PROP = "type";
	private static final String NAME_PROP = "name";
	private static final String GRANTEE_PROP = "grantee";
	private static final String SYSTEM_METADATA_PROP = "systemMetadata";
	private static final String CONTENT_TYPE_PROP = "contentType";
	private static final String ACL_PROP = "acl";
    private static final String ACL_GROUPS_PROP = "groups";
    private static final String ACL_USERS_PROP = "users";
	private static final String METADATA_PROP = "metadata";
    private static final String RETENTION_ENABLED_PROP = "retentionEnabled";
    private static final String RETENTION_END_PROP = "retentionEndDate";
    private static final String EXPIRATION_ENABLED_PROP = "expirationEnabled";
    private static final String EXPIRATION_PROP = "expirationDate";
    private static final Logger l4j = Logger.getLogger(AtmosMetadata.class);
	
	private Map<String, Metadata> metadata;
	private Map<String, Metadata> systemMetadata;
	private Acl acl;
	private String contentType;
    private boolean retentionEnabled;
    private Date retentionEndDate;
    private boolean expirationEnabled;
    private Date expirationDate;
	
	public static final String META_DIR = ".atmosmeta"; // Special subdir for Atmos metadata
	public static final String DIR_META = ".dirmeta"; // Special file for directory-level metadata


	private static final String[] SYSTEM_METADATA_TAGS = new String[] {
		"atime",
		"ctime",
		"gid",
		"itime",
		"mtime",
		"nlink",
		"objectid",
		"objname",
		"policyname",
		"size",
		TYPE_PROP,
		"uid",
		"x-emc-wschecksum"
	};
	private static final Set<String> SYSTEM_TAGS = 
			Collections.unmodifiableSet(
					new HashSet<String>(Arrays.asList(SYSTEM_METADATA_TAGS)));

    // tags that should not be returned as user metadata, but in rare cases have been
    private static final String[] BAD_USERMETA_TAGS = new String[] {
        "user.maui.expirationEnd",
        "user.maui.retentionEnd"
    };
    private static final Set<String> BAD_TAGS =
            Collections.unmodifiableSet(
                    new HashSet<String>(Arrays.asList(BAD_USERMETA_TAGS)));

	/**
	 * Creates an instance of AtmosMetadata based on an ObjectMetadata
	 * retrieved through the Atmos API.  This separates the system metadata
	 * from the user metadata.
	 * 
	 * @param om the Object Metadata
	 * @return an AtmosMetadata
	 */
	public static AtmosMetadata fromObjectMetadata(ObjectMetadata om) {
		AtmosMetadata meta = new AtmosMetadata();
		
		Map<String, Metadata> umeta = new TreeMap<String, Metadata>();
		Map<String, Metadata> smeta = new TreeMap<String, Metadata>();
		for(Metadata m : om.getMetadata().values()) {
            if (BAD_TAGS.contains(m.getName())) {
                continue;
            } else if (SYSTEM_TAGS.contains(m.getName())) {
				smeta.put(m.getName(), m);
			} else {
				umeta.put(m.getName(), m);
			}
		}
		meta.setMetadata(umeta);
		meta.setSystemMetadata(smeta);
		meta.setContentType(om.getContentType());
		meta.setAcl(om.getAcl());
		
		return meta;
	}
	
	/**
	 * For a given file, returns the appropriate file that should contain that
	 * file's AtmosMetadata.  This is a file with the same name inside the
	 * .atmosmeta subdirectory.  If the file is a directory, it's 
	 * ./.atmosmeta/.dirmeta.
	 * @param f the file to compute the metadata file name from
	 * @return the file that should contain this file's metadata.  The file
	 * may not exist.
	 */
	public static File getMetaFile(File f) {
		if(f.isDirectory()) {
			return new File(new File(f, META_DIR), DIR_META);
		} else {
			return new File(new File(f.getParentFile(), META_DIR), f.getName());
		}
	}
	
	/**
	 * Reads the given metadata file and builds an AtmosMetadata from the
	 * file's JSON contents.  
	 * @param metaFile the metadata file, see getMetaFile().
	 * @return the AtmosMetadata object.
	 * @throws IOException if reading the file fails
	 */
	public static AtmosMetadata fromFile(File metaFile) throws IOException {
		BufferedReader br = null;
		
		try {
			StringBuffer sb = new StringBuffer();
			br = new BufferedReader(new InputStreamReader(
				new FileInputStream(metaFile), "UTF-8"));
			String s = null;
			while((s = br.readLine()) != null) {
				sb.append(s);
				sb.append("\n");
			}
			return fromJson(sb.toString());
		} finally {
			if(br != null) {
				br.close();
			}
		}
	}
	

	public static AtmosMetadata fromJson(String json) {
		AtmosMetadata am = new AtmosMetadata();
		JsonParser jp = new JsonParser();
		
		JsonElement je = jp.parse(json);
		JsonObject mdata = (JsonObject)je;
		
		JsonObject jsonMetadata = (JsonObject) mdata.get(METADATA_PROP);
		JsonObject jsonAcl = (JsonObject) mdata.get(ACL_PROP);
		JsonElement jsonMime = mdata.get(CONTENT_TYPE_PROP);
		JsonObject jsonSysmeta = (JsonObject) mdata.get(SYSTEM_METADATA_PROP);
        JsonElement jsonRetentionEnabled = mdata.get(RETENTION_ENABLED_PROP);
        JsonElement jsonRetentionEnd = mdata.get(RETENTION_END_PROP);
        JsonElement jsonExpirationEnabled = mdata.get(EXPIRATION_ENABLED_PROP);
        JsonElement jsonExpiration = mdata.get(EXPIRATION_PROP);
		
		if(jsonMetadata != null) {
			am.setMetadata(decodeMetadata(jsonMetadata));
		} else {
			am.setMetadata(new TreeMap<String, Metadata>());
		}
		if(jsonAcl != null) {
			am.setAcl(decodeAcl(jsonAcl));
		} else {
			am.setAcl(null);
		}
		if(jsonMime != null) {
			am.setContentType(jsonMime.getAsString());
		}
		if(jsonSysmeta != null) {
			am.setSystemMetadata(decodeMetadata(jsonSysmeta));
		} else {
			am.setSystemMetadata(new TreeMap<String, Metadata>());
		}
        if (jsonRetentionEnd != null) {
            am.setRetentionEnabled(jsonRetentionEnabled.getAsBoolean());
            am.setRetentionEndDate(Iso8601Util.parse(jsonRetentionEnd.getAsString()));
        }
        if (jsonExpiration != null) {
            am.setExpirationEnabled(jsonExpirationEnabled.getAsBoolean());
            am.setExpirationDate(Iso8601Util.parse(jsonExpiration.getAsString()));
        }

        return am;
	}
	
	private static Acl decodeAcl(JsonObject jsonAcl) {
		Acl acl = new Acl();

        JsonArray groups = (JsonArray) jsonAcl.get(ACL_GROUPS_PROP);
        JsonArray users = (JsonArray) jsonAcl.get(ACL_USERS_PROP);
		for (JsonElement ele : groups) {
			JsonObject grant = (JsonObject)ele;
            acl.addGroupGrant(grant.get(NAME_PROP).getAsString(),
                              Permission.valueOf(grant.get(PERMISSION_PROP).getAsString()));
		}
        for (JsonElement ele : users) {
            JsonObject grant = (JsonObject)ele;
            acl.addUserGrant(grant.get(NAME_PROP).getAsString(),
                             Permission.valueOf(grant.get(PERMISSION_PROP).getAsString()));
        }
		
		return acl;
	}

	private static Map<String, Metadata> decodeMetadata(JsonObject jsonMetadata) {
		Map<String, Metadata> meta = new TreeMap<String, Metadata>();
		
		for(Entry<String, JsonElement> ent : jsonMetadata.entrySet()) {
			String name = ent.getKey();
			JsonObject value = (JsonObject) ent.getValue();
			boolean listable = value.get(LISTABLE_PROP).getAsBoolean();
			String mvalue = null;
            if (value.get(VALUE_PROP) != null) mvalue = value.get(VALUE_PROP).getAsString();
			Metadata m = new Metadata(name, mvalue, listable);
			meta.put(name, m);
		}
		
		return meta;
	}
	
	public AtmosMetadata() {
		metadata = new TreeMap<String, Metadata>();
		systemMetadata = new TreeMap<String, Metadata>();
	}


	/**
	 * @return the metadata
	 */
	public Map<String, Metadata> getMetadata() {
		return metadata;
	}

	/**
	 * @param metadata the metadata to set
	 */
	public void setMetadata(Map<String, Metadata> metadata) {
		this.metadata = metadata;
	}

	/**
	 * @return the systemMetadata
	 */
	public Map<String, Metadata> getSystemMetadata() {
		return systemMetadata;
	}

	/**
	 * @param systemMetadata the systemMetadata to set
	 */
	public void setSystemMetadata(Map<String, Metadata> systemMetadata) {
		this.systemMetadata = systemMetadata;
	}

	/**
	 * @return the acl
	 */
	public Acl getAcl() {
		return acl;
	}

	/**
	 * @param acl the acl to set
	 */
	public void setAcl(Acl acl) {
		this.acl = acl;
	}

	/**
	 * @return the contentType
	 */
	public String getContentType() {
		return contentType;
	}

	/**
	 * @param contentType the contentType to set
	 */
	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

    public boolean isRetentionEnabled() {
        return retentionEnabled;
    }

    public void setRetentionEnabled( boolean retentionEnabled ) {
        this.retentionEnabled = retentionEnabled;
    }

    public Date getRetentionEndDate() {
        return retentionEndDate;
    }

    public void setRetentionEndDate( Date retentionEndDate ) {
        this.retentionEndDate = retentionEndDate;
    }

    public boolean isExpirationEnabled() {
        return expirationEnabled;
    }

    public void setExpirationEnabled( boolean expirationEnabled ) {
        this.expirationEnabled = expirationEnabled;
    }

    public Date getExpirationDate() {
        return expirationDate;
    }

    public void setExpirationDate( Date expirationDate ) {
        this.expirationDate = expirationDate;
    }

    /**
	 * Convenience method to locate and parse the mtime attribute into a 
	 * Java Date object.  If the mtime cannot be found or parsed, null will
	 * be returned.
	 */
	public Date getMtime() {
		if(systemMetadata.get("mtime") == null) {
			return null;
		}
		String mtime = systemMetadata.get("mtime").getValue();
		return Iso8601Util.parse(mtime);
	}
	
	public void setMtime(Date mtime) {
		String smtime = Iso8601Util.format(mtime);
		systemMetadata.put("mtime", new Metadata("mtime", smtime, false));
	}
	
	/**
	 * Convenience method to locate and parse the ctime attribute into a 
	 * Java Date object.  If the ctime cannot be found or parsed, null will
	 * be returned.
	 */
	public Date getCtime() {
		if(systemMetadata.get("ctime") == null) {
			return null;
		}
		String ctime = systemMetadata.get("ctime").getValue();
		return Iso8601Util.parse(ctime);
	}
	
	
	public String toJson() {
		JsonObject root = new JsonObject();
		JsonObject metadata = new JsonObject();
		root.add("metadata", metadata);
		JsonObject sysmeta = new JsonObject();
		root.add(SYSTEM_METADATA_PROP, sysmeta);
		JsonObject acl = new JsonObject();
		root.add(ACL_PROP, acl);
		root.addProperty(CONTENT_TYPE_PROP, contentType);
        if (retentionEndDate != null) {
            root.addProperty(RETENTION_ENABLED_PROP, isRetentionEnabled());
            root.addProperty(RETENTION_END_PROP, Iso8601Util.format(retentionEndDate));
        }
		if (expirationDate != null) {
            root.addProperty(EXPIRATION_ENABLED_PROP, isExpirationEnabled());
            root.addProperty(EXPIRATION_PROP, Iso8601Util.format(expirationDate));
        }

		writeMetadata(this.metadata, metadata);
		writeMetadata(this.systemMetadata, sysmeta);
		writeAcl(this.acl, acl);
		
		Gson gs = new Gson();
		return gs.toJson(root);
	}
	
	public void toFile(File metaFile) throws IOException {
		PrintWriter pw = null;
		
		try {
			pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(metaFile), "UTF-8"));
			pw.println(toJson());
		} finally {
			if(pw != null) {
				pw.close();
			}
		}
	}
	
	private void writeAcl(Acl acl, JsonObject jacl) {
		if(acl == null) {
			return;
		}
        JsonArray groups = new JsonArray();
        JsonArray users = new JsonArray();
		for(String name : acl.getGroupAcl().keySet()) {
			JsonObject grant = new JsonObject();
            grant.addProperty(NAME_PROP, name);
			grant.addProperty(PERMISSION_PROP, acl.getGroupAcl().get(name).toString());
			groups.add(grant);
		}
        for(String name : acl.getUserAcl().keySet()) {
            JsonObject grant = new JsonObject();
            grant.addProperty(NAME_PROP, name);
            grant.addProperty(PERMISSION_PROP, acl.getUserAcl().get(name).toString());
            users.add(grant);
        }
        jacl.add(ACL_GROUPS_PROP, groups);
        jacl.add(ACL_USERS_PROP, users);
	}

	private void writeMetadata(Map<String, Metadata> metadata, JsonObject jmetadata) {
		if(metadata == null) {
			return;
		}
		for(Metadata m : metadata.values()) {
			JsonObject jm = new JsonObject();
			jm.addProperty(VALUE_PROP, m.getValue());
			jm.addProperty(LISTABLE_PROP, m.isListable());
			jmetadata.add(m.getName(), jm);	
		}
	}
}
