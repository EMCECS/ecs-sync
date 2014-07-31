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
package com.emc.atmos.sync.plugins;

import com.emc.atmos.AtmosException;
import com.emc.atmos.api.ObjectIdentifier;
import com.emc.atmos.api.bean.Metadata;
import com.emc.atmos.sync.Timeable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * @author cwikj
 *
 */
public class AtmosDeletePlugin extends SyncPlugin {
	private static final Logger l4j = Logger.getLogger(AtmosDeletePlugin.class);

	public static final String DELETE_OPT = "atmos-delete";
	public static final String DELETE_OPT_DESC = "Enables the AtmosDelete plugin.";

	public static final String DELETE_TAGS_OPT = "delete-listable-tags";
	public static final String DELETE_TAGS_DESC = "if set, any listable tags will be deleted first before deleting the object";

    // timed operations
    private static final String OPERATION_DELETE_OBJECT = "AtmosDeleteObject";
    private static final String OPERATION_GET_USER_META = "AtmosGetUserMeta";
    private static final String OPERATION_DELETE_META = "AtmosDeleteMeta";

	private boolean deleteTags = false;

	private AtmosSource source;

	@Override
	public void filter(SyncObject obj) {
		ObjectIdentifier id;
		SourceAtmosId idAnn = (SourceAtmosId) obj.getAnnotation(SourceAtmosId.class);
		if(idAnn.getId() != null) {
			id = idAnn.getId();
		} else {
			id = idAnn.getPath();
		}
        final ObjectIdentifier fId = id;
        try {
            if (deleteTags) {
                Map<String, Metadata> metaMap = time(new Timeable<Map<String, Metadata>>() {
                    @Override
                    public Map<String, Metadata> call() {
                        return source.getAtmos().getUserMetadata(fId);
                    }
                }, OPERATION_GET_USER_META);
                for (final Metadata m : metaMap.values()) {
                    if (m.isListable()) {
                        l4j.debug("Deleting tag " + m.getName() + " from " + id);
                        try {
                            time(new Timeable<Void>() {
                                @Override
                                public Void call() {
                                    source.getAtmos().deleteUserMetadata(fId, m.getName());
                                    return null;
                                }
                            }, OPERATION_DELETE_META);
                        } catch (AtmosException e) {
                            if (e.getErrorCode() == 1005) {
                                // Already deleted
                                l4j.warn("Tag " + m.getName() + " already deleted (Atmos code 1005)");
                            } else {
                                throw e;
                            }
                        }
                    }
                }
            }
            l4j.debug("Deleting " + id);
            time(new Timeable<Void>() {
                @Override
                public Void call() {
				    source.getAtmos().delete(fId);
                    return null;
                }
            }, OPERATION_DELETE_OBJECT);
        } catch (AtmosException e) {
            if (e.getHttpCode() == 404) {
                // Good (already deleted)
                l4j.debug("Object already deleted");
                getNext().filter(obj);
                return;
            } else {
                throw e;
            }
        }
        getNext().filter(obj);

	}

	@SuppressWarnings("static-access")
	@Override
	public Options getOptions() {
		Options opts = new Options();

		opts.addOption(OptionBuilder.withLongOpt(DELETE_OPT)
				.withDescription(DELETE_OPT_DESC).create());
		opts.addOption(OptionBuilder.withLongOpt(DELETE_TAGS_OPT)
				.withDescription(DELETE_TAGS_DESC).create());

		return opts;
	}

	@Override
	public boolean parseOptions(CommandLine line) {
		if (line.hasOption(DELETE_TAGS_OPT)) {
			deleteTags = true;
		}

		return line.hasOption(DELETE_OPT);
	}

	@Override
	public void validateChain(SyncPlugin first) {
		// Source must be AtmosSource and destination must be DummyDestination
		SyncPlugin source = first;
		SyncPlugin dest = null;
		while (first != null) {
			dest = first;
			first = first.getNext();
		}

		if (!(source instanceof AtmosSource)) {
			throw new IllegalArgumentException(
					"Source must be an AtmosSource to use AtmosDeletePlugin");
		}
		if (!(dest instanceof DummyDestination)) {
			throw new IllegalArgumentException(
					"Destination must be a DummyDestination to use AtmosDeletePlugin");
		}

		this.source = (AtmosSource) source;
	}

	@Override
	public String getName() {
		return "Atmos Delete";
	}

	@Override
	public String getDocumentation() {
		return "This plugin can be combined with an AtmosSource and a " +
				"DummyDestination to delete objects from Atmos.  It can handle " +
				"retries, extended timeouts, and can delete listable tags " +
				"before deleting the object.";
	}


	/**
	 * This app can be used to build a tracking database.  See the sample
	 * spring/atmos-delete.xml for the DDL and queries.
	 * @param args the first argument is the Spring configuration file.  We will
	 * get the DataSource from here.  The second argument is a list of
	 * ObjectIDs.
	 */
	public static void main(String[] args) {
		File springXml = new File(args[0]);
		if(!springXml.exists()) {
			System.err.println("The Spring XML file: " + springXml + " does not exist");
			System.exit(1);
		}

		l4j.info("Loading configuration from Spring XML file: " + springXml);
		FileSystemXmlApplicationContext ctx =
				new FileSystemXmlApplicationContext(args[0]);

		if(!ctx.containsBean("dataSource")) {
			System.err.println("Your Spring XML file: " + springXml +
					" must contain one bean named 'dataSource' that " +
					"initializes a DataSource object");
			System.exit(1);
		}

		try {
			DataSource ds = (DataSource) ctx.getBean("dataSource");
			File idFile = new File(args[1]);
			BufferedReader br = new BufferedReader(new FileReader(idFile));
			String id;
			int count = 0;
			Connection con = ds.getConnection();
			PreparedStatement ps = con.prepareStatement("INSERT INTO object_list(oid) VALUES(?)");
			while((id = br.readLine()) != null) {
				id = id.trim();
				if(id.length()<1) {
					continue;
				}
				ps.setString(1, id);
				try {
					ps.executeUpdate();
				} catch(SQLException e) {
					if(e.getErrorCode() == 1062) {
						// dupe
						continue;
					}
					l4j.warn("SQL Error: " + e.getErrorCode());
					throw e;
				}
				count++;
				if(count % 1000 == 0) {
					System.out.println("Inserted " + count + " records");
				}
			}
			System.out.println("COMPLETE. Inserted " + count + " records");
			System.exit(0);
		} catch(Exception e) {
			e.printStackTrace();
			System.exit(1);
		}


	}

	/**
	 * @return the deleteTags
	 */
	public boolean isDeleteTags() {
		return deleteTags;
	}

	/**
	 * @param deleteTags the deleteTags to set
	 */
	public void setDeleteTags(boolean deleteTags) {
		this.deleteTags = deleteTags;
	}
}
