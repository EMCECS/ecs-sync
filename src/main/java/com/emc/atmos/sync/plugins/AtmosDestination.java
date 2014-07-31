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
import com.emc.atmos.api.*;
import com.emc.atmos.api.bean.Metadata;
import com.emc.atmos.api.bean.ObjectMetadata;
import com.emc.atmos.api.bean.ServiceInformation;
import com.emc.atmos.api.jersey.AtmosApiClient;
import com.emc.atmos.api.request.CreateObjectRequest;
import com.emc.atmos.api.request.UpdateObjectRequest;
import com.emc.atmos.sync.Timeable;
import com.emc.atmos.sync.util.AtmosUtil;
import com.emc.atmos.sync.util.Iso8601Util;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Stores objects into an Atmos system.
 * @author cwikj
 */
public class AtmosDestination extends DestinationPlugin implements InitializingBean {
	/**
	 * This pattern is used to activate this plugin.
	 */
	public static final String URI_PATTERN = "^(http|https)://([a-zA-Z0-9/\\-@_\\.]+):([a-zA-Z0-9\\+/=]+)@([^/]*?)(:[0-9]+)?(?:/)?$";

	public static final String DEST_NAMESPACE_OPTION = "dest-namespace";
	public static final String DEST_NAMESPACE_DESC = "The destination within the Atmos namespace.  Note that a directory must end with a trailing slash (e.g. /dir1/dir2/) otherwise it will be interpreted as a single file (only useful for transferring a single file).";
	public static final String DEST_NAMESPACE_ARG_NAME = "atmos-path";

	public static final String DEST_NO_UPDATE_OPTION = "no-update";
	public static final String DEST_NO_UPDATE_DESC = "If specified, no updates will be applied to the destination";

	public static final String DEST_CHECKSUM_OPT = "atmos-dest-checksum";
	public static final String DEST_CHECKSUM_DESC = "If specified, the atmos wschecksum feature will be applied to uploads.  Valid algorithms are SHA0 for Atmos < 2.1 and SHA0, SHA1, or MD5 for 2.1+";
	public static final String DEST_CHECKSUM_ARG_NAME = "checksum-alg";

    // timed operations
    private static final String OPERATION_SET_USER_META = "AtmosSetUserMeta";
    private static final String OPERATION_SET_ACL = "AtmosSetAcl";
    private static final String OPERATION_CREATE_OBJECT = "AtmosCreateObject";
    private static final String OPERATION_CREATE_OBJECT_ON_PATH = "AtmosCreateObjectOnPath";
    private static final String OPERATION_CREATE_OBJECT_FROM_SEGMENT = "AtmosCreateObjectFromSegment";
    private static final String OPERATION_CREATE_OBJECT_FROM_SEGMENT_ON_PATH = "AtmosCreateObjectFromSegmentOnPath";
    private static final String OPERATION_UPDATE_OBJECT_FROM_SEGMENT = "AtmosUpdateObjectFromSegment";
    private static final String OPERATION_CREATE_OBJECT_FROM_STREAM = "AtmosCreateObjectFromStream";
    private static final String OPERATION_CREATE_OBJECT_FROM_STREAM_ON_PATH = "AtmosCreateObjectFromStreamOnPath";
    private static final String OPERATION_DELETE_OBJECT = "AtmosDeleteObject";
    private static final String OPERATION_SET_RETENTION_EXPIRATION = "AtmosSetRetentionExpiration";
    private static final String OPERATION_GET_ALL_META = "AtmosGetAllMeta";
    private static final String OPERATION_GET_SYSTEM_META = "AtmosGetSystemMeta";
    private static final String OPERATION_TOTAL = "TotalTime";

	private static final Logger l4j = Logger.getLogger(AtmosDestination.class);

	private String destNamespace;
	private List<String> hosts;
	private String protocol;
	private int port;
	private String uid;
	private String secret;
	private AtmosApi atmos;
	private boolean force;
	private boolean noUpdate;
    private boolean includeRetentionExpiration;
    private long retentionDelayWindow = 1; // 1 second by default
    private String checksum;

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#filter(com.emc.atmos.sync.plugins.SyncObject)
	 */
	@Override
	public void filter(final SyncObject obj) {
        // skip the root namespace since it obviously exists
        if ("/".equals(destNamespace + obj.getRelativePath())) {
            l4j.debug("Destination namespace is root");
            return;
        }

        timeOperationStart(OPERATION_TOTAL);
		try {
            // some sync objects lazy-load their metadata (i.e. AtmosSyncObject)
            // since this may be a timed operation, ensure it loads outside of other timed operations
            final Map<String, Metadata> umeta = obj.getMetadata().getMetadata();

			if(destNamespace != null) {
				// Determine a name for the object.
				ObjectPath destPath;
				if(!destNamespace.endsWith("/")) {
					// A specific file was mentioned.
					destPath = new ObjectPath(destNamespace);
				} else {
					destPath = new ObjectPath(destNamespace + obj.getRelativePath());
				}
                final ObjectPath fDestPath = destPath;

				obj.setDestURI(new URI(protocol, uid + ":" + secret,
						hosts.get(0), port, destPath.toString(), null, null));

				// See if the destination exists
				if(destPath.isDirectory()) {
					Map<String, Metadata> smeta = getSystemMetadata(destPath);

					if(smeta != null && obj.getMetadata().getSystemMetadata() != null) {
						// See if a metadata update is required
						Date srcCtime = obj.getMetadata().getCtime();
						Date dstCtime = parseDate(smeta.get("ctime"));

						if((srcCtime != null && dstCtime != null && srcCtime.after(dstCtime)) || force) {
							if(umeta != null && umeta.size()>0) {
								LogMF.debug(l4j, "Updating metadata on {0}", destPath);
								time(new Timeable<Void>() {
                                    @Override
                                    public Void call() {
                                        atmos.setUserMetadata(fDestPath, umeta.values().toArray(new Metadata[umeta.size()]));
                                        return null;
                                    }
                                }, OPERATION_SET_USER_META);
							}
							if(obj.getMetadata().getAcl() != null) {
								LogMF.debug( l4j, "Updating ACL on {0}", destPath );
                                time(new Timeable<Void>() {
                                    @Override
                                    public Void call() {
                                        atmos.setAcl( fDestPath, obj.getMetadata().getAcl() );
                                        return null;
                                    }
                                }, OPERATION_SET_ACL);
							}
						} else {
							LogMF.debug(l4j, "No changes from source {0} to dest {1}",
									obj.getSourceURI(),
									obj.getDestURI());
							return;
						}
					} else {
						// Directory does not exist on destination
						ObjectId id = time(new Timeable<ObjectId>() {
                            @Override
                            public ObjectId call() {
                                return atmos.createDirectory(fDestPath, obj.getMetadata().getAcl(),
                                                             umeta.values().toArray(new Metadata[umeta.size()]));
                            }
                        }, OPERATION_CREATE_OBJECT_ON_PATH);
						DestinationAtmosId destId = new DestinationAtmosId();
						destId.setId(id);
						destId.setPath(destPath);
						obj.addAnnotation(destId);
					}

				} else {
					// File, not directory
					ObjectMetadata destMeta = getMetadata(destPath);
					if(destMeta == null) {
						// Destination does not exist.
						InputStream in = null;
						try {
							in = obj.getInputStream();
							ObjectId id = null;
							if(in == null) {
								// Create an empty object
                                final CreateObjectRequest request = new CreateObjectRequest();
                                request.identifier(destPath).acl(obj.getMetadata().getAcl());
                                request.setUserMetadata(umeta.values());
                                request.contentType(obj.getMetadata().getContentType());
								id = time(new Timeable<ObjectId>() {
                                    @Override
                                    public ObjectId call() {
                                        return atmos.createObject(request).getObjectId();
                                    }
                                }, OPERATION_CREATE_OBJECT_ON_PATH);
							} else {
								if(checksum != null) {
									final RunningChecksum ck = new RunningChecksum(ChecksumAlgorithm.valueOf(checksum));
									byte[] buffer = new byte[1024*1024];
									long read = 0;
									int c;
									while((c = in.read(buffer)) != -1) {
										final BufferSegment bs = new BufferSegment(buffer, 0, c);
										if(read == 0) {
											// Create
                                            ck.update(bs.getBuffer(), bs.getOffset(), bs.getSize());
                                            final CreateObjectRequest request = new CreateObjectRequest();
                                            request.identifier(destPath).acl( obj.getMetadata().getAcl() ).content(bs);
                                            request.setUserMetadata(umeta.values());
                                            request.contentType(obj.getMetadata().getContentType()).wsChecksum(ck);
											id = time(new Timeable<ObjectId>() {
                                                @Override
                                                public ObjectId call() {
                                                    return atmos.createObject(request).getObjectId();
                                                }
                                            }, OPERATION_CREATE_OBJECT_FROM_SEGMENT_ON_PATH);
										} else {
											// Append
                                            ck.update(bs.getBuffer(), bs.getOffset(), bs.getSize());
                                            Range r = new Range(read, read + c - 1);
                                            final UpdateObjectRequest request = new UpdateObjectRequest();
                                            request.identifier(id).acl( obj.getMetadata().getAcl() ).content(bs).range(r);
                                            request.setUserMetadata(umeta.values());
                                            request.contentType(obj.getMetadata().getContentType()).wsChecksum(ck);
											time(new Timeable<Object>() {
                                                @Override
                                                public Object call() {
                                                    atmos.updateObject(request);
                                                    return null;
                                                }
                                            }, OPERATION_UPDATE_OBJECT_FROM_SEGMENT);
										}
										read += c;
									}
								} else {
                                    final CreateObjectRequest request = new CreateObjectRequest();
                                    request.identifier(destPath).acl(obj.getMetadata().getAcl()).content(in);
                                    request.setUserMetadata(umeta.values());
                                    request.contentLength(obj.getSize()).contentType(obj.getMetadata().getContentType());
									id = time(new Timeable<ObjectId>() {
                                        @Override
                                        public ObjectId call() {
                                            return atmos.createObject(request).getObjectId();
                                        }
                                    }, OPERATION_CREATE_OBJECT_FROM_STREAM_ON_PATH);
								}
							}

                            updateRetentionExpiration(obj, id);

							DestinationAtmosId destId = new DestinationAtmosId();
							destId.setId(id);
							destId.setPath(destPath);
							obj.addAnnotation(destId);
						} finally {
							if(in != null) {
								in.close();
							}
						}

					} else {
						checkUpdate(obj, destPath, destMeta);
					}
				}
			} else {
				// Object Space

                // don't create directories when in objectspace (likely a filesystem source)
                // TODO: is this a valid use-case (should we create these objects)?
                if (obj.isDirectory()) {
                    LogMF.debug(l4j, "Source {0} is a directory, but destination is in objectspace, ignoring",
                                obj.getSourceURI());
                    return;
                }

				InputStream in = null;
				try {
					ObjectId id = null;
					// Check and see if a destination ID was alredy computed
					DestinationAtmosId ann = obj.getAnnotation(DestinationAtmosId.class);
					if(ann != null) {
						id = ann.getId();
					}

					if(id != null) {
						ObjectMetadata destMeta = getMetadata(id);
						if(destMeta == null) {
							// Destination ID not found!
							throw new RuntimeException("The destination object ID " + id + " was not found!");
						}
						obj.setDestURI(new URI(protocol, uid + ":"+ secret,
								hosts.get(0), port, "/"+id.toString(), null, null));
						checkUpdate(obj, id, destMeta);
					} else {
						in = obj.getInputStream();
						if(in == null) {
							// Usually some sort of directory
                            final CreateObjectRequest request = new CreateObjectRequest();
                            request.acl(obj.getMetadata().getAcl()).contentType( obj.getMetadata().getContentType() );
                            request.setUserMetadata(umeta.values());
                            id = time(new Timeable<ObjectId>() {
                                @Override
                                public ObjectId call() {
                                    return atmos.createObject(request).getObjectId();
                                }
                            }, OPERATION_CREATE_OBJECT);
						} else {
							if(checksum != null) {
								final RunningChecksum ck = new RunningChecksum(ChecksumAlgorithm.valueOf( checksum ));
								byte[] buffer = new byte[1024*1024];
								long read = 0;
								int c;
								while((c = in.read(buffer)) != -1) {
									final BufferSegment bs = new BufferSegment(buffer, 0, c);
									if(read == 0) {
										// Create
                                        ck.update(bs.getBuffer(), bs.getOffset(), bs.getSize());
                                        final CreateObjectRequest request = new CreateObjectRequest();
                                        request.acl( obj.getMetadata().getAcl() ).content(bs);
                                        request.setUserMetadata(umeta.values());
                                        request.contentType(obj.getMetadata().getContentType()).wsChecksum(ck);
										id = time(new Timeable<ObjectId>() {
                                            @Override
                                            public ObjectId call() {
                                                return atmos.createObject(request).getObjectId();
                                            }
                                        }, OPERATION_CREATE_OBJECT_FROM_SEGMENT);
									} else {
										// Append
                                        ck.update(bs.getBuffer(), bs.getOffset(), bs.getSize());
                                        Range r = new Range(read, read + c - 1);
                                        final UpdateObjectRequest request = new UpdateObjectRequest();
                                        request.identifier(id).acl( obj.getMetadata().getAcl() ).content(bs).range(r);
                                        request.setUserMetadata(umeta.values());
                                        request.contentType(obj.getMetadata().getContentType()).wsChecksum(ck);
										time(new Timeable<Void>() {
                                            @Override
                                            public Void call() {
                                                atmos.updateObject( request );
                                                return null;
                                            }
                                        }, OPERATION_UPDATE_OBJECT_FROM_SEGMENT);
									}
									read += c;
								}
							} else {
                                final CreateObjectRequest request = new CreateObjectRequest();
                                request.acl( obj.getMetadata().getAcl() ).content(in);
                                request.setUserMetadata(umeta.values());
                                request.contentLength(obj.getSize()).contentType( obj.getMetadata().getContentType() );
								id = time(new Timeable<ObjectId>() {
                                    @Override
                                    public ObjectId call() {
                                        return atmos.createObject(request).getObjectId();
                                    }
                                }, OPERATION_CREATE_OBJECT_FROM_STREAM);
							}
						}

                        updateRetentionExpiration(obj, id);

                        obj.setDestURI(new URI(protocol, uid + ":"+ secret,
								hosts.get(0), port, "/"+id.toString(), null, null));
						DestinationAtmosId destId = new DestinationAtmosId();
						destId.setId(id);
						obj.addAnnotation(destId);
					}
				} finally {
					try {
						if(in != null) {
							in.close();
						}
					} catch (IOException e) {
						// Ignore
					}
				}

			}
			LogMF.debug(l4j, "Wrote source {0} to dest {1}",
					obj.getSourceURI(),
					obj.getDestURI());

            timeOperationComplete(OPERATION_TOTAL);
		} catch(Exception e) {
            timeOperationFailed(OPERATION_TOTAL);
			throw new RuntimeException(
					"Failed to store object: " + e.getMessage(), e);
		}
	}


	/**
	 * If the destination exists, we perform some checks and update only what
	 * needs to be updated (metadata and/or content)
	 */
	private void checkUpdate(final SyncObject obj, final ObjectIdentifier destId, ObjectMetadata destMeta) throws IOException {
		// Exists.  Check timestamps
		Date srcMtime = obj.getMetadata().getMtime();
		Date dstMtime = parseDate(destMeta.getMetadata().get( "mtime" ));
		Date srcCtime = obj.getMetadata().getMtime();
		Date dstCtime = parseDate(destMeta.getMetadata().get( "ctime" ));
		if((srcMtime != null && dstMtime != null && srcMtime.after(dstMtime)) || force) {
			if(noUpdate) {
				LogMF.debug(l4j, "Skipping {0}, updates disabled.",
						obj.getSourceURI(),
						obj.getDestURI());
				return;
			}
			// Update the object
			InputStream in = null;
			try {
				in = obj.getInputStream();
				if(in == null) {
					// Metadata only
                    final Map<String, Metadata> metaMap = obj.getMetadata().getMetadata();
					if(metaMap != null && metaMap.size()>0) {
						LogMF.debug(l4j, "Updating metadata on {0}", destId);
						time(new Timeable<Void>() {
                            @Override
                            public Void call() {
                                atmos.setUserMetadata(destId, metaMap.values().toArray(new Metadata[metaMap.size()]));
                                return null;
                            }
                        }, OPERATION_SET_USER_META);
					}
					if(obj.getMetadata().getAcl() != null) {
						LogMF.debug(l4j, "Updating ACL on {0}", destId);
                        time(new Timeable<Void>() {
                            @Override
                            public Void call() {
						        atmos.setAcl(destId, obj.getMetadata().getAcl());
                                return null;
                            }
                        }, OPERATION_SET_ACL);
                    }
				} else {
					LogMF.debug(l4j, "Updating {0}", destId);
					if(checksum != null) {
						try {
							final RunningChecksum ck = new RunningChecksum(ChecksumAlgorithm.valueOf( checksum ));
							byte[] buffer = new byte[1024*1024];
							long read = 0;
							int c;
							while((c = in.read(buffer)) != -1) {
								final BufferSegment bs = new BufferSegment(buffer, 0, c);
								if(read == 0) {
									// You cannot update a checksummed object.
									// Delete and replace.
									if(destId instanceof ObjectId ) {
										throw new RuntimeException(
												"Cannot update checksummed " +
												"object by ObjectID, only " +
												"namespace objects are " +
												"supported");
									}
                                    time(new Timeable<Void>() {
                                        @Override
                                        public Void call() {
									        atmos.delete( destId );
                                            return null;
                                        }
                                    }, OPERATION_DELETE_OBJECT);
                                    ck.update(bs.getBuffer(), bs.getOffset(), bs.getSize());
                                    final CreateObjectRequest request = new CreateObjectRequest();
                                    request.identifier(destId).acl(obj.getMetadata().getAcl()).content(bs);
                                    request.setUserMetadata(obj.getMetadata().getMetadata().values());
                                    request.contentType(obj.getMetadata().getContentType()).wsChecksum(ck);
                                    time(new Timeable<Void>() {
                                        @Override
                                        public Void call() {
									        atmos.createObject(request);
                                            return null;
                                        }
                                    }, OPERATION_CREATE_OBJECT_FROM_SEGMENT_ON_PATH);
								} else {
									// Append
                                    ck.update(bs.getBuffer(), bs.getOffset(), bs.getSize());
									Range r = new Range(read, read + c - 1);
                                    final UpdateObjectRequest request = new UpdateObjectRequest();
                                    request.identifier(destId).acl(obj.getMetadata().getAcl()).content(bs).range(r);
                                    request.setUserMetadata(obj.getMetadata().getMetadata().values());
                                    request.contentType(obj.getMetadata().getContentType()).wsChecksum(ck);
                                    time(new Timeable<Void>() {
                                        @Override
                                        public Void call() {
    								        atmos.updateObject( request );
                                            return null;
                                        }
                                    }, OPERATION_UPDATE_OBJECT_FROM_SEGMENT);
								}
								read += c;
							}
						} catch (NoSuchAlgorithmException e) {
							throw new RuntimeException(
									"Incorrect checksum method: " + checksum,
									e);
						}
					} else {
                        final UpdateObjectRequest request = new UpdateObjectRequest();
                        request.identifier(destId).acl(obj.getMetadata().getAcl()).content(in);
                        request.setUserMetadata(obj.getMetadata().getMetadata().values());
                        request.contentLength(obj.getSize()).contentType(obj.getMetadata().getContentType());
                        time( new Timeable<Void>() {
                            @Override
                            public Void call() {
                                atmos.updateObject( request );
                                return null;
                            }
                        }, OPERATION_UPDATE_OBJECT_FROM_SEGMENT );
					}
				}

                // update retention/expiration in case policy changed
                updateRetentionExpiration(obj, destId);
			} finally {
				if(in != null) {
					in.close();
				}
			}

		} else if(srcCtime != null && dstCtime != null && srcCtime.after(dstCtime)) {
			if(noUpdate) {
				LogMF.debug(l4j, "Skipping {0}, updates disabled.",
						obj.getSourceURI(),
						obj.getDestURI());
				return;
			}
			// Metadata update required.
            final Map<String, Metadata> metaMap = obj.getMetadata().getMetadata();
			if(metaMap != null && metaMap.size()>0) {
				LogMF.debug(l4j, "Updating metadata on {0}", destId);
                time(new Timeable<Void>() {
                    @Override
                    public Void call() {
				        atmos.setUserMetadata(destId, metaMap.values().toArray(new Metadata[metaMap.size()]));
                        return null;
                    }
                }, OPERATION_SET_USER_META);
			}
			if(obj.getMetadata().getAcl() != null) {
				LogMF.debug(l4j, "Updating ACL on {0}", destId);
                time(new Timeable<Void>() {
                    @Override
                    public Void call() {
				        atmos.setAcl(destId, obj.getMetadata().getAcl());
                        return null;
                    }
                }, OPERATION_SET_ACL);
			}

            // update retention/expiration in case policy changed
            updateRetentionExpiration(obj, destId);
		} else {
			// No updates
			LogMF.debug(l4j, "No changes from source {0} to dest {1}",
					obj.getSourceURI(),
					obj.getDestURI());
		}
	}

    private void updateRetentionExpiration(final SyncObject obj, final ObjectIdentifier destId) {
        if (includeRetentionExpiration) {
            try {
                final List<Metadata> retExpList = AtmosUtil.getExpirationMetadataForUpdate( obj.getMetadata() );
                retExpList.addAll( AtmosUtil.getRetentionMetadataForUpdate( obj.getMetadata() ) );
                if (retExpList.size() > 0) {
                    time(new Timeable<Void>() {
                        @Override
                        public Void call() {
                            atmos.setUserMetadata( destId, retExpList.toArray( new Metadata[retExpList.size()] ) );
                            return null;
                        }
                    }, OPERATION_SET_RETENTION_EXPIRATION);
                }
            } catch (AtmosException e) {
                LogMF.error(l4j, "Failed to manually set retention/expiration\n" +
                                 "(destId: {0}, retentionEnd: {1}, expiration: {2})\n" +
                                 "[http: {3}, atmos: {4}, msg: {5}]", new Object[]{
                        destId, Iso8601Util.format(obj.getMetadata().getRetentionEndDate()),
                        Iso8601Util.format(obj.getMetadata().getExpirationDate()),
                        e.getHttpCode(), e.getErrorCode(), e.getMessage()});
            } catch (RuntimeException e) {
                LogMF.error(l4j, "Failed to manually set retention/expiration\n" +
                                 "(destId: {0}, retentionEnd: {1}, expiration: {2})\n[error: {3}]", new Object[]{
                        destId, Iso8601Util.format(obj.getMetadata().getRetentionEndDate()),
                        Iso8601Util.format(obj.getMetadata().getExpirationDate()), e.getMessage()});
            }
        }
    }

	/**
	 * Gets the metadata for an object.  IFF the object does not exist, null
	 * is returned.  If any other error condition exists, the exception is
	 * thrown.
	 * @param destId The object to get metadata for.
	 * @return the object's metadata or null.
	 */
	private ObjectMetadata getMetadata(final ObjectIdentifier destId) {
		try {
			return time(new Timeable<ObjectMetadata>() {
                @Override
                public ObjectMetadata call() {
                    return atmos.getObjectMetadata( destId );
                }
            }, OPERATION_GET_ALL_META);
		} catch(AtmosException e) {
			if(e.getHttpCode() == 404) {
				// Object not found
				return null;
			} else {
				// Some other error, rethrow it
				throw e;
			}
		}
	}

	/**
	 * Tries to parse an ISO-8601 date out of a metadata value.  If the value
	 * is null or the parse fails, null is returned.
	 * @param m the metadata value
	 * @return the Date or null if a date could not be parsed from the value.
	 */
	private Date parseDate(Metadata m) {
		if(m == null || m.getValue() == null) {
			return null;
		}
		return Iso8601Util.parse( m.getValue() );
	}

	/**
	 * Get system metadata.  IFF the object doesn't exist, return null.  On any
	 * other error (e.g. permission denied), throw exception.
	 */
	private Map<String, Metadata> getSystemMetadata(final ObjectPath destPath) {
		try {
			return time(new Timeable<Map<String, Metadata>>() {
                @Override
                public Map<String, Metadata> call() {
                    return atmos.getSystemMetadata(destPath);
                }
            }, OPERATION_GET_SYSTEM_META);
		} catch(AtmosException e) {
			if(e.getErrorCode() == 1003) {
				// Object not found --OK
				return null;
			} else {
				throw new RuntimeException(
						"Error checking for object existance: " +
								e.getMessage(), e);
			}
		}
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getOptions()
	 */
	@SuppressWarnings("static-access")
	@Override
	public Options getOptions() {
		Options opts = new Options();

		opts.addOption(OptionBuilder.withDescription(DEST_NAMESPACE_DESC)
				.withLongOpt(DEST_NAMESPACE_OPTION).hasArg()
				.withArgName(DEST_NAMESPACE_ARG_NAME).create());

		opts.addOption(OptionBuilder.withDescription(DEST_NO_UPDATE_DESC)
				.withLongOpt(DEST_NO_UPDATE_OPTION).create());

		opts.addOption(OptionBuilder.withLongOpt(DEST_CHECKSUM_OPT)
				.withDescription(DEST_CHECKSUM_DESC)
				.hasArg().withArgName(DEST_CHECKSUM_ARG_NAME).create());

		return opts;
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#parseOptions(org.apache.commons.cli.CommandLine)
	 */
	@Override
	public boolean parseOptions(CommandLine line) {
		if(line.hasOption(CommonOptions.DESTINATION_OPTION)) {
			Pattern p = Pattern.compile(URI_PATTERN);
			String source = line.getOptionValue(CommonOptions.DESTINATION_OPTION);
			Matcher m = p.matcher(source);
			if(!m.matches()) {
				LogMF.debug(l4j, "{0} does not match {1}", source, p);
				return false;
			}
			protocol = m.group(1);
			uid = m.group(2);
			secret = m.group(3);
			String sHost = m.group(4);
			String sPort = null;
			if(m.groupCount() == 5) {
				sPort = m.group(5);
			}
			hosts = Arrays.asList(sHost.split(","));
			if(sPort != null) {
				port = Integer.parseInt(sPort.substring(1));
			} else {
				if("https".equals(protocol)) {
					port = 443;
				} else {
					port = 80;
				}
			}

			if(line.hasOption(CommonOptions.FORCE_OPTION)) {
				setForce(true);
			}

			if(line.hasOption(DEST_NAMESPACE_OPTION)) {
				destNamespace = line.getOptionValue(DEST_NAMESPACE_OPTION);
			}

			if(line.hasOption(DEST_NO_UPDATE_OPTION)) {
				noUpdate = true;
				l4j.info("Overwrite/update destination objects disabled");
			}

			if(line.hasOption(DEST_CHECKSUM_OPT)) {
				checksum = line.getOptionValue(DEST_CHECKSUM_OPT);
			} else {
				checksum = null;
			}

            includeRetentionExpiration = line.hasOption(CommonOptions.INCLUDE_RETENTION_EXPIRATION_OPTION);

            if (line.hasOption(CommonOptions.RETENTION_DELAY_WINDOW_OPTION)) {
                retentionDelayWindow = Long.parseLong(line.getOptionValue(CommonOptions.RETENTION_DELAY_WINDOW_OPTION));
                l4j.info("Retention start delay window set to " + retentionDelayWindow);
            }

            // Create and verify Atmos connection
			afterPropertiesSet();

			return true;
		}

		return false;
	}

	/**
	 * Initialize the Atmos connection object and validate the credentials.
	 */
	public void afterPropertiesSet() {
		atmos = new AtmosApiClient(new AtmosConfig(uid, secret, getEndpoints()));

		ServiceInformation info = atmos.getServiceInformation();
		LogMF.info(l4j, "Connected to Atmos {0} on {1}", info.getAtmosVersion(),
				hosts);

		// Use unicode if available
		if(!info.hasFeature(ServiceInformation.Feature.Utf8)) {
			l4j.warn("The destination Atmos server does not support unicode " +
					"metadata. You may encounter errors if metadata " +
					"contains extended characters");
		}
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#validateChain(com.emc.atmos.sync.plugins.SyncPlugin)
	 */
	@Override
	public void validateChain(SyncPlugin first) {
		// No known incompatible plugins
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getName()
	 */
	@Override
	public String getName() {
		return "Atmos Destination";
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getDocumentation()
	 */
	@Override
	public String getDocumentation() {
		return "The Atmos destination plugin is triggered by the destination pattern:\n" +
				"http://uid:secret@host[:port]  or\n" +
				"https://uid:secret@host[:port]\n" +
				"Note that the uid should be the 'full token ID' including the " +
				"subtenant ID and the uid concatenated by a slash\n" +
				"If you want to software load balance across multiple hosts, " +
				"you can provide a comma-delimited list of hostnames or IPs " +
				"in the host part of the URI.\n" +
				"By default, objects will be written to Atmos using the " +
				"object API unless --dest-namespace is specified.\n" +
				"When --dest-namespace is used, the --force flag may be used " +
				"to overwrite destination objects even if they exist.";
	}

	public String getDestNamespace() {
		return destNamespace;
	}

	public void setDestNamespace(String destNamespace) {
		this.destNamespace = destNamespace;
	}

    public boolean isIncludeRetentionExpiration() {
        return includeRetentionExpiration;
    }

    public void setIncludeRetentionExpiration(boolean includeRetentionExpiration) {
        this.includeRetentionExpiration = includeRetentionExpiration;
    }

    public List<String> getHosts() {
		return hosts;
	}

	public void setHosts(List<String> hosts) {
		this.hosts = hosts;
	}

	public String getProtocol() {
		return protocol;
	}

	public void setProtocol(String protocol) {
		this.protocol = protocol;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

    public long getRetentionDelayWindow() {
        return retentionDelayWindow;
    }

    public void setRetentionDelayWindow(long retentionDelayWindow) {
        this.retentionDelayWindow = retentionDelayWindow;
    }

    public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public String getSecret() {
		return secret;
	}

	public void setSecret(String secret) {
		this.secret = secret;
	}

	/**
	 * @return the atmos
	 */
	public AtmosApi getAtmos() {
		return atmos;
	}

	/**
	 * @param atmos the atmos to set
	 */
	public void setAtmos(AtmosApi atmos) {
		this.atmos = atmos;
	}

	/**
	 * @return the force
	 */
	public boolean isForce() {
		return force;
	}

	/**
	 * @param force the force to set
	 */
	public void setForce(boolean force) {
		this.force = force;
	}

    protected URI[] getEndpoints() {
        try {
            List<URI> uris = new ArrayList<URI>();
            for (String host : hosts) {
                uris.add(new URI(protocol, null, host, port, null, null, null));
            }
            return uris.toArray(new URI[hosts.size()]);
        } catch (URISyntaxException e) {
            throw new RuntimeException("Unable to create endpoints", e);
        }
    }
}
