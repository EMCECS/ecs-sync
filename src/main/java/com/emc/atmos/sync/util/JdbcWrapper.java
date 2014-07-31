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

import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JdbcWrapper {
    private static final Logger l4j = Logger.getLogger( JdbcWrapper.class );

    private static final String SQL_PARAMETER_PATTERN = ":([a-zA-Z_]+)";

    private DataSource dataSource;
    private boolean hardThreaded = false;
    private ThreadLocal<Connection> threadConnection = new ThreadLocal<Connection>();
    private Queue<Connection> threadConnectionCache = new LinkedBlockingQueue<Connection>();

    public JdbcWrapper() {
    }

    public JdbcWrapper( DataSource dataSource ) {
        this.dataSource = dataSource;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource( DataSource dataSource ) {
        this.dataSource = dataSource;
    }

    public boolean isHardThreaded() {
        return hardThreaded;
    }

    public void setHardThreaded( boolean hardThreaded ) {
        this.hardThreaded = hardThreaded;
    }

    /**
     * Allows the use of parameterized SQL statements (i.e. "select x from y where id = :id")
     */
    public <T> T executeQueryForObject( String sql, Map<String, String> params, Class<T> returnType ) {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            connection = getConnection();
            statement = prepareStatement( connection, sql );
            setParameters( statement, sql, params );
            resultSet = statement.executeQuery();
            if ( !resultSet.next() ) return null;
            return castResult( resultSet, 1, returnType );
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        } finally {
            close( resultSet );
            close( statement );
            close( connection );
        }
    }

    public void executeUpdate( String sql, Map<String, String> params ) {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = getConnection();
            statement = prepareStatement( connection, sql );
            setParameters( statement, sql, params );
            statement.executeUpdate();
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        } finally {
            close( statement );
            close( connection );
        }
    }

    public void closeHardThreadedConnections() {
        for ( Connection connection : threadConnectionCache ) {
            close( connection );
        }
        threadConnectionCache.clear();
        threadConnection = new ThreadLocal<Connection>();
    }

    protected void close( ResultSet resultSet ) {
        if ( resultSet != null ) {
            try {
                resultSet.close();
            } catch ( SQLException e ) {
                // ignore
            }
        }
    }

    protected void close( Statement statement ) {
        if ( statement != null ) {
            try {
                statement.close();
            } catch ( SQLException e ) {
                // ignore
            }
        }
    }

    protected void close( Connection connection ) {
        if ( connection != null ) {
            try {
                connection.close();
            } catch ( SQLException e ) {
                // ignore
            }
        }
    }

    protected Connection getConnection() throws SQLException {
        if ( hardThreaded ) {
            Connection connection = threadConnection.get();
            if ( connection == null ) {
                connection = new NonClosingConnection( dataSource.getConnection() );
                threadConnection.set( connection );
            }
            return connection;
        } else {
            return dataSource.getConnection();
        }
    }

    /**
     * Allows the use of parameterized SQL statements (i.e. "select x from y where id = :id")
     */
    protected PreparedStatement prepareStatement( Connection connection, String sql ) throws SQLException {
        String indexedSql = sql.replaceAll( SQL_PARAMETER_PATTERN, "?" );
        l4j.debug( "Creating new statement for SQL:\nparameterized: " + sql + "\nindexed      : " + indexedSql );
        return connection.prepareStatement( indexedSql );
    }

    protected void setParameters( PreparedStatement statement, String sql, Map<String, String> params )
            throws SQLException {
        l4j.debug( "Preparing statement parameters for SQL:\n" + sql );
        Matcher matcher = Pattern.compile( SQL_PARAMETER_PATTERN ).matcher( sql );
        int paramIndex = 1;
        while ( matcher.find() ) {
            String paramName = matcher.group( 1 );
            l4j.debug( "Setting parameter (index " + paramIndex + "): " + paramName + "=" + params.get( paramName ) );
            statement.setString( paramIndex++, params.get( paramName ) );
        }
    }

    @SuppressWarnings("unchecked")
    protected <T> T castResult( ResultSet resultSet, int columnIndex, Class<T> returnType ) throws SQLException {
        if ( String.class == returnType ) {
            return (T) resultSet.getString( columnIndex );
        } else if ( Integer.class == returnType ) {
            return (T) new Integer( resultSet.getInt( columnIndex ) );
        } else {
            throw new UnsupportedOperationException(
                    "return type " + returnType + " is not supported (maybe you should add it)" );
        }
    }
}
