package com.emc.ecs.sync.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

public interface SqlDateMapper {
    Date getResultDate(ResultSet rs, String name) throws SQLException;

    Object getDateParam(Date date);
}
