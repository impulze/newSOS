/**
 * Copyright (C) 2012-2015 52°North Initiative for Geospatial Open Source
 * Software GmbH
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published
 * by the Free Software Foundation.
 *
 * If the program is linked with libraries which are licensed under one of
 * the following licenses, the combination of the program with the linked
 * library is not considered a "derivative work" of the program:
 *
 *     - Apache License, version 2.0
 *     - Apache Software License, version 1.0
 *     - GNU Lesser General Public License, version 3
 *     - Mozilla Public License, versions 1.0, 1.1 and 2.0
 *     - Common Development and Distribution License (CDDL), version 1.0
 *
 * Therefore the distribution of the program linked with libraries licensed
 * under the aforementioned licenses, is permitted by the copyright holders
 * if the distribution is compliant with both the GNU General Public
 * License version 2 and the aforementioned licenses.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 */
package org.n52.sos.ds.hibernate.dao.series;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.hibernate.Session;
import org.n52.sos.ds.hibernate.dao.AbstractObservationDAO;
import org.n52.sos.ds.hibernate.entities.series.Series;
import org.n52.sos.ds.hibernate.util.TimeCriterion;

import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Geometry;

public abstract class AbstractSeriesObservationDAO extends AbstractObservationDAO {
    @Override
    public List<Geometry> getSamplingGeometries(String feature, Session session) {
    	// TODOHZG: return all sampling geometries ordered by phenomenon time start
    	return Lists.newArrayList();
    }
    
    /**
     * Get the result times for this series, offerings and filters
     * addSpecificRestrictions(c, request);
     * @param series
     *            Timeseries to get result times for
     * @param offerings
     *            Offerings to restrict matching result times
     * @param filter
     *            Temporal filter to restrict matching result times
     * @param session
     *            Hibernate session
     * @return Matching result times
     */
    public List<Date> getResultTimesForSeriesObservation(Series series, List<String> offerings, Map<String, Collection<TimeCriterion>> temporalFilterDisjunctions,
            Session session) {
    	// TODOHZG: should this really return ALL observation times?
        return Lists.newArrayList();
    }
}
