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
import java.util.List;

import org.hibernate.Session;
import org.n52.sos.ds.hibernate.dao.ObservationValueFK;
import org.n52.sos.ds.hibernate.entities.series.Series;
import org.n52.sos.exception.CodedException;
import org.n52.sos.request.GetObservationRequest;

/**
 * Hibernate data access class for series
 * 
 * @since 4.0.0
 * 
 */
public class SeriesDAO extends AbstractSeriesDAO {
    @Override
    public List<Series> getSeries(GetObservationRequest request, Collection<String> features, Session session) throws CodedException {
    	throw new RuntimeException("SeriesDAO not supported.");
    }

    @Override
    public List<Series> getSeries(Collection<String> procedures, Collection<String> observedProperties,
            Collection<String> features, Session session) {
    	throw new RuntimeException("SeriesDAO not supported.");
    }

    @Override
    public List<Series> getSeries(String observedProperty, Collection<String> features, Session session) {
    	throw new RuntimeException("SeriesDAO not supported.");
    }

    @Override
    public Series getOrInsertSeries(SeriesIdentifiers identifiers, final Session session) throws CodedException {
        return getOrInsert(identifiers, session);
    }

    @Override
    protected Class <?>getSeriesClass() {
        return Series.class;
    }

	@Override
	public <T extends Series> void setValueFK(T series, ObservationValueFK valueFK) {
		throw new RuntimeException("SeriesDAO not supported.");
	}
}
