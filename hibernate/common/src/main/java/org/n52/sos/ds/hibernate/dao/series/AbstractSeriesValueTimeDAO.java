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

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.n52.sos.ds.hibernate.dao.AbstractValueTimeDAO;
import org.n52.sos.ds.hibernate.dao.ObservationValueFK;
import org.n52.sos.ds.hibernate.entities.series.Series;
import org.n52.sos.ds.hibernate.entities.series.values.SeriesValueTime;
import org.n52.sos.ds.hibernate.util.ObservationTimeExtrema;
import org.n52.sos.ogc.ows.OwsExceptionReport;
import org.n52.sos.request.GetObservationRequest;
import org.n52.sos.util.DateTimeHelper;

/**
 * Abstract value time data access object class for {@link SeriesValueTime}
 * 
 * @author Carsten Hollmann <c.hollmann@52north.org>
 * @since 4.3.0
 *
 */
public abstract class AbstractSeriesValueTimeDAO extends AbstractValueTimeDAO {
    /**
     * Get the concrete {@link SeriesValueTime} class.
     * 
     * @return The concrete {@link SeriesValueTime} class
     */
    protected abstract Class<?> getSeriesValueTimeClass();

    /**
     * Get {@link ObservationTimeExtrema} for a {@link Series} with temporal
     * filter.
     * 
     * @param request
     *            {@link GetObservationRequest} request
     * @param series
     *            {@link Series} to get time extrema for
     * @param temporalFilterCriterion
     *            Temporal filter
     * @param session
     *            Hibernate session
     * @return Time extrema for {@link Series}
     * @throws OwsExceptionReport
     *             If an error occurs
     */
    public ObservationTimeExtrema getTimeExtremaForSeries(GetObservationRequest request, ObservationValueFK valueFK,
            Criterion temporalFilterCriterion, Session session) throws OwsExceptionReport {
    	// TODOHZG: called from the series streaming value, check how often this is called, see performance impact
    	final Criteria criteria = session.createCriteria(valueFK.getForClass())
    		.add(Restrictions.eq("observedPropertyInstance", valueFK.getObservedPropertyInstance()))
    		.setProjection(Projections.projectionList()
    			.add(Projections.min("date"))
    			.add(Projections.max("date")));

    	// TODOHZG: check for offering match here? do spatial and temporal filtering here

    	final Object[] result = (Object[]) criteria.uniqueResult();
    	final ObservationTimeExtrema ote = new ObservationTimeExtrema();

    	if (result == null) {
    		return ote;
    	}

    	ote.setMinPhenTime(DateTimeHelper.makeDateTime(result[0]));
    	ote.setMaxPhenTime(DateTimeHelper.makeDateTime(result[1]));
    	ote.setMaxResultTime(ote.getMaxPhenTime());

    	return ote;
    }

    /**
     * Get default {@link Criteria} for {@link Class}
     * 
     * @param clazz
     *            {@link Class} to get default {@link Criteria} for
     * @param session
     *            Hibernate Session
     * @return Default {@link Criteria}
     */
    public Criteria getDefaultObservationCriteria(Session session) {
        return session.createCriteria(getSeriesValueTimeClass()).add(Restrictions.eq(SeriesValueTime.DELETED, false))
                .setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
    }
}
