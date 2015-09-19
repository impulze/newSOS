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
import java.util.Map;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.ProjectionList;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.n52.sos.ds.hibernate.dao.AbstractValueTimeDAO;
import org.n52.sos.ds.hibernate.dao.ObservationValueFK;
import org.n52.sos.ds.hibernate.entities.series.Series;
import org.n52.sos.ds.hibernate.entities.series.values.SeriesValueTime;
import org.n52.sos.ds.hibernate.util.ObservationTimeExtrema;
import org.n52.sos.ds.hibernate.util.TimeCriterion;
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
    		Map<String, Collection<TimeCriterion>> temporalFilterDisjunctions, Session session) throws OwsExceptionReport {
    	// TODOHZG: see how often this is called and if it causes performance problems
    	final Criteria criteria = session.createCriteria(valueFK.getValueClass())
    			.add(Restrictions.eq("observedPropertyInstance", valueFK.getObservedPropertyInstance()));
        //checkAndAddSpatialFilteringProfileCriterion(c, request, session);

    	// TODOHZG: is offerings of request relevant here if we filter by observed property instance?

        //addIndeterminateTimeRestriction(c, sosIndeterminateTime);

        addMinMaxTimeProjection(criteria);
        return parseMinMaxTime((Object[])criteria.uniqueResult());
    }

    private void addMinMaxTimeProjection(Criteria c) {
        ProjectionList projectionList = Projections.projectionList();
        projectionList.add(Projections.min("date"));
        projectionList.add(Projections.max("date"));
        c.setProjection(projectionList);
    }

    private ObservationTimeExtrema parseMinMaxTime(Object[] result) {
        ObservationTimeExtrema ote = new ObservationTimeExtrema();
        if (result != null) {
            ote.setMinPhenTime(DateTimeHelper.makeDateTime(result[0]));
            ote.setMaxPhenTime(DateTimeHelper.makeDateTime(result[1]));
            ote.setMaxResultTime(DateTimeHelper.makeDateTime(result[1]));
        }
        return ote;
    }
}
