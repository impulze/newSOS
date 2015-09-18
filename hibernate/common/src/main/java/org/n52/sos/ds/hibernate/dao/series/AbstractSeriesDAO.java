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

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.joda.time.DateTime;
import org.n52.sos.ds.hibernate.entities.series.Series;
import org.n52.sos.ds.hibernate.util.TimeExtrema;
import org.n52.sos.exception.CodedException;
import org.n52.sos.request.GetObservationRequest;
import org.n52.sos.service.SosContextListener;
import org.n52.sos.util.DateTimeHelper;

import de.hzg.common.SOSConfiguration;
import de.hzg.values.CalculatedData;
import de.hzg.values.RawData;

public abstract class AbstractSeriesDAO {
    protected abstract Class<?> getSeriesClass();
    
    /**
     * Get series for GetObservation request and featuresOfInterest
     * 
     * @param request
     *            GetObservation request to get series for
     * @param features
     *            FeaturesOfInterest to get series for
     * @param session
     *            Hibernate session
     * @return Series that fit
     * @throws CodedException 
     */
    public abstract List<Series> getSeries(GetObservationRequest request, Collection<String> features, Session session) throws CodedException;
    
    /**
     * Query series for observedProiperty and featuresOfInterest
     * 
     * @param observedProperty
     *            ObservedProperty to get series for
     * @param features
     *            FeaturesOfInterest to get series for
     * @param session
     *            Hibernate session
     * @return Series list
     */
    public abstract List<Series> getSeries(String observedProperty, Collection<String> features, Session session);
    
    /**
     * Create series for parameter
     * 
     * @param procedures
     *            Procedures to get series for
     * @param observedProperties
     *            ObservedProperties to get series for
     * @param features
     *            FeaturesOfInterest to get series for
     * @param session
     *            Hibernate session
     * @return Series that fir
     */
    public abstract List<Series> getSeries(Collection<String> procedures, Collection<String> observedProperties,
            Collection<String> features, Session session);
	
	private Criteria createTimeExtremaCriteria(String name, Class<?> clazz, Session session) {
		final Criteria criteria = session.createCriteria(clazz)
				.createAlias("observedPropertyInstance", "ob")
				.createAlias("ob.sensor", "s")
				.add(Restrictions.eq("s.name", name))
				.setProjection(Projections.projectionList()
						.add(Projections.min("date"))
						.add(Projections.max("date")));

		return criteria;
	}

	public TimeExtrema getProcedureTimeExtrema(Session session, String procedure) {
		final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;
		final String prefix = sosConfiguration.getProcedureIdentifierPrefix();
		final String name = procedure.substring(prefix.length());
		final TimeExtrema pte = new TimeExtrema();

		Object[] result = (Object[])createTimeExtremaCriteria(name, CalculatedData.class, session).uniqueResult();

		if (result != null) {
			pte.setMinTime(DateTimeHelper.makeDateTime(result[0]));
			pte.setMaxTime(DateTimeHelper.makeDateTime(result[1]));
		}

		result = (Object[])createTimeExtremaCriteria(name, RawData.class, session).uniqueResult();

		if (result != null) {
			final DateTime rawMin = DateTimeHelper.makeDateTime(result[0]);
			final DateTime rawMax = DateTimeHelper.makeDateTime(result[1]);

			if (pte.getMinTime() == null || rawMin.isBefore(pte.getMinTime())) {
				pte.setMinTime(rawMin);
			}

			if (pte.getMaxTime() == null || rawMax.isAfter(pte.getMaxTime())) {
				pte.setMaxTime(rawMin);
			}
		}

		return pte;
	}
}
