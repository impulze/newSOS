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
import java.util.Collections;
import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.n52.sos.ds.hibernate.entities.series.Series;
import org.n52.sos.ds.hibernate.entities.series.SeriesObservation;
import org.n52.sos.ds.hibernate.util.TimeExtrema;
import org.n52.sos.exception.CodedException;
import org.n52.sos.request.GetObservationRequest;
import org.n52.sos.service.SosContextListener;
import org.n52.sos.util.DateTimeHelper;

import de.hzg.common.SOSConfiguration;

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
    public List<Series> getSeries(GetObservationRequest request, Collection<String> features, Session session) throws CodedException {
    	return getSeries(request.getProcedures(), request.getObservedProperties(), features, session);
    }
    
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
    public List<Series> getSeries(String observedProperty, Collection<String> features, Session session) {
        return getSeries(Collections.<String>emptyList(), Collections.singletonList(observedProperty), features, session);
    }
    
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
    
    /**
     * Get series for procedure, observableProperty and featureOfInterest
     * 
     * @param procedure
     *            Procedure identifier parameter
     * @param observableProperty
     *            ObservableProperty identifier parameter
     * @param featureOfInterest
     *            FeatureOfInterest identifier parameter
     * @param session
     *            Hibernate session
     * @return Matching series
     */
    public Series getSeriesFor(String procedure, String observableProperty, String featureOfInterest, Session session) {
       	final List<Series> result = getSeries(Collections.singletonList(procedure), Collections.singletonList(observableProperty), Collections.singletonList(featureOfInterest), session);

        if (result == null) {
        	return null;
        }

        return result.get(0);
    }

    /**
     * Update Series for procedure by setting deleted flag and return changed
     * series
     * 
     * @param procedure
     *            Procedure for which the series should be changed
     * @param deleteFlag
     *            New deleted flag value
     * @param session
     *            Hibernate session
     * @return Updated Series
     */
    public List<Series> updateSeriesSetAsDeletedForProcedureAndGetSeries(String procedure, boolean deleteFlag,
            Session session) {
    	throw new RuntimeException("Updating series not supported yet.");
    }
    
	/**
	 * Check {@link Series} if the deleted observation time stamp corresponds to
	 * the first/last series time stamp
	 * 
	 * @param series
	 *            Series to update
	 * @param observation
	 *            Deleted observation
	 * @param session
	 *            Hibernate session
	 */
	public void updateSeriesAfterObservationDeletion(Series series, SeriesObservation observation, Session session) {
		throw new RuntimeException("Updating series not supported yet.");
	}
	
	public TimeExtrema getProcedureTimeExtrema(Session session, String procedure) {
		final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;

	    if (!procedure.startsWith(sosConfiguration.getProcedureIdentifierPrefix())) {
	        return new TimeExtrema();
	    }

	    final Query query = session.createQuery("SELECT MIN(cd.date), MAX(cd.date) FROM CalculatedData cd LEFT JOIN cd.observedPropertyInstance AS opi LEFT JOIN opi.sensor AS s WHERE cd.observedPropertyInstance = opi AND opi.sensor = s AND s.name = :name")
            .setParameter("name", procedure.substring(sosConfiguration.getProcedureIdentifierPrefix().length()));
	    final Object[] result = (Object[])query.uniqueResult();
        final TimeExtrema pte = new TimeExtrema();

        if (result != null) {
            pte.setMinTime(DateTimeHelper.makeDateTime(result[0]));
            pte.setMaxTime(DateTimeHelper.makeDateTime(result[1]));
        }

        return pte;
    }
}
