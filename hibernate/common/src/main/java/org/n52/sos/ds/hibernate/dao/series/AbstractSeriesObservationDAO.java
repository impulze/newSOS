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

import static org.hibernate.criterion.Restrictions.eq;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.n52.sos.ds.hibernate.dao.AbstractObservationDAO;
import org.n52.sos.ds.hibernate.entities.AbstractObservationTime;
import org.n52.sos.ds.hibernate.entities.FeatureOfInterest;
import org.n52.sos.ds.hibernate.entities.ObservableProperty;
import org.n52.sos.ds.hibernate.entities.Offering;
import org.n52.sos.ds.hibernate.entities.Procedure;
import org.n52.sos.ds.hibernate.entities.series.Series;
import org.n52.sos.ds.hibernate.entities.series.SeriesObservation;
import org.n52.sos.ds.hibernate.entities.series.SeriesObservationTime;
import org.n52.sos.ds.hibernate.util.HibernateHelper;
import org.n52.sos.ds.hibernate.util.TimeCriterion;
import org.n52.sos.exception.CodedException;
import org.n52.sos.ogc.ows.OwsExceptionReport;
import org.n52.sos.ogc.sos.SosConstants.SosIndeterminateTime;
import org.n52.sos.request.GetObservationRequest;
import org.n52.sos.util.CollectionHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;

public abstract class AbstractSeriesObservationDAO extends AbstractObservationDAO {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSeriesObservationDAO.class);

    @SuppressWarnings("unchecked")
    @Override
    public List<Geometry> getSamplingGeometries(String feature, Session session) {
        Criteria criteria = getDefaultObservationTimeCriteria(session).createAlias(SeriesObservation.SERIES, "s");
        criteria.createCriteria("s." + Series.FEATURE_OF_INTEREST).add(eq(FeatureOfInterest.IDENTIFIER, feature));
        criteria.add(Restrictions.isNotNull(AbstractObservationTime.SAMPLING_GEOMETRY));
        criteria.addOrder(Order.asc(AbstractObservationTime.PHENOMENON_TIME_START));
        criteria.setProjection(Projections.property(AbstractObservationTime.SAMPLING_GEOMETRY));
        return criteria.list();
    }

    /**
     * Create series observation query criteria for series
     * 
     * @param Class
     *            to query
     * @param series
     *            Series to get values for
     * @param session
     *            Hibernate session
     * @return Criteria to query series observations
     */
    protected Criteria createCriteriaFor(Class<?> clazz, Series series, Session session) {
        final Criteria criteria = getDefaultObservationCriteria(session);
        criteria.createCriteria(SeriesObservation.SERIES).add(Restrictions.eq(Series.ID, series.getSeriesId()));
        return criteria;
    }
    
    /**
     * Get the result times for this series, offerings and filters
     * 
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
    @SuppressWarnings("unchecked")
    public List<Date> getResultTimesForSeriesObservation(Series series, List<String> offerings, Map<String, Collection<TimeCriterion>> temporalFilterDisjunctions,
            Session session) {
        Criteria criteria = createCriteriaFor(getObservationTimeClass(), series, session);
        if (CollectionHelper.isNotEmpty(offerings)) {
            criteria.createCriteria(SeriesObservationTime.OFFERINGS).add(
                    Restrictions.in(Offering.IDENTIFIER, offerings));
        }
        // TODOHZG: filter series observations by temporal filters
        criteria.setProjection(Projections.distinct(Projections.property(SeriesObservationTime.RESULT_TIME)));
        criteria.addOrder(Order.asc(SeriesObservationTime.RESULT_TIME));
        LOGGER.debug("QUERY getResultTimesForSeriesObservation({}): {}", HibernateHelper.getSqlString(criteria));
        return criteria.list();
    }
    
    /**
     * Create series observations {@link Criteria} for GetObservation request, features, and filter criterion (typically a temporal filter) or
     * an indeterminate time (first/latest). This method is private and accepts all possible arguments for request-based
     * getSeriesObservationFor. Other public methods overload this method with sensible combinations of arguments.
     * 
     * @param request
     *              GetObservation request
     * @param features
     *              Collection of feature identifiers resolved from the request 
     * @param filterCriterion
     *              Criterion to apply to criteria query (typically a temporal filter)
     * @param sosIndeterminateTime
     *              Indeterminate time to use in a temporal filter (first/latest)
     * @param session
     * @return Series observations {@link Criteria}
     * @throws OwsExceptionReport 
     */
    protected Criteria getSeriesObservationCriteriaFor(GetObservationRequest request, Collection<String> features,
    		Map<String, Collection<TimeCriterion>> temporalFilterDisjunctions, SosIndeterminateTime sosIndeterminateTime, Session session) throws OwsExceptionReport {
            
            final Criteria c = getDefaultObservationCriteria(session);
            String seriesAliasPrefix = createSeriesAliasAndRestrictions(c);
            checkAndAddSpatialFilteringProfileCriterion(c, request, session);
            addSpecificRestrictions(c, request);
            
            if (CollectionHelper.isNotEmpty(request.getProcedures())) {
                c.createCriteria(seriesAliasPrefix + Series.PROCEDURE).add(Restrictions.in(Procedure.IDENTIFIER, request.getProcedures()));
            }
            
            if (CollectionHelper.isNotEmpty(request.getObservedProperties())) {
                c.createCriteria(seriesAliasPrefix + Series.OBSERVABLE_PROPERTY).add(Restrictions.in(ObservableProperty.IDENTIFIER,
                        request.getObservedProperties()));
            }
            
            if (CollectionHelper.isNotEmpty(features)) {
                c.createCriteria(seriesAliasPrefix + Series.FEATURE_OF_INTEREST).add(Restrictions.in(FeatureOfInterest.IDENTIFIER, features));
            }
            
            if (CollectionHelper.isNotEmpty(request.getOfferings())) {
                c.createCriteria(SeriesObservation.OFFERINGS).add(Restrictions.in(Offering.IDENTIFIER, request.getOfferings()));
            }
            
            String logArgs = "request, features, offerings";
            // TODOHZG: filter series by temporal filters
            if (temporalFilterDisjunctions != null) {
                logArgs += ", filterCriterion";
                //c.add(filterCriterion);
            }
            if (sosIndeterminateTime != null) {
                logArgs += ", sosIndeterminateTime";
                addIndeterminateTimeRestriction(c, sosIndeterminateTime);
            }
            LOGGER.debug("QUERY getSeriesObservationFor({}): {}", logArgs, HibernateHelper.getSqlString(c));
            return c;
    }
    
    
    private String createSeriesAliasAndRestrictions(Criteria c) {
        String alias = "s";
        String aliasWithDot = alias + ".";
        c.createAlias(SeriesObservation.SERIES, alias);
        c.add(Restrictions.eq(aliasWithDot + Series.DELETED, false));
        c.add(Restrictions.eq(aliasWithDot + Series.PUBLISHED, true));
        return aliasWithDot;
    }
    
    /**
     * Query series observations for GetObservation request and features
     * 
     * @param request
     *            GetObservation request
     * @param features
     *            Collection of feature identifiers resolved from the request
     * @param session
     *            Hibernate session
     * @return Series observations that fit
     * @throws OwsExceptionReport
     */
    public abstract List<SeriesObservation> getSeriesObservationsFor(GetObservationRequest request, Collection<String> features, Session session) throws OwsExceptionReport;
    
    /**
     * Query series observations for GetObservation request, features, and a
     * filter criterion (typically a temporal filter)
     * 
     * @param request
     *            GetObservation request
     * @param features
     *            Collection of feature identifiers resolved from the request
     * @param filterCriterion
     *            Criterion to apply to criteria query (typically a temporal
     *            filter)
     * @param session
     *            Hibernate session
     * @return Series observations that fit
     * @throws OwsExceptionReport
     */
    public abstract List<SeriesObservation> getSeriesObservationsFor(GetObservationRequest request, Collection<String> features, Map<String, Collection<TimeCriterion>> temporalFilterDisjunctions, Session session) throws OwsExceptionReport;
    
    /**
     * Query series observations for GetObservation request, features, and an
     * indeterminate time (first/latest)
     * 
     * @param request
     *            GetObservation request
     * @param features
     *            Collection of feature identifiers resolved from the request
     * @param sosIndeterminateTime
     *            Indeterminate time to use in a temporal filter (first/latest)
     * @param session
     *            Hibernate session
     * @return Series observations that fit
     * @throws OwsExceptionReport
     */
    public abstract List<SeriesObservation> getSeriesObservationsFor(GetObservationRequest request, Collection<String> features, SosIndeterminateTime sosIndeterminateTime, Session session) throws OwsExceptionReport;
    
    /**
     * Query series observations for GetObservation request, features, and
     * filter criterion (typically a temporal filter) or an indeterminate time
     * (first/latest). This method is private and accepts all possible arguments
     * for request-based getSeriesObservationFor. Other public methods overload
     * this method with sensible combinations of arguments.
     * 
     * @param request
     *            GetObservation request
     * @param features
     *            Collection of feature identifiers resolved from the request
     * @param filterCriterion
     *            Criterion to apply to criteria query (typically a temporal
     *            filter)
     * @param sosIndeterminateTime
     *            Indeterminate time to use in a temporal filter (first/latest)
     * @param session
     * @return Series observations that fit
     * @throws OwsExceptionReport
     */
    protected abstract List<SeriesObservation> getSeriesObservationsFor(GetObservationRequest request, Collection<String> features, Map<String, Collection<TimeCriterion>> temporalFilterDisjunctions, SosIndeterminateTime sosIndeterminateTime, Session session) throws OwsExceptionReport;
    
    public abstract List<SeriesObservation> getSeriesObservationsFor(Series series, GetObservationRequest request, SosIndeterminateTime sosIndeterminateTime, Session session) throws OwsExceptionReport;
    
    protected abstract void addSpecificRestrictions(Criteria c, GetObservationRequest request) throws CodedException;
    
    protected Criteria getSeriesObservationCriteriaFor(Series series, GetObservationRequest request,
            SosIndeterminateTime sosIndeterminateTime, Session session) throws OwsExceptionReport {
        final Criteria c =
                getDefaultObservationCriteria(session).add(
                        Restrictions.eq(SeriesObservation.SERIES, series));
        checkAndAddSpatialFilteringProfileCriterion(c, request, session);

        if (request.isSetOffering()) {
            c.createCriteria(SeriesObservation.OFFERINGS).add(
                    Restrictions.in(Offering.IDENTIFIER, request.getOfferings()));
        }
        String logArgs = "request, features, offerings";
        logArgs += ", sosIndeterminateTime";
        addIndeterminateTimeRestriction(c, sosIndeterminateTime);
        LOGGER.debug("QUERY getSeriesObservationFor({}): {}", logArgs, HibernateHelper.getSqlString(c));
        return c;
        
    }
}
