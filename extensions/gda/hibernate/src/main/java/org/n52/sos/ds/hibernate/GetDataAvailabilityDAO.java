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
package org.n52.sos.ds.hibernate;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.Criteria;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernate.transform.ResultTransformer;
import org.n52.sos.ds.FeatureQueryHandlerQueryObject;
import org.n52.sos.ds.HibernateDatasourceConstants;
import org.n52.sos.ds.hibernate.dao.AbstractObservationDAO;
import org.n52.sos.ds.hibernate.dao.DaoFactory;
import org.n52.sos.ds.hibernate.dao.HibernateSqlQueryConstants;
import org.n52.sos.ds.hibernate.dao.series.AbstractSeriesObservationDAO;
import org.n52.sos.ds.hibernate.dao.series.SeriesObservationTimeDAO;
import org.n52.sos.ds.hibernate.entities.AbstractObservation;
import org.n52.sos.ds.hibernate.entities.Offering;
import org.n52.sos.ds.hibernate.entities.series.Series;
import org.n52.sos.ds.hibernate.entities.series.SeriesObservationInfo;
import org.n52.sos.ds.hibernate.entities.series.SeriesObservationTime;
import org.n52.sos.ds.hibernate.util.HibernateHelper;
import org.n52.sos.ds.hibernate.util.TemporalRestrictions;
import org.n52.sos.exception.ows.NoApplicableCodeException;
import org.n52.sos.gda.AbstractGetDataAvailabilityDAO;
import org.n52.sos.gda.GetDataAvailabilityRequest;
import org.n52.sos.gda.GetDataAvailabilityResponse;
import org.n52.sos.gda.GetDataAvailabilityResponse.DataAvailability;
import org.n52.sos.ogc.filter.TemporalFilter;
import org.n52.sos.ogc.gml.AbstractFeature;
import org.n52.sos.ogc.gml.ReferenceType;
import org.n52.sos.ogc.gml.time.TimeInstant;
import org.n52.sos.ogc.gml.time.TimePeriod;
import org.n52.sos.ogc.ows.OwsExceptionReport;
import org.n52.sos.ogc.sos.Sos2Constants;
import org.n52.sos.ogc.sos.SosConstants;
import org.n52.sos.ogc.swes.SwesExtension;
import org.n52.sos.ogc.swes.SwesExtensions;
import org.n52.sos.service.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * {@code IGetDataAvailabilityDao} to handle {@link GetDataAvailabilityRequest}
 * s.
 * 
 * @author Christian Autermann
 * @since 4.0.0
 */
public class GetDataAvailabilityDAO extends AbstractGetDataAvailabilityDAO implements HibernateSqlQueryConstants {

    private static final Logger LOGGER = LoggerFactory.getLogger(GetDataAvailabilityDAO.class);

    private HibernateSessionHolder sessionHolder = new HibernateSessionHolder();

    public GetDataAvailabilityDAO() {
        super(SosConstants.SOS);
    }

    @Override
    public GetDataAvailabilityResponse getDataAvailability(GetDataAvailabilityRequest req) throws OwsExceptionReport {
        Session session = sessionHolder.getSession();
        try {
            List<?> dataAvailabilityValues = queryDataAvailabilityValues(req, session);
            GetDataAvailabilityResponse response = new GetDataAvailabilityResponse();
            response.setService(req.getService());
            response.setVersion(req.getVersion());
            response.setNamespace(req.getNamespace());
            for (Object o : dataAvailabilityValues) {
                if (o != null) {
                    response.addDataAvailability((DataAvailability) o);
                }
            }
            return response;
        } catch (HibernateException he) {
            throw new NoApplicableCodeException().causedBy(he).withMessage(
                    "Error while querying data for GetDataAvailability!");
        } finally {
            sessionHolder.returnSession(session);
        }
    }

    /**
     * Query data availability information depending on supported functionality
     * 
     * @param req
     *            GetDataAvailability request
     * @param session
     *            Hibernate session
     * @return Data availability information
     * @throws OwsExceptionReport
     *             If an error occurs
     */
    private List<?> queryDataAvailabilityValues(GetDataAvailabilityRequest req, Session session)
            throws OwsExceptionReport {
        // check if series mapping is supporte
            return querySeriesDataAvailabilities(req, session);
    }

    /**
     * GetDataAvailability processing is series mapping is supported.
     * 
     * @param request
     *            GetDataAvailability request
     * @param session
     *            Hibernate session
     * @return List of valid data availability information
     * @throws OwsExceptionReport
     *             If an error occurs
     */
    private List<?> querySeriesDataAvailabilities(GetDataAvailabilityRequest request, Session session)
            throws OwsExceptionReport {
        List<DataAvailability> dataAvailabilityValues = Lists.newLinkedList();
        Map<String, ReferenceType> procedures = new HashMap<String, ReferenceType>();
        Map<String, ReferenceType> observableProperties = new HashMap<String, ReferenceType>();
        Map<String, ReferenceType> featuresOfInterest = new HashMap<String, ReferenceType>();
        AbstractSeriesObservationDAO seriesObservationDAO = getSeriesObservationDAO();
        SeriesMinMaxTransformer seriesMinMaxTransformer = new SeriesMinMaxTransformer();
        for (final Series series : DaoFactory
                .getInstance()
                .getSeriesDAO()
                .getSeries(request.getProcedures(), request.getObservedProperties(), request.getFeaturesOfInterest(), session)) {
            TimePeriod timePeriod = null;
            if (!request.isSetOfferings()) {
                // get time information from series object
                if (series.isSetFirstLastTime()) {
                    timePeriod = new TimePeriod(series.getFirstTimeStamp(), series.getLastTimeStamp());
                }
            }
            // get time information from SeriesGetDataAvailability mapping if
            // supported
            if (timePeriod == null) {
                SeriesObservationTimeDAO seriesObservationTimeDAO =
                        (SeriesObservationTimeDAO) DaoFactory.getInstance().getObservationTimeDAO();
                timePeriod =
                        getTimePeriodFromSeriesGetDataAvailability(seriesObservationTimeDAO, series, request,
                                seriesMinMaxTransformer, session);
            }
            // create DataAvailabilities
            if (timePeriod != null && !timePeriod.isEmpty()) {
                DataAvailability dataAvailability =
                        new DataAvailability(getProcedureReference(series, procedures), getObservedPropertyReference(
                                series, observableProperties), getFeatureOfInterestReference(series,
                                featuresOfInterest, session), timePeriod);
                if (isShowCount(request)) {
                    dataAvailability.setCount(getCountFor(series, request, session));
                }
                if (isIncludeResultTime(request)) {
                    dataAvailability.setResultTimes(getResultTimesFromSeriesObservation(seriesObservationDAO, series,
                            request, session));
                }
                dataAvailabilityValues.add(dataAvailability);
            }

        }
        return dataAvailabilityValues;
    }

    /**
     * Get time information from SeriesGetDataAvailability mapping
     * 
     * @param seriesGetDataAvailabilityDAO
     *            Series GetDataAvailability DAO class
     * @param series
     *            Series to get information for
     * @param request
     * @param seriesMinMaxTransformer
     *            Hibernate result transformator for min/max time value
     * @param session
     *            Hibernate Session
     * @return Time period
     */
    private TimePeriod getTimePeriodFromSeriesGetDataAvailability(
            SeriesObservationTimeDAO seriesGetDataAvailabilityDAO, Series series, GetDataAvailabilityRequest request,
            SeriesMinMaxTransformer seriesMinMaxTransformer, Session session) {
        Criteria criteria =
                seriesGetDataAvailabilityDAO.getMinMaxTimeCriteriaForSeriesGetDataAvailabilityDAO(series,
                        request.getOfferings(), session);
        criteria.setResultTransformer(seriesMinMaxTransformer);
        LOGGER.debug("QUERY getTimePeriodFromSeriesObservation(series): {}", HibernateHelper.getSqlString(criteria));
        return (TimePeriod) criteria.uniqueResult();
    }

    /**
     * Get the result times for the timeseries
     * 
     * @param seriesObservationDAO
     *            DAO
     * @param series
     *            time series
     * @param request
     *            GetDataAvailability request
     * @param session
     *            Hibernate session
     * @return List of result times
     * @throws OwsExceptionReport
     *             if the requested temporal filter is not supported
     */
    private List<TimeInstant> getResultTimesFromSeriesObservation(AbstractSeriesObservationDAO seriesObservationDAO,
            Series series, GetDataAvailabilityRequest request, Session session) throws OwsExceptionReport {
        Criterion filter = null;
        if (hasPhenomenonTimeFilter(request.getExtensions())) {
            filter = TemporalRestrictions.filter(getPhenomenonTimeFilter(request.getExtensions()));
        }
        List<Date> dateTimes =
                seriesObservationDAO.getResultTimesForSeriesObservation(series, request.getOfferings(), filter,
                        session);
        List<TimeInstant> resultTimes = Lists.newArrayList();
        for (Date date : dateTimes) {
            resultTimes.add(new TimeInstant(date));
        }
        return resultTimes;
    }

    /**
     * Get count of available observation for this time series
     * 
     * @param series
     *            Time series
     * @param request
     *            GetDataAvailability request
     * @param session
     *            Hibernate session
     * @return Count of available observations
     */
    private Long getCountFor(Series series, GetDataAvailabilityRequest request, Session session) {
        Criteria criteria =
                session.createCriteria(SeriesObservationInfo.class).add(
                        Restrictions.eq(AbstractObservation.DELETED, false));
        criteria.add(Restrictions.eq(SeriesObservationInfo.SERIES, series));
        if (request.isSetOfferings()) {
            criteria.createCriteria(SeriesObservationTime.OFFERINGS).add(
                    Restrictions.in(Offering.IDENTIFIER, request.getOfferings()));
        }
        criteria.setProjection(Projections.rowCount());
        return (Long) criteria.uniqueResult();
    }

    private ReferenceType getProcedureReference(Series series, Map<String, ReferenceType> procedures) {
        String identifier = series.getProcedure().getIdentifier();
        if (!procedures.containsKey(identifier)) {
            ReferenceType referenceType = new ReferenceType(identifier);
            // TODO
            // SosProcedureDescription sosProcedureDescription = new
            // HibernateProcedureConverter().createSosProcedureDescription(procedure,
            // procedure.getProcedureDescriptionFormat().getProcedureDescriptionFormat(),
            // Sos2Constants.SERVICEVERSION, session);
            // if ()
            procedures.put(identifier, referenceType);
        }
        return procedures.get(identifier);
    }

    private ReferenceType getObservedPropertyReference(Series series, Map<String, ReferenceType> observableProperties) {
        String identifier = series.getObservableProperty().getIdentifier();
        if (!observableProperties.containsKey(identifier)) {
            ReferenceType referenceType = new ReferenceType(identifier);
            // TODO
            // if (observableProperty.isSetDescription()) {
            // referenceType.setTitle(observableProperty.getDescription());
            // }
            observableProperties.put(identifier, referenceType);
        }
        return observableProperties.get(identifier);
    }

    private ReferenceType getFeatureOfInterestReference(Series series, Map<String, ReferenceType> featuresOfInterest,
            Session session) throws OwsExceptionReport {
        String identifier = series.getFeatureOfInterest().getIdentifier();
        if (!featuresOfInterest.containsKey(identifier)) {
            ReferenceType referenceType = new ReferenceType(identifier);
            FeatureQueryHandlerQueryObject queryObject = new FeatureQueryHandlerQueryObject();
            queryObject.addFeatureIdentifier(identifier).setConnection(session)
                    .setVersion(Sos2Constants.SERVICEVERSION);
            AbstractFeature feature = Configurator.getInstance().getFeatureQueryHandler().getFeatureByID(queryObject);
            if (feature.isSetName() && feature.getFirstName().isSetValue()) {
                referenceType.setTitle(feature.getFirstName().getValue());
            }
            featuresOfInterest.put(identifier, referenceType);
        }
        return featuresOfInterest.get(identifier);
    }

    /**
     * Check if optional count should be added
     * 
     * @param request
     *            GetDataAvailability request
     * @return <code>true</code>, if optional count should be added
     */
    private boolean isShowCount(GetDataAvailabilityRequest request) {
        if (request.isSetExtensions()) {
            return request.getExtensions().isBooleanExtensionSet(SHOW_COUNT);
        }
        return isForceValueCount();
    }

    /**
     * Check if result times should be added
     * 
     * @param request
     *            GetDataAvailability request
     * @return <code>true</code>, if result times should be added
     */
    private boolean isIncludeResultTime(GetDataAvailabilityRequest request) {
        if (request.isSetExtensions()) {
            return request.getExtensions().isBooleanExtensionSet(INCLUDE_RESULT_TIMES)
                    || hasPhenomenonTimeFilter(request.getExtensions());
        }
        return false;
    }

    /**
     * Check if extensions contains a temporal filter with valueReference
     * phenomenonTime
     * 
     * @param extensions
     *            Extensions to check
     * @return <code>true</code>, if extensions contains a temporal filter with
     *         valueReference phenomenonTime
     */
    private boolean hasPhenomenonTimeFilter(SwesExtensions extensions) {
        boolean hasFilter = false;
        for (SwesExtension<?> extension : extensions.getExtensions()) {
            if (extension.getValue() instanceof TemporalFilter) {
                TemporalFilter filter = (TemporalFilter) extension.getValue();
                if (TemporalRestrictions.PHENOMENON_TIME_VALUE_REFERENCE.equals(filter.getValueReference())) {
                    hasFilter = true;
                }
            }
        }
        return hasFilter;
    }

    /**
     * Get the temporal filter with valueReference phenomenonTime from
     * extensions
     * 
     * @param extensions
     *            To get filter from
     * @return Temporal filter with valueReference phenomenonTime
     */
    private TemporalFilter getPhenomenonTimeFilter(SwesExtensions extensions) {
        for (SwesExtension<?> extension : extensions.getExtensions()) {
            if (extension.getValue() instanceof TemporalFilter) {
                TemporalFilter filter = (TemporalFilter) extension.getValue();
                if (TemporalRestrictions.PHENOMENON_TIME_VALUE_REFERENCE.equals(filter.getValueReference())) {
                    return filter;
                }
            }
        }
        return null;
    }

    protected AbstractSeriesObservationDAO getSeriesObservationDAO() throws OwsExceptionReport {
        AbstractObservationDAO observationDAO = DaoFactory.getInstance().getObservationDAO();
        if (observationDAO instanceof AbstractSeriesObservationDAO) {
            return (AbstractSeriesObservationDAO) observationDAO;
        } else {
            throw new NoApplicableCodeException().withMessage("The required '%s' implementation is no supported!",
                    AbstractObservationDAO.class.getName());
        }
    }

    private static class SeriesMinMaxTransformer implements ResultTransformer {
        private static final long serialVersionUID = -373512929481519459L;

        @Override
        public TimePeriod transformTuple(Object[] tuple, String[] aliases) {
            if (tuple != null) {
                return new TimePeriod(tuple[0], tuple[1]);
            }
            return null;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public List transformList(List collection) {
            return collection;
        }
    }

    @Override
    public String getDatasourceDaoIdentifier() {
        return HibernateDatasourceConstants.ORM_DATASOURCE_DAO_IDENTIFIER;
    }
}
