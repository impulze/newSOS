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
package org.n52.sos.ds.hibernate.dao;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.ProjectionList;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.n52.sos.ds.hibernate.dao.ereporting.EReportingSeriesDAO;
import org.n52.sos.ds.hibernate.dao.series.AbstractSeriesObservationDAO;
import org.n52.sos.ds.hibernate.dao.series.SeriesObservationDAO;
import org.n52.sos.ds.hibernate.entities.AbstractObservation;
import org.n52.sos.ds.hibernate.entities.ObservationInfo;
import org.n52.sos.ds.hibernate.entities.Procedure;
import org.n52.sos.ds.hibernate.entities.ProcedureDescriptionFormat;
import org.n52.sos.ds.hibernate.entities.TProcedure;
import org.n52.sos.ds.hibernate.entities.ValidProcedureTime;
import org.n52.sos.ds.hibernate.entities.ereporting.EReportingSeries;
import org.n52.sos.ds.hibernate.entities.series.Series;
import org.n52.sos.ds.hibernate.entities.series.SeriesObservationInfo;
import org.n52.sos.ds.hibernate.util.HibernateHelper;
import org.n52.sos.ds.hibernate.util.QueryHelper;
import org.n52.sos.ds.hibernate.util.TimeExtrema;
import org.n52.sos.exception.CodedException;
import org.n52.sos.exception.ows.concrete.UnsupportedOperatorException;
import org.n52.sos.exception.ows.concrete.UnsupportedTimeException;
import org.n52.sos.exception.ows.concrete.UnsupportedValueReferenceException;
import org.n52.sos.ogc.gml.time.Time;
import org.n52.sos.ogc.ows.OwsExceptionReport;
import org.n52.sos.service.SosContextListener;
import org.n52.sos.util.CollectionHelper;
import org.n52.sos.util.DateTimeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import de.hzg.common.SOSConfiguration;
import de.hzg.measurement.Sensor;

/**
 * Hibernate data access class for procedure
 *
 * @author CarstenHollmann
 * @since 4.0.0
 */
public class ProcedureDAO extends AbstractIdentifierNameDescriptionDAO implements HibernateSqlQueryConstants {
    //public class ProcedureDAO extends TimeCreator implements HibernateSqlQueryConstants {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcedureDAO.class);

    public TProcedure createProcedureWithSensor(Sensor sensor, Session session) {
    	final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;
    	final TProcedure procedure = new TProcedure();

    	// TODOHZG: set description and format
    	procedure.setDeleted(false);
    	procedure.setIdentifier(sosConfiguration.getProcedureIdentifierPrefix() + sensor.getName());

    	return procedure;
    }

    /**
     * Get all procedure objects
     *
     * @param session
     *            Hibernate session
     * @return Procedure objects
     */
    public List<Procedure> getProcedureObjects(final Session session) {
    	// TODOHZG: create procedures
    	return Lists.newArrayList();
    }

    /**
     * Get map keyed by undeleted procedure identifiers with
     * collections of parent procedures (if supported) as values
     * @param session
     * @return Map keyed by procedure identifier with values of parent procedure identifier collections
     */
    public Map<String,Collection<String>> getProcedureIdentifiers(final Session session) {
    	final List<Procedure> procedures = getProcedureObjects(session);
        final Map<String, Collection<String>> map = Maps.newHashMap();

        for (final Procedure procedure: procedures) {
        	map.put(procedure.getIdentifier(), null);

        	if (procedure instanceof TProcedure) {
        		for (final Procedure parent: ((TProcedure)procedure).getParents()) {
        			CollectionHelper.addToCollectionMap(procedure.getIdentifier(), parent.getIdentifier(), map);
        		}
        	}
        }

        return map;
    }

    /**
     * Get Procedure object for procedure identifier
     *
     * @param identifier
     *            Procedure identifier
     * @param session
     *            Hibernate session
     * @return Procedure object
     */
    public Procedure getProcedureForIdentifier(final String identifier, final Session session) {
        Criteria criteria = getDefaultCriteria(session).add(Restrictions.eq(Procedure.IDENTIFIER, identifier));
        LOGGER.debug("QUERY getProcedureForIdentifier(identifier): {}", HibernateHelper.getSqlString(criteria));
        Procedure procedure = (Procedure)criteria.uniqueResult();
            criteria.createCriteria(TProcedure.VALID_PROCEDURE_TIME).add(Restrictions.isNull(ValidProcedureTime.END_TIME));
            LOGGER.debug("QUERY getProcedureForIdentifier(identifier): {}", HibernateHelper.getSqlString(criteria));
            Procedure proc = (Procedure)criteria.uniqueResult();
            if (proc != null) {
                return proc;
            }
        return procedure;

    }
//
//    private Procedure getProcedureWithLatestValidProcedureDescription(String identifier, Session session) {
//        Criteria criteria = getDefaultCriteria(session);
//        criteria.add(Restrictions.eq(Procedure.IDENTIFIER, identifier));
//        criteria.createCriteria(TProcedure.VALID_PROCEDURE_TIME).add(Restrictions.isNull(ValidProcedureTime.END_TIME));
//        LOGGER.debug("QUERY getProcedureForIdentifier(identifier): {}", HibernateHelper.getSqlString(criteria));
//        return (Procedure) criteria.uniqueResult();
//    }

    /**
     * Get Procedure object for procedure identifier
     *
     * @param identifier
     *            Procedure identifier
     * @param session
     *            Hibernate session
     * @return Procedure object
     */
    public Procedure getProcedureForIdentifier(final String identifier, Time time, final Session session) {
        Criteria criteria = getDefaultCriteria(session).add(Restrictions.eq(Procedure.IDENTIFIER, identifier));
        LOGGER.debug("QUERY getProcedureForIdentifier(identifier): {}", HibernateHelper.getSqlString(criteria));
        return (Procedure) criteria.uniqueResult();
    }

    /**
     * Get Procedure objects for procedure identifiers
     *
     * @param identifiers
     *            Procedure identifiers
     * @param session
     *            Hibernate session
     * @return Procedure objects
     */
    @SuppressWarnings("unchecked")
    public List<Procedure> getProceduresForIdentifiers(final Collection<String> identifiers, final Session session) {
        if (identifiers == null || identifiers.isEmpty()) {
            return Collections.EMPTY_LIST;
        }
        Criteria criteria = getDefaultCriteria(session).add(Restrictions.in(Procedure.IDENTIFIER, identifiers));
        LOGGER.debug("QUERY getProceduresForIdentifiers(identifiers): {}", HibernateHelper.getSqlString(criteria));
        return criteria.list();
    }

    /**
     * Get procedure identifiers for all FOIs
     *
     * @param session
     *            Hibernate session
     *
     * @return Map of foi identifier to procedure identifier collection
     * @throws HibernateException 
     * @throws CodedException
     */
    public Map<String,Collection<String>> getProceduresForAllFeaturesOfInterest(final Session session) {
        List<Object[]> results = getFeatureProcedureResult(session);
        Map<String,Collection<String>> foiProcMap = Maps.newHashMap();
        if (CollectionHelper.isNotEmpty(results)) {
            for (Object[] result : results) {
                String foi = (String) result[0];
                String proc = (String) result[1];
                Collection<String> foiProcs = foiProcMap.get(foi);
                if (foiProcs == null) {
                    foiProcs = Lists.newArrayList();
                    foiProcMap.put(foi, foiProcs);
                }
                foiProcs.add(proc);
            }
        }
        return foiProcMap;
    }
    
    private List<Object[]> getFeatureProcedureResult(Session session) {
    	// TODOHZG: for all series get FeatureOfInterest and Procedure
    	final List<EReportingSeries> allSeries = new EReportingSeriesDAO().getAllSeries(session);
    	final List<Object[]> foisAndProcedures = Lists.newArrayList();

    	for (final EReportingSeries series: allSeries) {
    		final Object[] pair = new Object[] { series.getFeatureOfInterest().getIdentifier(), series.getProcedure().getIdentifier() };

    		foisAndProcedures.add(pair);
    	}

    	return foisAndProcedures;
    }

    private Criteria getDefaultCriteria(Session session) {
        return session.createCriteria(Procedure.class).add(Restrictions.eq(Procedure.DELETED, false))
                .setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
    }

    private Criteria getDefaultTProcedureCriteria(Session session) {
        return session.createCriteria(TProcedure.class).add(Restrictions.eq(Procedure.DELETED, false))
                .setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
    }

    /**
     * Get procedure for identifier, possible procedureDescriptionFormats and
     * valid time
     *
     * @param identifier
     *            Identifier of the procedure
     * @param possibleProcedureDescriptionFormats
     *            Possible procedureDescriptionFormats
     * @param validTime
     *            Valid time of the procedure
     * @param session
     *            Hibernate Session
     * @return Procedure entity that match the parameters
     * @throws UnsupportedTimeException
     *             If the time is not supported
     * @throws UnsupportedValueReferenceException
     *             If the valueReference is not supported
     * @throws UnsupportedOperatorException
     *             If the temporal operator is not supported
     */
    public TProcedure getTProcedureForIdentifier(String identifier, Set<String> possibleProcedureDescriptionFormats,
            Time validTime, Session session) throws UnsupportedTimeException, UnsupportedValueReferenceException,
            UnsupportedOperatorException {
        Criteria criteria =
                getDefaultTProcedureCriteria(session).add(Restrictions.eq(Procedure.IDENTIFIER, identifier));
        Criteria createValidProcedureTime = criteria.createCriteria(TProcedure.VALID_PROCEDURE_TIME);
        Criterion validTimeCriterion = QueryHelper.getValidTimeCriterion(validTime);
        if (validTime == null || validTimeCriterion == null) {
            createValidProcedureTime.add(Restrictions.isNull(ValidProcedureTime.END_TIME));
        } else {
            createValidProcedureTime.add(validTimeCriterion);
        }
        createValidProcedureTime.createCriteria(ValidProcedureTime.PROCEDURE_DESCRIPTION_FORMAT).add(
                Restrictions.in(ProcedureDescriptionFormat.PROCEDURE_DESCRIPTION_FORMAT,
                        possibleProcedureDescriptionFormats));
        LOGGER.debug(
                "QUERY getTProcedureForIdentifier(identifier, possibleProcedureDescriptionFormats, validTime): {}",
                HibernateHelper.getSqlString(criteria));
        return (TProcedure) criteria.uniqueResult();
    }

    public TimeExtrema getProcedureTimeExtremaFromNamedQuery(Session session, String procedureIdentifier) {
        Object[] result = null;

        return parseProcedureTimeExtremaResult(result);
    }
    
    private TimeExtrema parseProcedureTimeExtremaResult(Object[] result) {
        TimeExtrema pte = new TimeExtrema();
        if (result != null) {
            pte.setMinTime(DateTimeHelper.makeDateTime(result[1]));
            DateTime maxPhenStart = DateTimeHelper.makeDateTime(result[2]);
            DateTime maxPhenEnd = DateTimeHelper.makeDateTime(result[3]);
            pte.setMaxTime(DateTimeHelper.max(maxPhenStart, maxPhenEnd));
        }
        return pte;
    }

    /**
     * Query procedure time extrema for the provided procedure identifier
     *
     * @param session
     * @param procedureIdentifier
     * @return ProcedureTimeExtrema
     * @throws CodedException
     */
    public TimeExtrema getProcedureTimeExtrema(final Session session, String procedureIdentifier)
            throws OwsExceptionReport {
        Object[] result;
        AbstractObservationDAO observationDAO = DaoFactory.getInstance().getObservationDAO();
        Criteria criteria = observationDAO.getDefaultObservationInfoCriteria(session);
        if (observationDAO instanceof AbstractSeriesObservationDAO) {
            criteria.createAlias(SeriesObservationInfo.SERIES, "s");
            criteria.createAlias("s." + Series.PROCEDURE, "p");
        } else {
            criteria.createAlias(ObservationInfo.PROCEDURE, "p");
        }
        criteria.add(Restrictions.eq("p." + Procedure.IDENTIFIER, procedureIdentifier));
        ProjectionList projectionList = Projections.projectionList();
        projectionList.add(Projections.groupProperty("p." + Procedure.IDENTIFIER));
        projectionList.add(Projections.min(AbstractObservation.PHENOMENON_TIME_START));
        projectionList.add(Projections.max(AbstractObservation.PHENOMENON_TIME_START));
        projectionList.add(Projections.max(AbstractObservation.PHENOMENON_TIME_END));
        criteria.setProjection(projectionList);

        LOGGER.debug("QUERY getProcedureTimeExtrema(procedureIdentifier): {}", HibernateHelper.getSqlString(criteria));
        result = (Object[]) criteria.uniqueResult();
        
        return parseProcedureTimeExtremaResult(result);
    }

    /**
     * Get min time from observations for procedure
     *
     * @param procedure
     *            Procedure identifier
     * @param session
     *            Hibernate session
     * @return min time for procedure
     * @throws CodedException
     */
    public DateTime getMinDate4Procedure(final String procedure, final Session session) throws OwsExceptionReport {
        Object min = null;
            AbstractObservationDAO observationDAO = DaoFactory.getInstance().getObservationDAO();
            Criteria criteria = observationDAO.getDefaultObservationInfoCriteria(session);
            if (observationDAO instanceof SeriesObservationDAO) {
                addProcedureRestrictionForSeries(criteria, procedure);
            } else {
                addProcedureRestrictionForObservation(criteria, procedure);
            }
            addMinMaxProjection(criteria, MinMax.MIN, AbstractObservation.PHENOMENON_TIME_START);
            LOGGER.debug("QUERY getMinDate4Procedure(procedure): {}", HibernateHelper.getSqlString(criteria));
            min = criteria.uniqueResult();
        if (min != null) {
            return new DateTime(min, DateTimeZone.UTC);
        }
        return null;
    }

    /**
     * Get max time from observations for procedure
     *
     * @param procedure
     *            Procedure identifier
     * @param session
     *            Hibernate session
     * @return max time for procedure
     * @throws CodedException
     */
    public DateTime getMaxDate4Procedure(final String procedure, final Session session) throws OwsExceptionReport {
        Object maxStart = null;
        Object maxEnd = null;
            AbstractObservationDAO observationDAO = DaoFactory.getInstance().getObservationDAO();
            Criteria cstart = observationDAO.getDefaultObservationInfoCriteria(session);
            Criteria cend = observationDAO.getDefaultObservationInfoCriteria(session);
            if (observationDAO instanceof SeriesObservationDAO) {
                addProcedureRestrictionForSeries(cstart, procedure);
                addProcedureRestrictionForSeries(cend, procedure);
            } else {
                addProcedureRestrictionForObservation(cstart, procedure);
                addProcedureRestrictionForObservation(cend, procedure);
            }
            addMinMaxProjection(cstart, MinMax.MAX, AbstractObservation.PHENOMENON_TIME_START);
            addMinMaxProjection(cend, MinMax.MAX, AbstractObservation.PHENOMENON_TIME_END);
            LOGGER.debug("QUERY getMaxDate4Procedure(procedure) start: {}", HibernateHelper.getSqlString(cstart));
            LOGGER.debug("QUERY getMaxDate4Procedure(procedure) end: {}", HibernateHelper.getSqlString(cend));
            if (HibernateHelper.getSqlString(cstart).endsWith(HibernateHelper.getSqlString(cend))) {
                maxStart = cstart.uniqueResult();
                maxEnd = maxStart;
                LOGGER.debug("Max time start and end query are identically, only one query is executed!");
            } else {
                maxStart = cstart.uniqueResult();
                maxEnd = cend.uniqueResult();
            }
        if (maxStart == null && maxEnd == null) {
            return null;
        } else {
            final DateTime start = new DateTime(maxStart, DateTimeZone.UTC);
            if (maxEnd != null) {
                final DateTime end = new DateTime(maxEnd, DateTimeZone.UTC);
                if (end.isAfter(start)) {
                    return end;
                }
            }
            return start;
        }
    }

    /**
     * Add procedure identifier restriction to Hibernate Criteria for series
     *
     * @param criteria
     *            Hibernate Criteria for series to add restriction
     * @param procedure
     *            Procedure identifier
     */
    private void addProcedureRestrictionForSeries(Criteria criteria, String procedure) {
        Criteria seriesCriteria = criteria.createCriteria(SeriesObservationInfo.SERIES);
        seriesCriteria.createCriteria(SeriesObservationInfo.PROCEDURE).add(
                Restrictions.eq(Procedure.IDENTIFIER, procedure));
    }

    /**
     * Add procedure identifier restriction to Hibernate Criteria
     *
     * @param criteria
     *            Hibernate Criteria to add restriction
     * @param procedure
     *            Procedure identifier
     */
    private void addProcedureRestrictionForObservation(Criteria criteria, String procedure) {
        criteria.createCriteria(ObservationInfo.PROCEDURE).add(Restrictions.eq(Procedure.IDENTIFIER, procedure));
    }

    public Map<String,String> getProcedureFormatMap(Session session) {
            //get the latest validProcedureTimes' procedureDescriptionFormats
            return new ValidProcedureTimeDAO().getTProcedureFormatMap(session);
    }
}
