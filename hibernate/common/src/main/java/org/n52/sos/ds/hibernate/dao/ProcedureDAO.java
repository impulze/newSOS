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
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;
import org.n52.sos.ds.hibernate.dao.ereporting.EReportingSeriesDAO;
import org.n52.sos.ds.hibernate.entities.Procedure;
import org.n52.sos.ds.hibernate.entities.TProcedure;
import org.n52.sos.ds.hibernate.entities.series.Series;
import org.n52.sos.exception.CodedException;
import org.n52.sos.exception.ows.concrete.UnsupportedOperatorException;
import org.n52.sos.exception.ows.concrete.UnsupportedTimeException;
import org.n52.sos.exception.ows.concrete.UnsupportedValueReferenceException;
import org.n52.sos.ogc.gml.time.Time;
import org.n52.sos.ogc.sensorML.SensorML20Constants;
import org.n52.sos.service.SosContextListener;
import org.n52.sos.util.CollectionHelper;

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

    public List<Procedure> createProceduresWithSensors(List<Sensor> sensors, Session session) {
    	final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;
    	final List<Procedure> procedures = Lists.newArrayListWithCapacity(sensors.size());

    	for (final Sensor sensor: sensors) {
    		final TProcedure procedure = new TProcedure();

    		// TODOHZG: set description and format
    		procedure.setDeleted(false);
    		procedure.setIdentifier(sosConfiguration.getProcedureIdentifierPrefix() + sensor.getName());
    		procedure.setProcedureDescriptionFormat(new ProcedureDescriptionFormatDAO().createProcedureDescriptionFormat());

    		procedures.add(procedure);
    	}

    	return procedures;
    }

    /**
     * Get all procedure objects
     *
     * @param session
     *            Hibernate session
     * @return Procedure objects
     */
    public List<Procedure> getProcedureObjects(final Session session) {
    	final Criteria criteria = session.createCriteria(Sensor.class);
    	@SuppressWarnings("unchecked")
		final List<Sensor> sensors = criteria.list();

    	return createProceduresWithSensors(sensors, session);
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
    	final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;
    	final String prefix = sosConfiguration.getProcedureIdentifierPrefix();

    	if (!identifier.startsWith(prefix)) {
    		return null;
    	}

    	final String name = identifier.substring(prefix.length());
    	final Criteria criteria = session.createCriteria(Sensor.class)
    		.add(Restrictions.eq("name", name));
    	final Sensor sensor = (Sensor)criteria.uniqueResult();

    	// TODOHZG: filter by valid procedure time (must be NULL)
    	if (sensor == null) {
    		return null;
    	}

    	return createProceduresWithSensors(Lists.newArrayList(sensor), session).get(0);
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

    	final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;
    	final String prefix = sosConfiguration.getProcedureIdentifierPrefix();
    	final List<String> names = Lists.newArrayList();

    	for (final String identifier: identifiers) {
    		if (identifier.startsWith(prefix)) {
    			names.add(identifier.substring(prefix.length()));
    		}
    	}

    	final Criteria criteria = session.createCriteria(Sensor.class)
    			.add(Restrictions.in("name", names));
    	final List<Sensor> sensors = criteria.list();

    	return createProceduresWithSensors(sensors, session);
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
    	final List<Series> allSeries = new EReportingSeriesDAO().getAllSeries(session);
    	final List<Object[]> foisAndProcedures = Lists.newArrayList();

    	for (final Series series: allSeries) {
    		final Object[] pair = new Object[] { series.getFeatureOfInterest().getIdentifier(), series.getProcedure().getIdentifier() };

    		foisAndProcedures.add(pair);
    	}

    	return foisAndProcedures;
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
    	if (!possibleProcedureDescriptionFormats.contains(SensorML20Constants.NS_SML_20)) {
    		return null;
    	}

    	final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;

    	if (!identifier.startsWith(sosConfiguration.getProcedureIdentifierPrefix())) {
    		return null;
    	}

    	final String name = identifier.substring(sosConfiguration.getProcedureIdentifierPrefix().length());
    	final Criteria criteria = session.createCriteria(Sensor.class)
    		.add(Restrictions.eq("name", name));
		final Sensor sensor = (Sensor)criteria.uniqueResult();

    	// TODOHZG: filter by QueryHelper.getValidTimeCriterion
    	/*
        Criteria createValidProcedureTime = criteria.createCriteria(TProcedure.VALID_PROCEDURE_TIME);
        Criterion validTimeCriterion = QueryHelper.getValidTimeCriterion(validTime);
        if (validTime == null || validTimeCriterion == null) {
            createValidProcedureTime.add(Restrictions.isNull(ValidProcedureTime.END_TIME));
        } else {
            createValidProcedureTime.add(validTimeCriterion);
        }
        */

		if (sensor == null) {
			return null;
		}
		
		return (TProcedure)createProceduresWithSensors(Lists.newArrayList(sensor), session).get(0);
    }

    public Map<String,String> getProcedureFormatMap(Session session) {
            //get the latest validProcedureTimes' procedureDescriptionFormats
            return new ValidProcedureTimeDAO().getTProcedureFormatMap(session);
    }
}
