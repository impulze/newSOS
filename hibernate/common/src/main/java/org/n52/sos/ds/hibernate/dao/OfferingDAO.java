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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.DetachedCriteria;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernate.criterion.Subqueries;
import org.joda.time.DateTime;
import org.n52.sos.ds.hibernate.entities.FeatureOfInterestType;
import org.n52.sos.ds.hibernate.entities.ObservableProperty;
import org.n52.sos.ds.hibernate.entities.ObservationConstellation;
import org.n52.sos.ds.hibernate.entities.ObservationType;
import org.n52.sos.ds.hibernate.entities.Offering;
import org.n52.sos.ds.hibernate.entities.Procedure;
import org.n52.sos.ds.hibernate.entities.TOffering;
import org.n52.sos.ds.hibernate.util.HibernateHelper;
import org.n52.sos.exception.CodedException;
import org.n52.sos.ogc.gml.time.TimePeriod;
import org.n52.sos.ogc.ows.OwsExceptionReport;
import org.n52.sos.service.SosContextListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import de.hzg.common.SOSConfiguration;

/**
 * Hibernate data access class for offering
 *
 * @author CarstenHollmann
 * @since 4.0.0
 */
public class OfferingDAO extends TimeCreator implements HibernateSqlQueryConstants {
    /**
     * Logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(OfferingDAO.class);

    private static TOffering createTOffering(Session session) {
    	final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;
    	final List<String> foiTypeStrings = new FeatureOfInterestTypeDAO().getFeatureOfInterestTypes(session);
    	final List<FeatureOfInterestType> foiTypes = new FeatureOfInterestTypeDAO().getFeatureOfInterestTypeObjects(foiTypeStrings, session);
    	final List<String> obsTypeStrings = Lists.newArrayList(ObservationTypeDAO.HZG_OBSERVATION_TYPE_STRING);
    	final List<ObservationType> obsTypes = new ObservationTypeDAO().getObservationTypeObjects(obsTypeStrings, session);
    	final TOffering offering = new TOffering();

    	offering.setIdentifier(sosConfiguration.getOfferingIdentifierPrefix() + sosConfiguration.getOfferingName());
    	offering.setDisabled(false);
    	offering.setFeatureOfInterestTypes(Sets.newHashSet(foiTypes));
    	offering.setObservationTypes(Sets.newHashSet(obsTypes));
    	offering.setName("some name");

    	return offering;
    }

    /**
     * Get transactional offering object for identifier
     *
     * @param identifier
     *            Offering identifier
     * @param session
     *            Hibernate session
     * @return Transactional offering object
     */
    public TOffering getTOfferingForIdentifier(final String identifier, final Session session) {
    	final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;

    	if (identifier.equals(sosConfiguration.getOfferingIdentifierPrefix() + sosConfiguration.getOfferingName())) {
    		return createTOffering(session);
    	}

    	return null;
    }

    /**
     * Get offering objects for cache update
     *
     * @param identifiers
     *            Optional collection of offering identifiers to fetch. If null, all offerings are returned.
     * @param session
     *            Hibernate session
     * @return Offering objects
     */
    public List<Offering> getOfferingObjectsForCacheUpdate(final Collection<String> identifiers, final Session session) {
    	if (identifiers.isEmpty()) {
    		return Lists.newArrayList((Offering)createTOffering(session));
    	}

    	final List<Offering> offerings = Lists.newArrayList();

    	for (final String identifier: identifiers) {
    		offerings.add(getTOfferingForIdentifier(identifier, session));
    	}

    	return offerings;
    }

    /**
     * Get Offering object for identifier
     *
     * @param identifier
     *            Offering identifier
     * @param session
     *            Hibernate session
     * @return Offering object
     */
    public Offering getOfferingForIdentifier(final String identifier, final Session session) {
    	return getTOfferingForIdentifier(identifier, session);
    }

    /**
     * Get Offering objects for identifiers
     *
     * @param identifiers
     *            Offering identifiers
     * @param session
     *            Hibernate session
     * @return Offering objects
     */
    @SuppressWarnings("unchecked")
    public Collection<Offering> getOfferingsForIdentifiers(final Collection<String> identifiers, final Session session) {
        Criteria criteria =
                session.createCriteria(Offering.class).add(Restrictions.in(Offering.IDENTIFIER, identifiers));
        LOGGER.debug("QUERY getOfferingsForIdentifiers(identifiers): {}", HibernateHelper.getSqlString(criteria));
        return (List<Offering>) criteria.list();
    }

    /**
     * Get offering identifiers for procedure identifier
     *
     * @param procedureIdentifier
     *            Procedure identifier
     * @param session
     *            Hibernate session
     * @return Offering identifiers
     * @throws OwsExceptionReport
     */
    @SuppressWarnings("unchecked")
    public List<String> getOfferingIdentifiersForProcedure(final String procedureIdentifier, final Session session) throws OwsExceptionReport {
        Criteria c = null;
            c = session.createCriteria(Offering.class);
            c.add(Subqueries.propertyIn(Offering.ID, getDetachedCriteriaOfferingForProcedureFromObservationConstellation(procedureIdentifier, session)));
            c.setProjection(Projections.distinct(Projections.property(Offering.IDENTIFIER)));
        LOGGER.debug(
                "QUERY getOfferingIdentifiersForProcedure(procedureIdentifier) using ObservationContellation entitiy ({}): {}",
                true, HibernateHelper.getSqlString(c));
        return c.list();
    }

    /**
     * Get offering identifiers for observable property identifier
     *
     * @param observablePropertyIdentifier
     *            Observable property identifier
     * @param session
     *            Hibernate session
     * @return Offering identifiers
     * @throws CodedException
     */
    @SuppressWarnings("unchecked")
    public Collection<String> getOfferingIdentifiersForObservableProperty(final String observablePropertyIdentifier,
            final Session session) throws OwsExceptionReport {
        Criteria c = null;
            c = session.createCriteria(Offering.class);
            c.add(Subqueries.propertyIn(Offering.ID,
                    getDetachedCriteriaOfferingForObservablePropertyFromObservationConstellation(observablePropertyIdentifier, session)));
            c.setProjection(Projections.distinct(Projections.property(Offering.IDENTIFIER)));
        LOGGER.debug(
                "QUERY getOfferingIdentifiersForObservableProperty(observablePropertyIdentifier) using ObservationContellation entitiy ({}): {}",
                true, HibernateHelper.getSqlString(c));
        return c.list();
    }

    public class OfferingTimeExtrema {
        private DateTime minPhenomenonTime;
        private DateTime maxPhenomenonTime;
        private DateTime minResultTime;
        private DateTime maxResultTime;

        public DateTime getMinPhenomenonTime() {
            return minPhenomenonTime;
        }

        public void setMinPhenomenonTime(DateTime minPhenomenonTime) {
            this.minPhenomenonTime = minPhenomenonTime;
        }

        public DateTime getMaxPhenomenonTime() {
            return maxPhenomenonTime;
        }

        public void setMaxPhenomenonTime(DateTime maxPhenomenonTime) {
            this.maxPhenomenonTime = maxPhenomenonTime;
        }

        public DateTime getMinResultTime() {
            return minResultTime;
        }

        public void setMinResultTime(DateTime minResultTime) {
            this.minResultTime = minResultTime;
        }

        public DateTime getMaxResultTime() {
            return maxResultTime;
        }

        public void setMaxResultTime(DateTime maxResultTime) {
            this.maxResultTime = maxResultTime;
        }
    }

    /**
     * Get offering time extrema
     *
     * @param identifiers
     *            Optional collection of offering identifiers to fetch. If null, all offerings are returned.
     * @param session
     *            Hibernate session Hibernate session
     * @return Map of offering time extrema, keyed by offering identifier
     * @throws CodedException
     */
    public Map<String,OfferingTimeExtrema> getOfferingTimeExtrema(final Collection<String> identifiers,
            final Session session) throws OwsExceptionReport {
    	/* TODOHZG: this should return the maximum phenomenon and result times
    	 * the code below was used previously in here
    	 */

    	final Map<String, OfferingTimeExtrema> map = Maps.newHashMap();

    	/*
    	for (Object[] result : results) {
            String offering = (String) result[0];
            OfferingTimeExtrema ote = new OfferingTimeExtrema();
            ote.setMinPhenomenonTime(DateTimeHelper.makeDateTime(result[1]));
            DateTime maxPhenStart = DateTimeHelper.makeDateTime(result[2]);
            DateTime maxPhenEnd = DateTimeHelper.makeDateTime(result[3]);
            ote.setMaxPhenomenonTime(DateTimeHelper.max(maxPhenStart, maxPhenEnd));
            ote.setMinResultTime(DateTimeHelper.makeDateTime(result[4]));
            ote.setMaxResultTime(DateTimeHelper.makeDateTime(result[5]));
            map.put(offering, ote);
        }
        */

        return map;
    }

    /**
     * Get temporal bounding box for each offering
     *
     * @param session
     *            Hibernate session
     * @return a Map containing the temporal bounding box for each offering
     * @throws CodedException
     */
    public Map<String, TimePeriod> getTemporalBoundingBoxesForOfferings(final Session session) throws OwsExceptionReport {
        if (session != null) {
        	final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;
        	final String offeringIdentifier = sosConfiguration.getOfferingIdentifierPrefix() + sosConfiguration.getOfferingName();
        	final Map<String, OfferingTimeExtrema> offeringTimeExtremas = getOfferingTimeExtrema(Lists.newArrayList(offeringIdentifier), session);
        	final HashMap<String, TimePeriod> temporalBBoxMap = new HashMap<String, TimePeriod>(offeringTimeExtremas.size());

        	for (final Map.Entry<String, OfferingTimeExtrema> entry: offeringTimeExtremas.entrySet()) {
        		final OfferingTimeExtrema extrema = entry.getValue();
        		final TimePeriod value = new TimePeriod(extrema.getMinPhenomenonTime(), extrema.getMaxPhenomenonTime());

        		temporalBBoxMap.put(entry.getKey(), value);
            }

        	return temporalBBoxMap;
        }
        return new HashMap<String, TimePeriod>(0);
    }

    /**
     * Get Hibernate Detached Criteria for class ObservationConstellation and
     * observableProperty identifier
     *
     * @param observablePropertyIdentifier
     *            ObservableProperty identifier parameter
     * @param session
     *            Hibernate session
     * @return Detached Criteria with Offering entities as result
     */
    private DetachedCriteria getDetachedCriteriaOfferingForObservablePropertyFromObservationConstellation(String observablePropertyIdentifier, Session session) {
        final DetachedCriteria detachedCriteria = DetachedCriteria.forClass(ObservationConstellation.class);
        detachedCriteria.add(Restrictions.eq(ObservationConstellation.DELETED, false));
        detachedCriteria.createCriteria(ObservationConstellation.OBSERVABLE_PROPERTY).add(
                Restrictions.eq(ObservableProperty.IDENTIFIER, observablePropertyIdentifier));
        detachedCriteria.setProjection(Projections.distinct(Projections.property(ObservationConstellation.OFFERING)));
        return detachedCriteria;
    }

    /**
     * Get Hibernate Detached Criteria for class ObservationConstellation and
     * procedure identifier
     *
     * @param procedureIdentifier
     *            Procedure identifier parameter
     * @param session
     *            Hibernate session
     * @return Detached Criteria with Offering entities as result
     */
    private DetachedCriteria getDetachedCriteriaOfferingForProcedureFromObservationConstellation(String procedureIdentifier, Session session) {
        final DetachedCriteria detachedCriteria = DetachedCriteria.forClass(ObservationConstellation.class);
        detachedCriteria.add(Restrictions.eq(ObservationConstellation.DELETED, false));
        detachedCriteria.createCriteria(ObservationConstellation.PROCEDURE).add(
                Restrictions.eq(Procedure.IDENTIFIER, procedureIdentifier));
        detachedCriteria.setProjection(Projections.distinct(Projections.property(ObservationConstellation.OFFERING)));
        return detachedCriteria;
    }

    /**
     * Query allowed FeatureOfInterestTypes for offering
     * @param offeringIdentifier Offering identifier
     * @param session
     *            Hibernate session
     * @return Allowed FeatureOfInterestTypes
     */
    public List<String> getAllowedFeatureOfInterestTypes(String offeringIdentifier, Session session) {
            Criteria criteria =
                    session.createCriteria(TOffering.class).add(Restrictions.eq(Offering.IDENTIFIER, offeringIdentifier));
            LOGGER.debug("QUERY getAllowedFeatureOfInterestTypes(offering): {}",
                    HibernateHelper.getSqlString(criteria));
            TOffering offering = (TOffering) criteria.uniqueResult();
            if (offering != null) {
                List<String> list = Lists.newArrayList();
                for (FeatureOfInterestType featureOfInterestType : offering.getFeatureOfInterestTypes()) {
                    list.add(featureOfInterestType.getFeatureOfInterestType());
                }
                return list;
            }
        return Lists.newArrayList();
    }
}
