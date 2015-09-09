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

import java.sql.Timestamp;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.n52.sos.ds.hibernate.entities.AbstractObservation;
import org.n52.sos.ds.hibernate.entities.FeatureOfInterestType;
import org.n52.sos.ds.hibernate.entities.ObservationType;
import org.n52.sos.ds.hibernate.entities.Offering;
import org.n52.sos.ds.hibernate.entities.Procedure;
import org.n52.sos.ds.hibernate.entities.RelatedFeature;
import org.n52.sos.ds.hibernate.entities.TOffering;
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
import de.hzg.values.CalculatedData;

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

    private static TOffering createTOffering(String identifier, Session session) {
        final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;
        final TOffering toffering = new TOffering();
        // we only have one offering, get all fois and ots
        final List<String> foiTypeStrings = new FeatureOfInterestTypeDAO().getFeatureOfInterestTypes(session);
        final List<FeatureOfInterestType> foiTypes = new FeatureOfInterestTypeDAO().getFeatureOfInterestTypeObjects(foiTypeStrings, session);
        final List<String> obsTypeStrings = Lists.newArrayList(ObservationTypeDAO.HZG_OBSERVATION_TYPE);
        final List<ObservationType> obsTypes= new ObservationTypeDAO().getObservationTypeObjects(obsTypeStrings, session);

        toffering.setIdentifier(sosConfiguration.getOfferingIdentifierPrefix() + identifier);
        toffering.setDisabled(false);
        toffering.setFeatureOfInterestTypes(Sets.newHashSet(foiTypes));
        toffering.setObservationTypes(Sets.newHashSet(obsTypes));
        toffering.setName("Some Name");

        return toffering;
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

        if (!identifier.equals(sosConfiguration.getOfferingIdentifierPrefix() + sosConfiguration.getOfferingName())) {
            return null;
        }

        return createTOffering(sosConfiguration.getOfferingName(), session);
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
        // TODOHZG: determine this from FOI
        final List<Offering> offerings = Lists.newArrayList();
        final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;

        if (!identifiers.isEmpty()) {
            for (final String identifier: identifiers) {
                if (identifier.equals(sosConfiguration.getOfferingIdentifierPrefix() + sosConfiguration.getOfferingName())) {
                    offerings.add(getTOfferingForIdentifier(sosConfiguration.getOfferingIdentifierPrefix() + sosConfiguration.getOfferingName(), session));
                }
            }
        } else {
            offerings.add(getTOfferingForIdentifier(sosConfiguration.getOfferingIdentifierPrefix() + sosConfiguration.getOfferingName(), session));
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
        final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;

        if (!identifier.equals(sosConfiguration.getOfferingIdentifierPrefix() + sosConfiguration.getOfferingName())) {
            return null;
        }

        return getTOfferingForIdentifier(sosConfiguration.getOfferingIdentifierPrefix() + sosConfiguration.getOfferingName(), session);
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
    public Collection<Offering> getOfferingsForIdentifiers(final Collection<String> identifiers, final Session session) {
        final List<Offering> offerings = Lists.newArrayList();
        final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;

        for (final String identifier: identifiers) {
            if (identifier.equals(sosConfiguration.getOfferingIdentifierPrefix() + sosConfiguration.getOfferingName())) {
                offerings.add(getTOfferingForIdentifier(sosConfiguration.getOfferingName(), session));
            }
        }

        return offerings;
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
    public List<String> getOfferingIdentifiersForProcedure(final String procedureIdentifier, final Session session) throws OwsExceptionReport {
        // check if the procedures are supported by this offering
        final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;
        final Procedure procedure = new ProcedureDAO().getProcedureForIdentifier(procedureIdentifier, session);

        if (procedure != null) {
            return Lists.newArrayList(sosConfiguration.getOfferingIdentifierPrefix() + sosConfiguration.getOfferingName());
        }

        return Collections.emptyList();
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
    public Collection<String> getOfferingIdentifiersForObservableProperty(final String observablePropertyIdentifier,
            final Session session) throws OwsExceptionReport {
        // observable properties = procedures for now
        return getOfferingIdentifiersForProcedure(observablePropertyIdentifier, session);
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
        final Map<String, OfferingTimeExtrema> map = Maps.newHashMap();
        final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;

        final DateTime oldestEntry = getMinDate4Offering(sosConfiguration.getOfferingIdentifierPrefix() + sosConfiguration.getOfferingName(), session);
        final DateTime futureEntry = getMaxDate4Offering(sosConfiguration.getOfferingIdentifierPrefix() + sosConfiguration.getOfferingName(), session);

        if (oldestEntry != null && futureEntry != null) {
            final OfferingTimeExtrema ote = new OfferingTimeExtrema();

            ote.setMinPhenomenonTime(oldestEntry);
            ote.setMaxPhenomenonTime(futureEntry);
            ote.setMinResultTime(oldestEntry);
            ote.setMaxResultTime(futureEntry);

            map.put(sosConfiguration.getOfferingIdentifierPrefix() + sosConfiguration.getOfferingName(), ote);
        }

        return map;
    }

    /**
     * Get min time from observations for offering
     *
     * @param offering
     *            Offering identifier
     * @param session
     *            Hibernate session Hibernate session
     * @return min time for offering
     * @throws CodedException
     */
    public DateTime getMinDate4Offering(final String offering, final Session session) throws OwsExceptionReport {
        final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;

        if (!offering.equals(sosConfiguration.getOfferingIdentifierPrefix() + sosConfiguration.getOfferingName())) {
            return null;
        }

        final Criteria criteria = session.createCriteria(CalculatedData.class)
                .setProjection(Projections.projectionList()
                        .add(Projections.min("date")));
        final Timestamp timestamp = (Timestamp)criteria.uniqueResult();

        if (timestamp == null) {
            return null;
        }

        return new DateTime(timestamp, DateTimeZone.UTC);
    }

    /**
     * Get max time from observations for offering
     *
     * @param offering
     *            Offering identifier
     * @param session
     *            Hibernate session Hibernate session
     * @return max time for offering
     * @throws CodedException
     */
    public DateTime getMaxDate4Offering(final String offering, final Session session) throws OwsExceptionReport {
        final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;

        if (!offering.equals(sosConfiguration.getOfferingIdentifierPrefix() + sosConfiguration.getOfferingName())) {
            return null;
        }

        // TODOHZG: only return maxdate if there's a calculation?
        return new DateTime(2025, 1, 1, 0, 0, DateTimeZone.UTC);
    }

    /**
     * Get min result time from observations for offering
     *
     * @param offering
     *            Offering identifier
     * @param session
     *            Hibernate session Hibernate session
     *
     * @return min result time for offering
     * @throws CodedException
     */
    public DateTime getMinResultTime4Offering(final String offering, final Session session) throws OwsExceptionReport {
        return getMinDate4Offering(offering, session);
    }

    /**
     * Get max result time from observations for offering
     *
     * @param offering
     *            Offering identifier
     * @param session
     *            Hibernate session Hibernate session
     *
     * @return max result time for offering
     * @throws CodedException
     */
    public DateTime getMaxResultTime4Offering(final String offering, final Session session) throws OwsExceptionReport {
        return getMaxDate4Offering(offering, session);
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
        if (session == null) {
            return Collections.emptyMap();
        }

        final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;
        // based on phenomenon time
        final DateTime oldestEntry = getMinDate4Offering(sosConfiguration.getOfferingIdentifierPrefix() + sosConfiguration.getOfferingName(), session);
        final DateTime newestEntry = getMaxDate4Offering(sosConfiguration.getOfferingIdentifierPrefix() + sosConfiguration.getOfferingName(), session);

        if (oldestEntry != null && newestEntry != null) {
            final TimePeriod period = new TimePeriod(oldestEntry, newestEntry);
            final HashMap<String, TimePeriod> temporalBBoxMap = Maps.newHashMap();

            temporalBBoxMap.put(sosConfiguration.getOfferingIdentifierPrefix() + sosConfiguration.getOfferingName(), period);

            return temporalBBoxMap;
        }

        return Collections.emptyMap();
    }

    /**
     * Insert or update and get offering
     *
     * @param offeringIdentifier
     *            Offering identifier
     * @param offeringName
     *            Offering name
     * @param relatedFeatures
     *            Related feature objects
     * @param observationTypes
     *            Allowed observation type objects
     * @param featureOfInterestTypes
     *            Allowed featureOfInterest type objects
     * @param session
     *            Hibernate session
     * @return Offering object
     */
    public Offering getAndUpdateOrInsertNewOffering(final String offeringIdentifier, final String offeringName,
            final List<RelatedFeature> relatedFeatures, final List<ObservationType> observationTypes,
            final List<FeatureOfInterestType> featureOfInterestTypes, final Session session) {
        final TOffering offering = getTOfferingForIdentifier(offeringIdentifier, session);

        if (offering != null) {
            if (!offering.getObservationTypes().containsAll(observationTypes)) {
                final StringBuilder stringBuilder = new StringBuilder();

                stringBuilder.append("The offering has the following observation types: ");
                stringBuilder.append(offering.getObservationTypes());
                stringBuilder.append(" but the observation types to set were: ");
                stringBuilder.append(observationTypes);
                stringBuilder.append(". Modifying observation types is not supported yet.");

                throw new RuntimeException(stringBuilder.toString());
            }

            if (!offering.getFeatureOfInterestTypes().containsAll(featureOfInterestTypes)) {
                final StringBuilder stringBuilder = new StringBuilder();

                stringBuilder.append("The offering has the following features of interest types: ");
                stringBuilder.append(offering.getFeatureOfInterestTypes());
                stringBuilder.append(" but the feature of interest types to set were: ");
                stringBuilder.append(observationTypes);
                stringBuilder.append(". Modifying feature of interest types is not supported yet.");

                throw new RuntimeException(stringBuilder.toString());
            }

            if (!relatedFeatures.isEmpty()) {
                throw new RuntimeException("Modifying related features is not supported yet.");
            }

            return offering;
        } else {
            throw new RuntimeException("Insertion of offerings not yet supported.");
        }
    }

    /**
     * Query allowed FeatureOfInterestTypes for offering
     * @param offeringIdentifier Offering identifier
     * @param session
     *            Hibernate session
     * @return Allowed FeatureOfInterestTypes
     */
    public List<String> getAllowedFeatureOfInterestTypes(String offeringIdentifier, Session session) {
        final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;

        if (!offeringIdentifier.equals(sosConfiguration.getOfferingIdentifierPrefix() + sosConfiguration.getOfferingName())) {
            return Collections.emptyList();
        }

        final TOffering offering = getTOfferingForIdentifier(sosConfiguration.getOfferingIdentifierPrefix() + sosConfiguration.getOfferingName(), session);
        final List<String> featureOfInterestTypeStrings = Lists.newArrayList();

        for (final FeatureOfInterestType foiType: offering.getFeatureOfInterestTypes()) {
            featureOfInterestTypeStrings.add(foiType.getFeatureOfInterestType());
        }

        return featureOfInterestTypeStrings;
    }

    /**
     * Add offering identifier restriction to Hibernate Criteria
     * @param criteria Hibernate Criteria to add restriction
     * @param offering Offering identifier
     */
    public void addOfferingRestricionForObservation(Criteria criteria, String offering) {
        criteria.createCriteria(AbstractObservation.OFFERINGS).add(Restrictions.eq(Offering.IDENTIFIER, offering));
    }
}
