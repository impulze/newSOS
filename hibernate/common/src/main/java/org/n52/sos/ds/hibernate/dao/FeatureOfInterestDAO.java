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

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.n52.sos.ds.hibernate.dao.series.SeriesObservationDAO;
import org.n52.sos.ds.hibernate.entities.AbstractObservation;
import org.n52.sos.ds.hibernate.entities.FeatureOfInterest;
import org.n52.sos.ds.hibernate.entities.ObservationConstellation;
import org.n52.sos.ds.hibernate.entities.ObservationInfo;
import org.n52.sos.ds.hibernate.entities.Offering;
import org.n52.sos.ds.hibernate.entities.TFeatureOfInterest;
import org.n52.sos.ds.hibernate.entities.series.Series;
import org.n52.sos.ds.hibernate.entities.series.SeriesObservationInfo;
import org.n52.sos.ds.hibernate.util.HibernateHelper;
import org.n52.sos.exception.CodedException;
import org.n52.sos.ogc.ows.OwsExceptionReport;
import org.n52.sos.service.SosContextListener;
import org.n52.sos.util.CollectionHelper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import de.hzg.common.SOSConfiguration;

/**
 * Hibernate data access class for featureOfInterest
 *
 * @author CarstenHollmann
 * @since 4.0.0
 */
public class FeatureOfInterestDAO extends AbstractIdentifierNameDescriptionDAO implements HibernateSqlQueryConstants {

    private static final Logger LOGGER = LoggerFactory.getLogger(FeatureOfInterestDAO.class);

    public FeatureOfInterest createFeatureOfInterest(Session session) {
    	final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;
    	final TFeatureOfInterest foi = new TFeatureOfInterest();

    	// TODOHZG: add additional information
    	foi.setIdentifier(sosConfiguration.getFeatureOfInterestIdentifierPrefix() + sosConfiguration.getFeatureOfInterestName());
    	foi.setName("some foi name");
    	foi.setFeatureOfInterestType(new FeatureOfInterestTypeDAO().getFeatureOfInterestTypeObject(FeatureOfInterestTypeDAO.HZG_FEATURE_OF_INTEREST_TYPE_STRING, session));

    	return foi;
    }

    /**
     * Get featureOfInterest object for identifier
     *
     * @param identifier
     *            FeatureOfInterest identifier
     * @param session
     *            Hibernate session Hibernate session
     * @return FeatureOfInterest entity
     */
    public FeatureOfInterest getFeatureOfInterest(final String identifier, final Session session) {
    	final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;

    	if (identifier.equals(sosConfiguration.getFeatureOfInterestIdentifierPrefix() + sosConfiguration.getFeatureOfInterestName())) {
    		return (FeatureOfInterest)createFeatureOfInterest(session);
    	}

    	return null;
    }

    /**
     * Get featureOfInterest identifiers for observation constellation
     *
     * @param observationConstellation
     *            Observation constellation
     * @param session
     *            Hibernate session Hibernate session
     * @return FeatureOfInterest identifiers for observation constellation
     * @throws CodedException
     */
    @SuppressWarnings("unchecked")
    public List<String> getFeatureOfInterestIdentifiersForObservationConstellation(
            final ObservationConstellation observationConstellation, final Session session) throws OwsExceptionReport {
            AbstractObservationDAO observationDAO = DaoFactory.getInstance().getObservationDAO();
            Criteria criteria = observationDAO.getDefaultObservationInfoCriteria(session);
            if (observationDAO instanceof SeriesObservationDAO) {
                Criteria seriesCriteria = criteria.createCriteria(SeriesObservationInfo.SERIES);
                seriesCriteria.add(Restrictions.eq(Series.PROCEDURE, observationConstellation.getProcedure())).add(
                        Restrictions.eq(Series.OBSERVABLE_PROPERTY, observationConstellation.getObservableProperty()));
                seriesCriteria.createCriteria(Series.FEATURE_OF_INTEREST).setProjection(
                        Projections.distinct(Projections.property(FeatureOfInterest.IDENTIFIER)));
            } else {
                criteria.add(Restrictions.eq(ObservationConstellation.PROCEDURE, observationConstellation.getProcedure()))
                        .add(Restrictions.eq(ObservationConstellation.OBSERVABLE_PROPERTY,
                                observationConstellation.getObservableProperty()));
                criteria.createCriteria(ObservationInfo.FEATURE_OF_INTEREST).setProjection(
                        Projections.distinct(Projections.property(FeatureOfInterest.IDENTIFIER)));
            }
            criteria.createCriteria(AbstractObservation.OFFERINGS).add(
                    Restrictions.eq(Offering.ID, observationConstellation.getOffering().getOfferingId()));
            LOGGER.debug(
                    "QUERY getFeatureOfInterestIdentifiersForObservationConstellation(observationConstellation): {}",
                    HibernateHelper.getSqlString(criteria));
            return criteria.list();
    }

    /**
     * Get featureOfInterest identifiers for an offering identifier
     *
     * @param offeringIdentifiers
     *            Offering identifier
     * @param session
     *            Hibernate session Hibernate session
     * @return FeatureOfInterest identifiers for offering
     * @throws CodedException
     */
    @SuppressWarnings({ "unchecked" })
    public List<String> getFeatureOfInterestIdentifiersForOffering(final String offeringIdentifiers,
            final Session session) throws OwsExceptionReport {
            AbstractObservationDAO observationDAO = DaoFactory.getInstance().getObservationDAO();
            Criteria c = observationDAO.getDefaultObservationInfoCriteria(session);
            if (observationDAO instanceof SeriesObservationDAO) {
                Criteria seriesCriteria = c.createCriteria(SeriesObservationInfo.SERIES);
                seriesCriteria.createCriteria(Series.FEATURE_OF_INTEREST).setProjection(
                        Projections.distinct(Projections.property(FeatureOfInterest.IDENTIFIER)));

            } else {
                c.createCriteria(AbstractObservation.FEATURE_OF_INTEREST).setProjection(
                        Projections.distinct(Projections.property(FeatureOfInterest.IDENTIFIER)));
            }
            new OfferingDAO().addOfferingRestricionForObservation(c, offeringIdentifiers);
            LOGGER.debug("QUERY getFeatureOfInterestIdentifiersForOffering(offeringIdentifiers): {}",
                    HibernateHelper.getSqlString(c));
            return c.list();
    }

    /**
     * Get featureOfInterest objects for featureOfInterest identifiers
     *
     * @param identifiers
     *            FeatureOfInterest identifiers
     * @param session
     *            Hibernate session
     * @return FeatureOfInterest objects
     */
    @SuppressWarnings("unchecked")
    public List<FeatureOfInterest> getFeatureOfInterestObject(final Collection<String> identifiers,
            final Session session) {
        if (identifiers != null && !identifiers.isEmpty()) {
            Criteria criteria =
                    session.createCriteria(FeatureOfInterest.class).add(
                            Restrictions.in(FeatureOfInterest.IDENTIFIER, identifiers));
            LOGGER.debug("QUERY getFeatureOfInterestObject(identifiers): {}", HibernateHelper.getSqlString(criteria));
            return criteria.list();
        }
        return Collections.emptyList();
    }

    /**
     * Get all featureOfInterest objects
     *
     * @param session
     *            Hibernate session
     * @return FeatureOfInterest objects
     */
    public List<FeatureOfInterest> getFeatureOfInterestObjects(final Session session) {
    	return Lists.newArrayList(createFeatureOfInterest(session));
    }

    /**
     * Load FOI identifiers and parent ids for use in the cache. Just loading the ids allows us to not load
     * the geometry columns, XML, etc.
     *
     * @param session
     * @return Map keyed by FOI identifiers, with value collections of parent FOI identifiers if supported
     */
    public Map<String,Collection<String>> getFeatureOfInterestIdentifiersWithParents(final Session session) {
    	final List<FeatureOfInterest> fois = Lists.newArrayList(createFeatureOfInterest(session));
        final Map<String, Collection<String>> map = Maps.newHashMap();

        for (final FeatureOfInterest foi: fois) {
        	map.put(foi.getIdentifier(), null);

        	if (foi instanceof TFeatureOfInterest) {
        		for (final FeatureOfInterest parent: ((TFeatureOfInterest)foi).getParents()) {
        			CollectionHelper.addToCollectionMap(foi.getIdentifier(), parent.getIdentifier(), map);
        		}
        	}
        }

        return map;
    }

    /**
     * Get all featureOfInterest identifiers
     *
     * @param session
     *            Hibernate session
     * @return FeatureOfInterest identifiers
     */
    @SuppressWarnings("unchecked")
    public List<String> getFeatureOfInterestIdentifiers(Session session) {
        Criteria criteria =
                session.createCriteria(FeatureOfInterest.class).setProjection(
                        Projections.distinct(Projections.property(FeatureOfInterest.IDENTIFIER)));
        LOGGER.debug("QUERY getFeatureOfInterestIdentifiers(): {}", HibernateHelper.getSqlString(criteria));
        return criteria.list();
    }
}