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

import static org.n52.sos.util.http.HTTPStatus.BAD_REQUEST;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.hibernate.Session;
import org.n52.sos.ds.hibernate.entities.FeatureOfInterest;
import org.n52.sos.ds.hibernate.entities.ObservationConstellation;
import org.n52.sos.ds.hibernate.entities.Offering;
import org.n52.sos.ds.hibernate.entities.RelatedFeature;
import org.n52.sos.ds.hibernate.entities.TFeatureOfInterest;
import org.n52.sos.exception.CodedException;
import org.n52.sos.exception.ows.NoApplicableCodeException;
import org.n52.sos.ogc.gml.AbstractFeature;
import org.n52.sos.ogc.om.features.samplingFeatures.SamplingFeature;
import org.n52.sos.ogc.ows.OwsExceptionReport;
import org.n52.sos.service.Configurator;
import org.n52.sos.service.SosContextListener;
import org.n52.sos.util.CollectionHelper;
import org.n52.sos.util.Constants;
import org.n52.sos.util.http.HTTPStatus;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

import de.hzg.common.SOSConfiguration;

/**
 * Hibernate data access class for featureOfInterest
 *
 * @author CarstenHollmann
 * @since 4.0.0
 */
public class FeatureOfInterestDAO extends AbstractIdentifierNameDescriptionDAO implements HibernateSqlQueryConstants {
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

    	if (!identifier.equals(sosConfiguration.getFeatureOfInterestIdentifierPrefix() + sosConfiguration.getFeatureOfInterestName())) {
    		return null;
    	}

    	final FeatureOfInterest foi = new FeatureOfInterest();

    	foi.setFeatureOfInterestType(new FeatureOfInterestTypeDAO().getFeatureOfInterestTypeObject(FeatureOfInterestTypeDAO.HZG_FEATURE_OF_INTEREST_TYPE, session));
    	foi.setIdentifier(identifier);
    	foi.setLongitude(20.5);
    	foi.setLatitude(10.5);

    	return foi;
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
    public List<String> getFeatureOfInterestIdentifiersForObservationConstellation(
            final ObservationConstellation observationConstellation, final Session session) throws OwsExceptionReport {
    	//TODO: check if obs const is valid for data?
    	final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;
    	return Collections.singletonList(sosConfiguration.getFeatureOfInterestIdentifierPrefix() + sosConfiguration.getFeatureOfInterestName());
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
    public List<String> getFeatureOfInterestIdentifiersForOffering(final String offeringIdentifiers,
            final Session session) throws OwsExceptionReport {
    	final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;

    	if (offeringIdentifiers.equals(sosConfiguration.getOfferingIdentifierPrefix() + sosConfiguration.getOfferingName())) {
    		return Collections.singletonList(sosConfiguration.getFeatureOfInterestIdentifierPrefix() + sosConfiguration.getFeatureOfInterestName()); 
    	}

    	return Collections.emptyList();
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
    public List<FeatureOfInterest> getFeatureOfInterestObject(final Collection<String> identifiers,
            final Session session) {
    	final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;

    	for (final String identifier: identifiers) {
    		if (identifier.equals(sosConfiguration.getFeatureOfInterestIdentifierPrefix() + sosConfiguration.getFeatureOfInterestName())) {
    			final FeatureOfInterest foi = getFeatureOfInterest(identifier, session);
    			return Collections.singletonList(foi);
    		}
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
    	final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;
    	final FeatureOfInterest foi = getFeatureOfInterest(sosConfiguration.getFeatureOfInterestIdentifierPrefix() + sosConfiguration.getFeatureOfInterestName(), session);

    	return Collections.singletonList(foi);
    }

    /**
     * Load FOI identifiers and parent ids for use in the cache. Just loading the ids allows us to not load
     * the geometry columns, XML, etc.
     *
     * @param session
     * @return Map keyed by FOI identifiers, with value collections of parent FOI identifiers if supported
     */
    public Map<String,Collection<String>> getFeatureOfInterestIdentifiersWithParents(final Session session) {
    	final List<String> identifiers = this.getFeatureOfInterestIdentifiers(session);
    	final Collection<String> collection = Collections.<String>emptyList();

    	if (identifiers.size() == 1) {
    		return Collections.singletonMap(identifiers.get(0), collection);
    	}

    	return Collections.emptyMap();
    }

    /**
     * Get all featureOfInterest identifiers
     *
     * @param session
     *            Hibernate session
     * @return FeatureOfInterest identifiers
     */
    public List<String> getFeatureOfInterestIdentifiers(Session session) {
    	final List<FeatureOfInterest> fois = getFeatureOfInterestObjects(session);
    	final List<String> identifiers = new ArrayList<String>(fois.size());

    	for (final FeatureOfInterest foi: fois) {
    		identifiers.add(foi.getIdentifier());
    	}

    	return identifiers;
    }

    /**
     * Insert and/or get featureOfInterest object for identifier
     *
     * @param identifier
     *            FeatureOfInterest identifier
     * @param url
     *            FeatureOfInterest URL, if defined as link
     * @param session
     *            Hibernate session
     * @return FeatureOfInterest object
     */
    public FeatureOfInterest getOrInsertFeatureOfInterest(final String identifier, final String url,
            final Session session) {
        final FeatureOfInterest feature = getFeatureOfInterest(identifier, session);

        if (feature == null) {
        	throw new RuntimeException("Adding features of interest not supported yet.");
        }

        return feature;
    }

    /**
     * Insert featureOfInterest relationship
     *
     * @param parentFeature
     *            Parent featureOfInterest
     * @param childFeature
     *            Child featureOfInterest
     * @param session
     *            Hibernate session
     */
    public void insertFeatureOfInterestRelationShip(final TFeatureOfInterest parentFeature,
            final FeatureOfInterest childFeature, final Session session) {
    	throw new RuntimeException("Insertion of feature of interest relationships not supported yet.");
    }

    /**
     * Insert featureOfInterest/related feature relations if relatedFeatures
     * exists for offering.
     *
     * @param featureOfInterest
     *            FeatureOfInerest
     * @param offering
     *            Offering
     * @param session
     *            Hibernate session
     */
    public void checkOrInsertFeatureOfInterestRelatedFeatureRelation(final FeatureOfInterest featureOfInterest,
            final Offering offering, final Session session) {
        final List<RelatedFeature> relatedFeatures =
                new RelatedFeatureDAO().getRelatedFeatureForOffering(offering.getIdentifier(), session);
        if (CollectionHelper.isNotEmpty(relatedFeatures)) {
            for (final RelatedFeature relatedFeature : relatedFeatures) {
                insertFeatureOfInterestRelationShip((TFeatureOfInterest) relatedFeature.getFeatureOfInterest(),
                        featureOfInterest, session);
            }
        }
    }

    /**
     * Insert featureOfInterest if it is supported
     *
     * @param featureOfInterest
     *            SOS featureOfInterest to insert
     * @param session
     *            Hibernate session
     * @return FeatureOfInterest object
     * @throws NoApplicableCodeException
     *             If SOS feature type is not supported (with status
     *             {@link HTTPStatus}.BAD_REQUEST
     */
    public FeatureOfInterest checkOrInsertFeatureOfInterest(final AbstractFeature featureOfInterest,
            final Session session) throws OwsExceptionReport {
        if (featureOfInterest instanceof SamplingFeature) {
            final String featureIdentifier =
                    Configurator.getInstance().getFeatureQueryHandler()
                            .insertFeature((SamplingFeature) featureOfInterest, session);
            return getOrInsertFeatureOfInterest(featureIdentifier, ((SamplingFeature) featureOfInterest).getUrl(),
                    session);
        } else {
            throw new NoApplicableCodeException().withMessage("The used feature type '%s' is not supported.",
                    featureOfInterest != null ? featureOfInterest.getClass().getName() : featureOfInterest).setStatus(
                    BAD_REQUEST);
        }
    }
}
