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
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.hibernate.Session;
import org.n52.sos.ds.hibernate.entities.FeatureOfInterestType;
import org.n52.sos.ogc.om.features.SfConstants;

import com.google.common.collect.Lists;

/**
 * Hibernate data access class for featureofInterest types
 * 
 * @author CarstenHollmann
 * @since 4.0.0
 */
public class FeatureOfInterestTypeDAO {
    public static final String HZG_FEATURE_OF_INTEREST_TYPE = SfConstants.SAMPLING_FEAT_TYPE_SF_SAMPLING_POINT;

    /**
     * Get all featureOfInterest types
     * 
     * @param session
     *            Hibernate session
     * @return All featureOfInterest types
     */
    public List<String> getFeatureOfInterestTypes(final Session session) {
        return Lists.newArrayList(HZG_FEATURE_OF_INTEREST_TYPE);
    }

    /**
     * Get featureOfInterest type object for featureOfInterest type
     * 
     * @param featureOfInterestType
     *            FeatureOfInterest type
     * @param session
     *            Hibernate session
     * @return FeatureOfInterest type object
     */
    public FeatureOfInterestType getFeatureOfInterestTypeObject(final String featureOfInterestType,
            final Session session) {
        if (!featureOfInterestType.equals(HZG_FEATURE_OF_INTEREST_TYPE)) {
            return null;
        }

        final FeatureOfInterestType foiType = new FeatureOfInterestType();

        foiType.setFeatureOfInterestType(HZG_FEATURE_OF_INTEREST_TYPE);

        return foiType;
    }

    /**
     * Get featureOfInterest type objects for featureOfInterest types
     * 
     * @param featureOfInterestType
     *            FeatureOfInterest types
     * @param session
     *            Hibernate session
     * @return FeatureOfInterest type objects
     */
    public List<FeatureOfInterestType> getFeatureOfInterestTypeObjects(final List<String> featureOfInterestType,
            final Session session) {
        final List<FeatureOfInterestType> foiTypes = Lists.newArrayList();

        for (final String featureOfInterestTypeElement: featureOfInterestType) {
            if (featureOfInterestTypeElement.equals(HZG_FEATURE_OF_INTEREST_TYPE)) {
                foiTypes.add(getFeatureOfInterestTypeObject(HZG_FEATURE_OF_INTEREST_TYPE, session));
            }
        }

        return foiTypes;
    }

    /**
     * Get featureOfInterest type objects for featureOfInterest identifiers
     * 
     * @param featureOfInterestIdentifiers
     *            FeatureOfInterest identifiers
     * @param session
     *            Hibernate session
     * @return FeatureOfInterest type objects
     */
    public List<String> getFeatureOfInterestTypesForFeatureOfInterest(
            final Collection<String> featureOfInterestIdentifiers, final Session session) {
        final List<String> foiTypeStrings = Lists.newArrayList();

        for (final String featureOfInterestIdentifier: featureOfInterestIdentifiers) {
            if (featureOfInterestIdentifier.equals(HZG_FEATURE_OF_INTEREST_TYPE)) {
                foiTypeStrings.add(HZG_FEATURE_OF_INTEREST_TYPE);
            }
        }

        return foiTypeStrings;
    }

    /**
     * Insert and/or get featureOfInterest type object for featureOfInterest
     * type
     * 
     * @param featureType
     *            FeatureOfInterest type
     * @param session
     *            Hibernate session
     * @return FeatureOfInterest type object
     */
    public FeatureOfInterestType getOrInsertFeatureOfInterestType(final String featureType, final Session session) {
        final FeatureOfInterestType foiType = getFeatureOfInterestTypeObject(featureType, session);

        if (foiType == null) {
            throw new RuntimeException("Insertion of feature of interest types is not supported yet.");
        }

        return foiType;
    }

    /**
     * Insert and/or get featureOfInterest type objects for featureOfInterest
     * types
     * 
     * @param featureOfInterestTypes
     *            FeatureOfInterest types
     * @param session
     *            Hibernate session
     * @return FeatureOfInterest type objects
     */
    public List<FeatureOfInterestType> getOrInsertFeatureOfInterestTypes(final Set<String> featureOfInterestTypes,
            final Session session) {
        final List<FeatureOfInterestType> featureTypes = new LinkedList<FeatureOfInterestType>();
        for (final String featureType : featureOfInterestTypes) {
            featureTypes.add(getOrInsertFeatureOfInterestType(featureType, session));
        }
        return featureTypes;
    }

}
