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

import java.util.Collections;
import java.util.List;

import org.hibernate.Session;
import org.n52.sos.ds.hibernate.entities.RelatedFeature;
import org.n52.sos.ds.hibernate.entities.RelatedFeatureRole;
import org.n52.sos.ogc.gml.AbstractFeature;
import org.n52.sos.ogc.ows.OwsExceptionReport;

/**
 * Hibernate data access class for related features
 * 
 * @author CarstenHollmann
 * @since 4.0.0
 */
public class RelatedFeatureDAO {
    /**
     * Get related feature objects for offering identifier
     * 
     * @param offering
     *            Offering identifier
     * @param session
     *            Hibernate session
     * @return Related feature objects
     */
    public List<RelatedFeature> getRelatedFeatureForOffering(final String offering, final Session session) {
        return Collections.emptyList();
    }

    /**
     * Get all related feature objects
     * 
     * @param session
     *            Hibernate session
     * @return Related feature objects
     */
    public List<RelatedFeature> getRelatedFeatureObjects(final Session session) {
        return Collections.emptyList();
    }

    /**
     * Get related feature objects for target identifier
     * 
     * @param targetIdentifier
     *            Target identifier
     * @param session
     *            Hibernate session
     * @return Related feature objects
     */
    public List<RelatedFeature> getRelatedFeatures(final String targetIdentifier, final Session session) {
        // TODOHZG: do we have those?
        return Collections.emptyList();
    }

    /**
     * Insert and get related feature objects.
     * 
     * @param feature
     *            Related feature
     * @param roles
     *            Related feature role objects
     * @param session
     *            Hibernate session
     * @return Related feature objects
     * @throws OwsExceptionReport
     *             If an error occurs
     */
    public List<RelatedFeature> getOrInsertRelatedFeature(final AbstractFeature feature, final List<RelatedFeatureRole> roles,
            final Session session) throws OwsExceptionReport {
        // TODO: create featureOfInterest and link to relatedFeature
        List<RelatedFeature> relFeats = getRelatedFeatures(feature.getIdentifierCodeWithAuthority().getValue(), session);
        if (relFeats != null) {
            return relFeats;
        }

        throw new RuntimeException("Insertion of related features is not supported yet.");
    }
}
