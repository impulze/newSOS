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

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.hibernate.Session;
import org.n52.sos.ds.hibernate.entities.ObservationType;
import org.n52.sos.ogc.om.OmConstants;

import com.google.common.collect.Lists;

/**
 * Hibernate data access class for observation types
 * 
 * @author CarstenHollmann
 * @since 4.0.0
 */
public class ObservationTypeDAO {
    public static final String HZG_OBSERVATION_TYPE = OmConstants.OBS_TYPE_SWE_ARRAY_OBSERVATION;

    /**
     * Get observation type objects for observation types
     * 
     * @param observationTypes
     *            Observation types
     * @param session
     *            Hibernate session
     * @return Observation type objects
     */
    public List<ObservationType> getObservationTypeObjects(List<String> observationTypes, Session session) {
        final List<ObservationType> obsTypes = Lists.newArrayList();

        for (final String observationType: observationTypes) {
            if (observationType.equals(HZG_OBSERVATION_TYPE)) {
                obsTypes.add(getObservationTypeObject(HZG_OBSERVATION_TYPE, session));
            }
        }

        return obsTypes;
    }

    /**
     * Get observation type object for observation type
     * 
     * @param observationType
     * @param session
     *            Hibernate session
     * @return Observation type object
     */
    public ObservationType getObservationTypeObject(String observationType, Session session) {
        if (!observationType.equals(HZG_OBSERVATION_TYPE)) {
            return null;
        }

        final ObservationType obsType = new ObservationType();

        obsType.setObservationType(HZG_OBSERVATION_TYPE);

        return obsType;
    }

    /**
     * Insert or/and get observation type object for observation type
     * 
     * @param observationType
     *            Observation type
     * @param session
     *            Hibernate session
     * @return Observation type object
     */
    public ObservationType getOrInsertObservationType(String observationType, Session session) {
        if (observationType.equals(HZG_OBSERVATION_TYPE)) {
            return getObservationTypeObject(observationType, session);
        }

        throw new RuntimeException("Insertion of observation types is not yet supported.");
    }

    /**
     * Insert or/and get observation type objects for observation types
     * 
     * @param observationTypes
     *            Observation types
     * @param session
     *            Hibernate session
     * @return Observation type objects
     */
    public List<ObservationType> getOrInsertObservationTypes(Set<String> observationTypes, Session session) {
        List<ObservationType> obsTypes = new LinkedList<ObservationType>();
        for (String observationType : observationTypes) {
            obsTypes.add(getOrInsertObservationType(observationType, session));
        }
        return obsTypes;
    }
}
