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
package org.n52.sos.ds.hibernate.dao.ereporting;

import org.hibernate.Session;
import org.n52.sos.aqd.AqdConstants;
import org.n52.sos.ds.hibernate.dao.AbstractIdentifierNameDescriptionDAO;
import org.n52.sos.ds.hibernate.entities.ereporting.EReportingSamplingPoint;

/**
 * DAO class for entity {@link EReportingSamplingPoint}
 * 
 * @author Carsten Hollmann <c.hollmann@52north.org>
 * @since 4.3.0
 *
 */
public class EReportingSamplingPointDAO extends AbstractIdentifierNameDescriptionDAO {
    /**
     * Get the {@link EReportingSamplingPoint} for the id
     * 
     * @param samplingPointId
     *            Id to get {@link EReportingSamplingPoint} for
     * @param session
     *            Hibernate session
     * @return The resulting {@link EReportingSamplingPoint}
     */
    public EReportingSamplingPoint getEReportingSamplingPoint(long samplingPointId, Session session) {
    	throw new RuntimeException("Obtaining AQD sampling points by samplingPointId not supported.");
    }

    /**
     * Get the {@link EReportingSamplingPoint} for the identifier
     * 
     * @param identifier
     *            Identifier to get {@link EReportingSamplingPoint} for
     * @param session
     *            Hibernate session
     * @return The resulting {@link EReportingSamplingPoint}
     */
    public EReportingSamplingPoint getEReportingSamplingPoint(String identifier, Session session) {
    	if (!identifier.equals("urn:aqd_sampling_points:sampling_point_1")) {
    		throw new RuntimeException("Only AQD sampling point 'urn:aqd_sampling_points:sampling_point_1 supported.");
    	}

    	final EReportingSamplingPoint eReportingSamplingPoint = new EReportingSamplingPoint();

    	eReportingSamplingPoint.setIdentifier(identifier);
    	eReportingSamplingPoint.setAssessmentType(new EReportingAssessmentTypeDAO().getEReportingAssessmentType(AqdConstants.AssessmentType.Fixed, session));

        return eReportingSamplingPoint;
    }
}