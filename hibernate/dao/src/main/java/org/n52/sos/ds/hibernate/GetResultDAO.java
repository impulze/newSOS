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
package org.n52.sos.ds.hibernate;

import java.util.Set;

import org.hibernate.Session;
import org.n52.sos.ds.AbstractGetResultDAO;
import org.n52.sos.ds.HibernateDatasourceConstants;
import org.n52.sos.ogc.ows.OwsExceptionReport;
import org.n52.sos.ogc.sos.ConformanceClasses;
import org.n52.sos.ogc.sos.SosConstants;
import org.n52.sos.request.GetResultRequest;
import org.n52.sos.response.GetResultResponse;
import org.n52.sos.service.ServiceConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

/**
 * Implementation of the abstract class AbstractGetResultDAO
 * 
 * @since 4.0.0
 * 
 */
public class GetResultDAO extends AbstractGetResultDAO {

    private static final Logger LOGGER = LoggerFactory.getLogger(GetResultDAO.class);

    private final HibernateSessionHolder sessionHolder = new HibernateSessionHolder();

    /**
     * constructor
     */
    public GetResultDAO() {
        super(SosConstants.SOS);
    }
    
    @Override
    public String getDatasourceDaoIdentifier() {
        return HibernateDatasourceConstants.ORM_DATASOURCE_DAO_IDENTIFIER;
    }

    @Override
    public GetResultResponse getResult(final GetResultRequest request) throws OwsExceptionReport {
    	throw new RuntimeException("getResult not supported yet.");
    }

    @Override
    public Set<String> getConformanceClasses() {
        try {
            Session session = sessionHolder.getSession();
            if (ServiceConfiguration.getInstance().isStrictSpatialFilteringProfile()) {
                return Sets.newHashSet(ConformanceClasses.SOS_V2_SPATIAL_FILTERING_PROFILE);
            }
            sessionHolder.returnSession(session);
        } catch (OwsExceptionReport owse) {
            LOGGER.error("Error while getting Spatial Filtering Profile conformance class!", owse);
        }
        return super.getConformanceClasses();
    }
}
