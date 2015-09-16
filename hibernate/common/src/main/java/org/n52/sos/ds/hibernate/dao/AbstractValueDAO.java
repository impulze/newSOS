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

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.n52.sos.ds.hibernate.entities.values.AbstractValue;
import org.n52.sos.ogc.filter.TemporalFilter;
import org.n52.sos.ogc.ows.OwsExceptionReport;
import org.n52.sos.request.GetObservationRequest;

/**
 * Abstract DAO class for querying {@link AbstractValue}
 * 
 * @author Carsten Hollmann <c.hollmann@52north.org>
 * @since 4.1.0
 *
 */
public abstract class AbstractValueDAO extends TimeCreator {

    /**
     * Check if a Spatial Filtering Profile filter is requested and add to
     * criteria
     * 
     * @param c
     *            Criteria to add crtierion
     * @param request
     *            GetObservation request
     * @param session
     *            Hiberante Session
     * @throws OwsExceptionReport
     *             If Spatial Filteirng Profile is not supported or an error
     *             occurs.
     */
    protected void checkAndAddSpatialFilteringProfileCriterion(Criteria c, GetObservationRequest request,
            Session session) throws OwsExceptionReport {
        if (request.hasSpatialFilteringProfileSpatialFilter()) {
        	// TODOHZG: add spatial filter
        }
    }

    /**
     * Add chunk information to {@link Criteria}
     * 
     * @param c
     *            {@link Criteria} to add information
     * @param chunkSize
     *            Chunk size
     * @param currentRow
     *            Start row
     * @param request 
     */
    protected void addChunkValuesToCriteria(Criteria c, int chunkSize, int currentRow, GetObservationRequest request) {
    	// TODOHZG: could be either phenomenon time start or result time, for now the order is hardcoded in the criteria anyway
        if (chunkSize > 0) {
            c.setMaxResults(chunkSize).setFirstResult(currentRow);
        }
    }
    
    private enum SortColumn {
    	RESULT_TIME, PHENOMENON_TIME_START
    };

    @SuppressWarnings("unused")
    private SortColumn getOrderColumn(GetObservationRequest request) {
        if (request.isSetTemporalFilter()) {
            TemporalFilter filter = request.getTemporalFilters().iterator().next();
            if (filter.getValueReference().contains(AbstractValue.RESULT_TIME)) {
               return SortColumn.RESULT_TIME;
            }
        }
        return SortColumn.PHENOMENON_TIME_START;
    }
}
