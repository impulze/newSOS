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
package org.n52.sos.ds.hibernate.values.series;

import java.util.Collection;

import org.hibernate.HibernateException;
import org.n52.sos.ds.hibernate.dao.ObservationValueFK;
import org.n52.sos.ds.hibernate.entities.values.AbstractValue;
import org.n52.sos.ds.hibernate.values.HibernateStreamingConfiguration;
import org.n52.sos.exception.CodedException;
import org.n52.sos.exception.ows.NoApplicableCodeException;
import org.n52.sos.ogc.om.OmObservation;
import org.n52.sos.ogc.om.TimeValuePair;
import org.n52.sos.ogc.ows.OwsExceptionReport;
import org.n52.sos.request.GetObservationRequest;
import org.n52.sos.service.ServiceConfiguration;
import org.n52.sos.util.CollectionHelper;
import org.n52.sos.util.http.HTTPStatus;

/**
 * Hibernate series streaming value implementation for chunk results
 * 
 * @author Carsten Hollmann <c.hollmann@52north.org>
 * @since 4.0.2
 *
 */
public class HibernateChunkSeriesStreamingValue<T> extends HibernateSeriesStreamingValue<T> {

    private static final long serialVersionUID = -1990901204421577265L;

    private Collection<T> seriesValuesResult;

    private int chunkSize;

    private int currentRow;

    private boolean noChunk = false;

    private int currentResultSize = 0;
    
    /**
     * constructor
     * 
     * @param request
     *            {@link GetObservationRequest}
     * @param series
     *            Datasource series id
     * @throws CodedException
     */
    public HibernateChunkSeriesStreamingValue(GetObservationRequest request, ObservationValueFK valueFK) throws CodedException {
        super(request, valueFK);
        this.chunkSize = HibernateStreamingConfiguration.getInstance().getChunkSize();
    }

    @Override
    public boolean hasNextValue() throws OwsExceptionReport {
        if (seriesValuesResult == null) {
            if (!noChunk) {
                getNextResults();
                if (chunkSize <= 0 || currentResultSize < chunkSize) {
                    noChunk = true;
                }
            }
        }

        if (seriesValuesResult == null) {
            sessionHolder.returnSession(session);
            return false;
        }

        return true;
    }

    @Override
    public Collection<T> nextEntities() throws OwsExceptionReport {
        final Collection<T> result = seriesValuesResult;
        seriesValuesResult = null;
        return result;
    }

    @Override
    public TimeValuePair nextValue() throws OwsExceptionReport {
        try {
            if (hasNextValue()) {
                final ValueCreator<T> creator = new ValueCreator<T>();
                final Collection<T> nextEntities = nextEntities();
                final AbstractValue abstractValue = creator.createValue(nextEntities);
                final TimeValuePair value = abstractValue.createTimeValuePairFrom();
                for (final T nextEntity: nextEntities) {
                	session.evict(nextEntity);
                }
                return value;
            }
            return null;
        } catch (final HibernateException he) {
            sessionHolder.returnSession(session);
            throw new NoApplicableCodeException().causedBy(he).withMessage("Error while querying observation data!")
                    .setStatus(HTTPStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public OmObservation nextSingleObservation() throws OwsExceptionReport {
        try {
            if (hasNextValue()) {
                OmObservation observation = observationTemplate.cloneTemplate();
                final ValueCreator<T> creator = new ValueCreator<T>();
                final Collection<T> nextEntities = nextEntities();
                final AbstractValue abstractValue = creator.createValue(nextEntities);
                abstractValue.addValuesToObservation(observation, getResponseFormat());
                checkForModifications(observation);
                for (final T nextEntity: nextEntities) {
                	session.evict(nextEntity);
                }
                return observation;
            }
            return null;
        } catch (final HibernateException he) {
            sessionHolder.returnSession(session);
            throw new NoApplicableCodeException().causedBy(he).withMessage("Error while querying observation data!")
                    .setStatus(HTTPStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Get the next results from database
     * 
     * @throws OwsExceptionReport
     *             If an error occurs when querying the next results
     */
    private void getNextResults() throws OwsExceptionReport {
        if (session == null) {
            session = sessionHolder.getSession();
        }
        try {
            Collection<T> seriesValuesResult = null;
                seriesValuesResult =
                        seriesValueDAO.getStreamingSeriesValuesFor(request, valueFK, temporalFilterCriterion,
                                chunkSize, currentRow, session);
            currentRow += chunkSize;
            checkMaxNumberOfReturnedValues(seriesValuesResult.size());
            setSeriesValuesResult(seriesValuesResult);
        } catch (final HibernateException he) {
            sessionHolder.returnSession(session);
            throw new NoApplicableCodeException().causedBy(he).withMessage("Error while querying observation data!")
                    .setStatus(HTTPStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Check the queried {@link AbstractValue}s for null and set them as
     * iterator to local variable.
     * 
     * @param seriesValuesResult
     *            Queried {@link AbstractValue}s
     */
    private void setSeriesValuesResult(Collection<T> seriesValuesResult) {
        if (CollectionHelper.isNotEmpty(seriesValuesResult)) {
            this.currentResultSize = seriesValuesResult.size();
            this.seriesValuesResult = seriesValuesResult;
        }

    }

}
