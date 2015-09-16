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

import org.hibernate.Session;
import org.n52.sos.ds.hibernate.dao.DaoFactory;
import org.n52.sos.ds.hibernate.dao.ObservablePropertyDAO;
import org.n52.sos.ds.hibernate.dao.ObservablePropertyDAO.ObservedPropertyInstanceTimeExtrema;
import org.n52.sos.ds.hibernate.dao.series.AbstractSeriesValueDAO;
import org.n52.sos.ds.hibernate.dao.series.AbstractSeriesValueTimeDAO;
import org.n52.sos.ds.hibernate.entities.series.ValueFK;
import org.n52.sos.ds.hibernate.values.AbstractHibernateStreamingValue;
import org.n52.sos.exception.CodedException;
import org.n52.sos.ogc.gml.time.TimeInstant;
import org.n52.sos.ogc.ows.OwsExceptionReport;
import org.n52.sos.request.GetObservationRequest;
import org.n52.sos.util.DateTimeHelper;
import org.n52.sos.util.GmlHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Abstract Hibernate series streaming value class for the series concept
 * 
 * @author Carsten Hollmann <c.hollmann@52north.org>
 * @since 4.0.2
 *
 */
public abstract class HibernateSeriesStreamingValue extends AbstractHibernateStreamingValue {

    private static final Logger LOGGER = LoggerFactory.getLogger(HibernateSeriesStreamingValue.class);

    private static final long serialVersionUID = 201732114914686926L;

    protected final AbstractSeriesValueDAO seriesValueDAO;

    protected final AbstractSeriesValueTimeDAO seriesValueTimeDAO;

    protected ValueFK valueFK;

    /**
     * constructor
     * 
     * @param request
     *            {@link GetObservationRequest}
     * @param series
     *            Datasource series id
     * @throws CodedException
     */
    public HibernateSeriesStreamingValue(GetObservationRequest request, ValueFK valueFK) throws CodedException {
        super(request);
        this.valueFK = valueFK;
        this.seriesValueDAO = (AbstractSeriesValueDAO) DaoFactory.getInstance().getValueDAO();
        this.seriesValueTimeDAO = (AbstractSeriesValueTimeDAO) DaoFactory.getInstance().getValueTimeDAO();
    }

    @Override
    protected void queryTimes() {
        Session s = null;
        try {
            s = sessionHolder.getSession();
            final ObservedPropertyInstanceTimeExtrema opite = new ObservablePropertyDAO().getValuesExtrema(Lists.newArrayList(valueFK.getObservedPropertyInstance()), session);

            // TODOHZG: which times to set?
            setPhenomenonTime(GmlHelper.createTime(DateTimeHelper.makeDateTime(opite.minPhenomenonStart), DateTimeHelper.makeDateTime(opite.maxPhenomenonEnd)));
            LOGGER.info("i've set phen time: " + getPhenomenonTime());
            setResultTime(new TimeInstant(opite.maxPhenomenonEnd));
        } catch (OwsExceptionReport owse) {
            LOGGER.error("Error while querying times", owse);
        } finally {
            sessionHolder.returnSession(s);
        }
    }

    @Override
    protected void queryUnit() {
        Session s = null;
        try {
           s = sessionHolder.getSession();
            setUnit(seriesValueDAO.getUnit(request, valueFK, s));
        } catch (OwsExceptionReport owse) {
            LOGGER.error("Error while querying unit", owse);
        } finally {
            sessionHolder.returnSession(s);
        }
    }

}
