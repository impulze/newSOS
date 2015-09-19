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
package org.n52.sos.ds.hibernate.entities.series.values;

import org.hibernate.Session;
import org.n52.sos.ds.hibernate.entities.series.Series;
import org.n52.sos.ds.hibernate.entities.series.HibernateSeriesRelations.HasSeries;
import org.n52.sos.ds.hibernate.entities.values.AbstractValue;
import org.n52.sos.ogc.om.AbstractObservationValue;
import org.n52.sos.ogc.om.MultiObservationValues;
import org.n52.sos.ogc.om.OmConstants;
import org.n52.sos.ogc.om.OmObservation;
import org.n52.sos.ogc.om.SingleObservationValue;
import org.n52.sos.ogc.om.values.MultiValue;
import org.n52.sos.ogc.om.values.Value;
import org.n52.sos.ogc.ows.OwsExceptionReport;
import org.n52.sos.ogc.swes.SwesExtensions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link AbstractValue} for series concept used in streaming
 * datasource
 * 
 * @author Carsten Hollmann <c.hollmann@52north.org>
 * @since 4.1.0
 *
 */
public class SeriesValue extends AbstractValue implements HasSeries {

    private static final long serialVersionUID = -2757686338936995366L;

    private Series series;

    @Override
    public Series getSeries() {
        return series;
    }

    @Override
    public void setSeries(Series series) {
        this.series = series;
    }

    @Override
    public boolean isSetSeries() {
        return getSeries() != null;
    }
    static Logger LOG = LoggerFactory.getLogger(SeriesValue.class);
    @Override
    public OmObservation mergeValueToObservation(OmObservation observation, String responseFormat) throws OwsExceptionReport {
        if (!observation.isSetValue()) {
        	LOG.info("initial merge");
            addValuesToObservation(observation, responseFormat);
        } else {
            // TODO
            if (!OmConstants.OBS_TYPE_SWE_ARRAY_OBSERVATION.equals(observation.getObservationConstellation()
                    .getObservationType())) {
            	LOG.info("set array type");
                observation.getObservationConstellation().setObservationType(
                        OmConstants.OBS_TYPE_SWE_ARRAY_OBSERVATION);
            }
            LOG.info("next merge");
            observation.mergeWithObservation(getObservationValue(getValueFrom(this)));
        }
        return observation;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private AbstractObservationValue<?> getObservationValue(Value<?> value) throws OwsExceptionReport {
    	/*
    	LOG.info("setting single observation value: " + value +  " // " + value.getClass());
    	if (value instanceof MultiValue) {
    		final MultiValue multiValue = (MultiValue)value;
    		final MultiObservationValues multiObservation = new MultiObservationValues();

    		//multiObservation.setPhenomenonTime();
    		multiObservation.setValue(multiValue);

    		return multiObservation;
    	}
    	*/
        return new SingleObservationValue(createPhenomenonTime(), value);
    }

    @Override
    protected void addValueSpecificDataToObservation(OmObservation observation, String responseFormat) throws OwsExceptionReport {
        // nothing to do
    }

    @Override
    public void addValueSpecificDataToObservation(OmObservation observation, Session session, SwesExtensions swesExtensions)
            throws OwsExceptionReport {
        // nothing to do
        
    }

    @Override
    protected void addObservationValueToObservation(OmObservation observation, Value<?> value, String responseFormat)
            throws OwsExceptionReport {
        observation.setValue(getObservationValue(value));
    }

    @Override
    public String getDiscriminator() {
        return null;
    }
}
