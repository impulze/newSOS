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

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.apache.xmlbeans.XmlObject;
import org.hibernate.HibernateException;
import org.hibernate.ScrollableResults;
import org.n52.sos.ds.hibernate.entities.ereporting.values.EReportingSweDataArrayValue;
import org.n52.sos.ds.hibernate.entities.series.ValueFK;
import org.n52.sos.exception.CodedException;
import org.n52.sos.exception.ows.NoApplicableCodeException;
import org.n52.sos.ogc.om.OmObservation;
import org.n52.sos.ogc.om.TimeValuePair;
import org.n52.sos.ogc.ows.OwsExceptionReport;
import org.n52.sos.ogc.sos.SosConstants.HelperValues;
import org.n52.sos.ogc.swe.SweConstants;
import org.n52.sos.ogc.swe.SweDataArray;
import org.n52.sos.ogc.swe.SweDataRecord;
import org.n52.sos.ogc.swe.SweField;
import org.n52.sos.ogc.swe.encoding.SweTextEncoding;
import org.n52.sos.ogc.swe.simpleType.SweCount;
import org.n52.sos.ogc.swe.simpleType.SweText;
import org.n52.sos.request.GetObservationRequest;
import org.n52.sos.service.ServiceConfiguration;
import org.n52.sos.util.CodingHelper;
import org.n52.sos.util.http.HTTPStatus;

import de.hzg.values.ValueData;

/**
 * Hibernate series streaming value implementation for {@link ScrollableResults}
 * 
 * @author Carsten Hollmann <c.hollmann@52north.org>
 * @since 4.0.2
 *
 */
public class HibernateScrollableSeriesStreamingValue extends HibernateSeriesStreamingValue {

    private static final long serialVersionUID = -6439122088572009613L;

    private ScrollableResults scrollableResult;

    /**
     * constructor
     * 
     * @param request
     *            {@link GetObservationRequest}
     * @param series
     *            Datasource series id
     * @throws CodedException 
     */
    public HibernateScrollableSeriesStreamingValue(GetObservationRequest request, ValueFK valueFK) throws CodedException {
        super(request, valueFK);
    }

    @Override
    public boolean hasNextValue() throws OwsExceptionReport {
        boolean next = false;
        if (scrollableResult == null) {
            getNextResults();
            if (scrollableResult != null) {
                next = scrollableResult.next();
            }
        } else {
            next = scrollableResult.next();
        }
        if (!next) {
            sessionHolder.returnSession(session);
        }
        return next;
    }

    public class WrappedEReportingSweDataArrayValue extends EReportingSweDataArrayValue {
		private static final long serialVersionUID = 319420289447689667L;
		private ValueData<?> valueData;

		WrappedEReportingSweDataArrayValue(ValueData<?> valueData) {
			this.valueData = valueData;

			final String blockSeparator = ServiceConfiguration.getInstance().getTupleSeparator();
			final String tokenSeparator = ServiceConfiguration.getInstance().getTokenSeparator();
			final String decimalSeparator = ServiceConfiguration.getInstance().getDecimalSeparator();
			final SweTextEncoding encoding = new SweTextEncoding();
			final List<List<String>> valuesList = new ArrayList<List<String>>();
			final List<String> values = new ArrayList<String>();
			final SweDataRecord dataRecord = new SweDataRecord();

			encoding.setBlockSeparator(blockSeparator);
			encoding.setTokenSeparator(tokenSeparator);
			encoding.setDecimalSeparator(decimalSeparator);
			values.add(valueData.getValue() != null ? valueData.getValue().toString() : valueData.getAverage().toString());
			valuesList.add(values);
			dataRecord.addField(new SweField("name", new SweText().setValue("monk").setDefinition("yes")));

			final SweDataArray array = new SweDataArray();

			array.setElementCount(new SweCount().setValue(1));
			array.setElementType(dataRecord);
			array.setEncoding(encoding);
			array.setValues(valuesList);

			try {
	            final Map<HelperValues, String> additionalValues = new EnumMap<HelperValues, String>(HelperValues.class);

	            additionalValues.put(HelperValues.FOR_OBSERVATION, null);

				final XmlObject xml = CodingHelper.encodeObjectToXml(SweConstants.NS_SWE_20, array, additionalValues);

				setValue(xml.xmlText());
			} catch (OwsExceptionReport e) {
				e.printStackTrace();
			}

			this.setPhenomenonTimeStart(valueData.getDate());
			this.setResultTime(valueData.getDate());
		}

		public ValueData<?> getValueData() {
			return valueData;
		}
    };

    @Override
    public WrappedEReportingSweDataArrayValue nextEntity() throws OwsExceptionReport {
        checkMaxNumberOfReturnedValues(1);
        final ValueData<?> realValue = (ValueData<?>)scrollableResult.get()[0];
        final WrappedEReportingSweDataArrayValue workValue = new WrappedEReportingSweDataArrayValue(realValue);

        return workValue;
    }

    @Override
    public TimeValuePair nextValue() throws OwsExceptionReport {
        try {
        	final WrappedEReportingSweDataArrayValue resultObject = nextEntity();
        	TimeValuePair value = resultObject.createTimeValuePairFrom();
            session.evict(resultObject.getValueData());
            return value;
        } catch (final HibernateException he) {
            sessionHolder.returnSession(session);
            throw new NoApplicableCodeException().causedBy(he).withMessage("Error while querying observation data!")
                    .setStatus(HTTPStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public OmObservation nextSingleObservation() throws OwsExceptionReport {
        try {
            OmObservation observation = observationTemplate.cloneTemplate();
            WrappedEReportingSweDataArrayValue resultObject = nextEntity();
            resultObject.addValuesToObservation(observation, getResponseFormat());
            checkForModifications(observation);
            session.evict(resultObject.getValueData());
            return observation;
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
            // query with temporal filter
            if (temporalFilterDisjunctions != null) {
                setScrollableResult(seriesValueDAO.getStreamingSeriesValuesFor(request, valueFK,
                        temporalFilterDisjunctions, session));
            }
            // query without temporal or indeterminate filters
            else {
                setScrollableResult(seriesValueDAO.getStreamingSeriesValuesFor(request, valueFK, session));
            }
        } catch (final HibernateException he) {
            sessionHolder.returnSession(session);
            throw new NoApplicableCodeException().causedBy(he).withMessage("Error while querying observation data!")
                    .setStatus(HTTPStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Set the queried {@link ScrollableResults} to local variable
     * 
     * @param scrollableResult
     *            Queried {@link ScrollableResults}
     */
    private void setScrollableResult(ScrollableResults scrollableResult) {
        this.scrollableResult = scrollableResult;
    }

}
