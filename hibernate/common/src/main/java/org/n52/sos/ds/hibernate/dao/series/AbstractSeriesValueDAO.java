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
package org.n52.sos.ds.hibernate.dao.series;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.opengis.swe.x101.DataArrayPropertyType;

import org.hibernate.Criteria;
import org.hibernate.HibernateException;
import org.hibernate.ScrollMode;
import org.hibernate.ScrollableResults;
import org.hibernate.Session;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Restrictions;
import org.n52.sos.ds.hibernate.dao.AbstractValueDAO;
import org.n52.sos.ds.hibernate.dao.ObservationValueFK;
import org.n52.sos.ds.hibernate.entities.Unit;
import org.n52.sos.ds.hibernate.entities.series.values.SeriesValue;
import org.n52.sos.ds.hibernate.entities.values.AbstractValue;
import org.n52.sos.ds.hibernate.util.TimeCriterion;
import org.n52.sos.ds.hibernate.HZGEReportingValue;
import org.n52.sos.ds.hibernate.ScrollableConverter;
import org.n52.sos.ogc.om.values.SweDataArrayValue;
import org.n52.sos.ogc.ows.OwsExceptionReport;
import org.n52.sos.ogc.sos.Sos2Constants;
import org.n52.sos.ogc.sos.SosConstants.HelperValues;
import org.n52.sos.ogc.swe.SweAbstractDataComponent;
import org.n52.sos.ogc.swe.SweAbstractDataRecord;
import org.n52.sos.ogc.swe.SweConstants;
import org.n52.sos.ogc.swe.SweDataArray;
import org.n52.sos.ogc.swe.SweDataRecord;
import org.n52.sos.ogc.swe.SweField;
import org.n52.sos.ogc.swe.SweSimpleDataRecord;
import org.n52.sos.ogc.swe.encoding.SweTextEncoding;
import org.n52.sos.ogc.swe.simpleType.SweCount;
import org.n52.sos.ogc.swe.simpleType.SweQuantity;
import org.n52.sos.request.GetObservationRequest;
import org.n52.sos.util.CodingHelper;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

import de.hzg.values.ValueData;

/**
 * Abstract value data access object class for {@link SeriesValue}
 * 
 * @author Carsten Hollmann <c.hollmann@52north.org>
 * @since 4.3.0
 *
 */
public abstract class AbstractSeriesValueDAO extends AbstractValueDAO {
    protected abstract Class<?> getSeriesValueClass();

    /**
     * Query streaming value for parameter as {@link ScrollableResults}
     * 
     * @param request
     *            {@link GetObservationRequest}
     * @param series
     *            Datasource series id
     * @param temporalFilterCriterion
     *            Temporal filter {@link Criterion}
     * @param session
     *            Hibernate Session
     * @return Resulting {@link ScrollableResults}
     * @throws HibernateException
     *             If an error occurs when querying the {@link AbstractValue}s
     * @throws OwsExceptionReport
     *             If an error occurs when querying the {@link AbstractValue}s
     */
    public ScrollableResults getStreamingSeriesValuesFor(GetObservationRequest request, ObservationValueFK valueFK,
    		Map<String, Collection<TimeCriterion>> temporalFilterDisjunctions, Session session) throws OwsExceptionReport {
        return getSeriesValueCriteriaFor(request, valueFK, temporalFilterDisjunctions, session).scroll(
                ScrollMode.FORWARD_ONLY);
    }

    /**
     * Query streaming value for parameter as chunk {@link List}
     * 
     * @param request
     *            {@link GetObservationRequest}
     * @param series
     *            Datasource series id
     * @param temporalFilterCriterion
     *            Temporal filter {@link Criterion}
     * @param chunkSize
     *            chunk size
     * @param currentRow
     *            Start row
     * @param session
     *            Hibernate Session
     * @return Resulting chunk {@link List}
     * @throws OwsExceptionReport
     *             If an error occurs when querying the {@link AbstractValue}s
     */
    @SuppressWarnings("unchecked")
    public List<AbstractValue> getStreamingSeriesValuesFor(GetObservationRequest request, ObservationValueFK valueFK,
    		Map<String, Collection<TimeCriterion>> temporalFilterDisjunctions, int chunkSize, int currentRow, Session session)
            throws OwsExceptionReport {
        Criteria c = getSeriesValueCriteriaFor(request, valueFK, temporalFilterDisjunctions, session);
        // TODOHZG: always order by date
        c.addOrder(Order.asc("date"));
        if (chunkSize > 0) {
            c.setMaxResults(chunkSize).setFirstResult(currentRow);
        }
        final List<ValueData<?>> valueDataList = c.list();
        final List<AbstractValue> valueList = Lists.transform(valueDataList, new Function<ValueData<?>, AbstractValue>() {
			@Override
			public AbstractValue apply(ValueData<?> valueData) {
				return createValueFromValueData(valueData);
			}
        });

        return valueList;
    }

    /**
     * Get {@link Criteria} for parameter
     * 
     * @param request
     *            {@link GetObservationRequest}
     * @param series
     *            Datasource series id
     * @param temporalFilterCriterion
     *            Temporal filter {@link Criterion}
     * @param session
     *            Hibernate Session
     * @return Resulting {@link Criteria}
     * @throws OwsExceptionReport
     *             If an error occurs when adding Spatial Filtering Profile
     *             restrictions
     */
    private Criteria getSeriesValueCriteriaFor(GetObservationRequest request, ObservationValueFK valueFK,
    		Map<String, Collection<TimeCriterion>> temporalFilterDisjunctions, Session session) throws OwsExceptionReport {
    	final Criteria criteria = session.createCriteria(valueFK.getValueClass())
    		.add(Restrictions.eq("observedPropertyInstance", valueFK.getObservedPropertyInstance()));

        //checkAndAddSpatialFilteringProfileCriterion(c, request, session);

        // TODOHZG: is offerings of request relevant here if we filter by observed property instance?
        // TODOHZG: filter series by temporal filters

        return criteria.setReadOnly(true);
    }

    /**
     * Query unit for parameter
     * 
     * @param request
     *            {@link GetObservationRequest}
     * @param series
     *            Datasource series id
     * @param session
     *            Hibernate Session
     * @return Unit or null if no unit is set
     * @throws OwsExceptionReport
     *             If an error occurs when querying the unit
     */
    static int now = 0;
    public String getUnit(GetObservationRequest request, ObservationValueFK valueFK, Session session) throws OwsExceptionReport {
    	// TODOHZG: get unit from first observation value
        Criteria c = getSeriesValueCriteriaFor(request, valueFK, null, session);
        final ValueData<?> valueData = (ValueData<?>) c.setMaxResults(1).uniqueResult();
        final HZGEReportingValue value = createValueFromValueData(valueData);
        final Unit unit = value == null ? null : value.getUnit();

        if (unit != null && unit.isSetUnit()) {
            return unit.getUnit();
        }
        return null;
    }

	public HZGEReportingValue createValueFromValueData(ValueData<?> valueData) {
		final HZGEReportingValue value = new HZGEReportingValue();

		value.setDeleted(false);
		value.setPhenomenonTimeStart(valueData.getDate());
		final SweDataArray dataArray = new SweDataArray();
		dataArray.setElementCount(new SweCount().setValue(1));
		final SweTextEncoding encoding = new SweTextEncoding();
		encoding.setBlockSeparator(";");
		encoding.setDecimalSeparator(".");
		encoding.setTokenSeparator(",");
		dataArray.setEncoding(encoding);
		final SweAbstractDataRecord dataRecord = new SweDataRecord();
		if (valueData.getValue() == null) {
			dataRecord.addField(new SweField("average", new SweQuantity()));
			dataRecord.addField(new SweField("max", new SweQuantity()));
			dataRecord.addField(new SweField("min", new SweQuantity()));
			dataRecord.addField(new SweField("median", new SweQuantity()));
			dataRecord.addField(new SweField("stddev", new SweQuantity()));
		} else {
			dataRecord.addField(new SweField("value", new SweQuantity()));
		}
		dataArray.setElementType(dataRecord);
		final List<List<String>> valuesList = new ArrayList<List<String>>();
		final List<String> values = new ArrayList<String>();
		if (valueData.getValue() == null) {
			values.add(valueData.getAverage().toString());
			values.add(valueData.getMax().toString());
			values.add(valueData.getMin().toString());
			values.add(valueData.getMedian().toString());
			values.add(valueData.getStddev().toString());
		} else {
			values.add(valueData.getValue().toString());
		}
		valuesList.add(values);
		dataArray.setValues(valuesList);
		final Map<HelperValues, String> additionalValues = new HashMap<HelperValues, String>();
		additionalValues.put(HelperValues.FOR_OBSERVATION, null);

		try {
			value.setValue(CodingHelper.encodeObjectToXmlText(SweConstants.NS_SWE_20, dataArray, additionalValues));
		} catch (OwsExceptionReport e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//value.setValue("<xml xmlns:swe=\"http://www.opengis.net/swe/2.0\"><swe:DataArray><swe:elementCount><swe:Count><swe:value>1</swe:value></swe:Count></swe:elementCount><swe:elementType name=\"components\"><swe:SimpleDataRecord><swe:field name=\"value\"><swe:Quantity></swe:Quantity></swe:field></swe:SimpleDataRecord></swe:elementType><swe:encoding><swe:TextBlock decimalSeparator=\".\" tokenSeparator=\",\" blockSeparator=\";\"/></swe:encoding><swe:values /></swe:DataArray></xml>");
		
		value.setSessionObject(valueData);

		return value;
	}

	public ScrollableConverter<?, ?> getScrollableConverter() {
		final ScrollableConverter<ValueData<?>, HZGEReportingValue> converter;

		converter = new ScrollableConverter<ValueData<?>, HZGEReportingValue>() {
			@Override
			public HZGEReportingValue convert(ValueData<?> valueData) {
				return createValueFromValueData(valueData); 
			}
			
		};

		return converter;
	}
}
