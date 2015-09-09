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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.Criteria;
import org.hibernate.ScrollableResults;
import org.hibernate.ScrollMode;
import org.hibernate.Session;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Restrictions;
import org.n52.sos.ds.hibernate.dao.FeatureOfInterestDAO;
import org.n52.sos.ds.hibernate.dao.ObservablePropertyDAO;
import org.n52.sos.ds.hibernate.dao.OfferingDAO;
import org.n52.sos.ds.hibernate.dao.ProcedureDAO;
import org.n52.sos.ds.hibernate.dao.UnitDAO;
import org.n52.sos.ds.hibernate.entities.Offering;
import org.n52.sos.ds.hibernate.entities.Unit;
import org.n52.sos.ds.hibernate.entities.series.Series;
import org.n52.sos.ds.hibernate.entities.series.SeriesBlobObservation;
import org.n52.sos.ds.hibernate.entities.series.SeriesBooleanObservation;
import org.n52.sos.ds.hibernate.entities.series.SeriesCategoryObservation;
import org.n52.sos.ds.hibernate.entities.series.SeriesCountObservation;
import org.n52.sos.ds.hibernate.entities.series.SeriesGeometryObservation;
import org.n52.sos.ds.hibernate.entities.series.SeriesNumericObservation;
import org.n52.sos.ds.hibernate.entities.series.SeriesObservation;
import org.n52.sos.ds.hibernate.entities.series.SeriesObservationInfo;
import org.n52.sos.ds.hibernate.entities.series.SeriesObservationTime;
import org.n52.sos.ds.hibernate.entities.series.SeriesSweDataArrayObservation;
import org.n52.sos.ds.hibernate.entities.series.SeriesTextObservation;
import org.n52.sos.exception.sos.ResponseExceedsSizeLimitException;
import org.n52.sos.ogc.ows.OwsExceptionReport;
import org.n52.sos.ogc.sos.SosConstants.SosIndeterminateTime;
import org.n52.sos.ogc.swe.SweDataArray;
import org.n52.sos.ogc.swe.SweDataRecord;
import org.n52.sos.ogc.swe.SweField;
import org.n52.sos.ogc.swe.encoding.SweTextEncoding;
import org.n52.sos.ogc.swe.simpleType.SweCount;
import org.n52.sos.ogc.swe.simpleType.SweQuantity;
import org.n52.sos.request.GetObservationRequest;
import org.n52.sos.service.ServiceConfiguration;
import org.n52.sos.service.SosContextListener;
import org.n52.sos.util.CollectionHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.hzg.common.SOSConfiguration;
import de.hzg.measurement.ObservedPropertyInstance;
import de.hzg.values.CalculatedData;
import de.hzg.values.RawData;
import de.hzg.values.ValueData;

/**
 * Hibernate data access class for series observations
 * 
 * @since 4.0.0
 * 
 */
public class SeriesObservationDAO extends AbstractSeriesObservationDAO {
	private Logger LOGGER = LoggerFactory.getLogger(SeriesObservationDAO.class);

    /**
     * Query series observations for GetObservation request and features
     * 
     * @param request
     *            GetObservation request
     * @param features
     *            Collection of feature identifiers resolved from the request
     * @param session
     *            Hibernate session
     * @return Series observations that fit
     * @throws OwsExceptionReport
     */
    public List<SeriesObservation> getSeriesObservationsFor(GetObservationRequest request,
            Collection<String> features, Session session) throws OwsExceptionReport {
        return getSeriesObservationsFor(request, features, null, null, session);
    }

    /**
     * Query series observations for GetObservation request, features, and a
     * filter criterion (typically a temporal filter)
     * 
     * @param request
     *            GetObservation request
     * @param features
     *            Collection of feature identifiers resolved from the request
     * @param filterCriterion
     *            Criterion to apply to criteria query (typically a temporal
     *            filter)
     * @param session
     *            Hibernate session
     * @return Series observations that fit
     * @throws OwsExceptionReport
     */
    public List<SeriesObservation> getSeriesObservationsFor(GetObservationRequest request,
            Collection<String> features, Criterion filterCriterion, Session session) throws OwsExceptionReport {
        return getSeriesObservationsFor(request, features, filterCriterion, null, session);
    }

    /**
     * Query series observations for GetObservation request, features, and an
     * indeterminate time (first/latest)
     * 
     * @param request
     *            GetObservation request
     * @param features
     *            Collection of feature identifiers resolved from the request
     * @param sosIndeterminateTime
     *            Indeterminate time to use in a temporal filter (first/latest)
     * @param session
     *            Hibernate session
     * @return Series observations that fit
     * @throws OwsExceptionReport
     */
    public List<SeriesObservation> getSeriesObservationsFor(GetObservationRequest request,
            Collection<String> features, SosIndeterminateTime sosIndeterminateTime, Session session)
            throws OwsExceptionReport {
        return getSeriesObservationsFor(request, features, null, sosIndeterminateTime, session);
    }

    private List<SeriesObservation> getSeriesObservationsFor(GetObservationRequest request, Collection<String> features,
    		Collection<String> offerings, Collection<String> procedures, Collection<String> observableProperties,
            Criterion filterCriterion, SosIndeterminateTime sosIndeterminateTime, Session session) throws OwsExceptionReport {
    	/* spatial filter for results not supported yet
        if (request.hasSpatialFilteringProfileSpatialFilter()) {
            c.add(SpatialRestrictions.filter(
                    AbstractObservation.SAMPLING_GEOMETRY,
                    request.getSpatialFilter().getOperator(),
                    GeometryHandler.getInstance().switchCoordinateAxisFromToDatasourceIfNeeded(
                            request.getSpatialFilter().getGeometry())));
        }*/

    	final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;
    	final List<String> procedureIdentifiers = new ArrayList<String>();
    	final List<String> observablePropertyIdentifiers = new ArrayList<String>();

    	if (CollectionHelper.isNotEmpty(features)) {
    		boolean found = false;

    		for (final String identifier: features) {
    			if (identifier.equals(sosConfiguration.getFeatureOfInterestIdentifierPrefix() + sosConfiguration.getFeatureOfInterestName())) {
    				found = true;
    				break;
    			}
    		}

    		if (!found) {
    			return null;
    		}
    	}

    	if (CollectionHelper.isNotEmpty(request.getOfferings())) {
    		boolean found = false;

    		for (final String identifier: request.getOfferings()) {
    			if (identifier.equals(sosConfiguration.getOfferingIdentifierPrefix() + sosConfiguration.getOfferingName())) {
    				found = true;
    				 break;
    			}
    		}

    		if (!found) {
    			return null;
    		}
    	}

    	if (CollectionHelper.isNotEmpty(request.getProcedures())) {
    		for (final String identifier: request.getProcedures()) {
    			if (identifier.startsWith(sosConfiguration.getProcedureIdentifierPrefix())) {
    				procedureIdentifiers.add(identifier.substring(sosConfiguration.getProcedureIdentifierPrefix().length()));
    			}
    		}
    	}

    	if (CollectionHelper.isNotEmpty(request.getObservedProperties())) {
    		for (final String identifier: request.getObservedProperties()) {
    			if (identifier.startsWith(sosConfiguration.getObservablePropertyIdentifierPrefix())) {
    				observablePropertyIdentifiers.add(identifier.substring(sosConfiguration.getObservablePropertyIdentifierPrefix().length()));
    			}
    		}
    	}

    	final Criteria calculatedCriteria = session.createCriteria(CalculatedData.class);
    	final Criteria calculatedOPICriteria = calculatedCriteria.createCriteria("observedPropertyInstance");
    	final Criteria rawCriteria = session.createCriteria(RawData.class);
    	final Criteria rawOPICriteria = rawCriteria.createCriteria("observedPropertyInstance");

    	if (!procedureIdentifiers.isEmpty()) {
    		calculatedOPICriteria.createCriteria("sensor").add(Restrictions.in("name", procedureIdentifiers));
    		rawOPICriteria.createCriteria("sensor").add(Restrictions.in("name", procedureIdentifiers));
    	}

    	if (!observablePropertyIdentifiers.isEmpty()) {
    		calculatedOPICriteria.add(Restrictions.in("name", observablePropertyIdentifiers));
    		rawOPICriteria.add(Restrictions.in("name", observablePropertyIdentifiers));
    	}

    	if (filterCriterion != null) {
    		calculatedCriteria.add(filterCriterion);
    		rawCriteria.add(filterCriterion);
    	}

    	if (sosIndeterminateTime != null) {
    		//TODO:
    		//addIndeterminateTimeRestriction(calculatedCriteria, sosIndeterminateTime);
    		//addIndeterminateTimeRestriction(rawCriteria, sosIndeterminateTime);
    	}

	calculatedCriteria.setReadOnly(true).setCacheable(false);
	rawCriteria.setReadOnly(true).setCacheable(false);

       @SuppressWarnings("unchecked")
       List<CalculatedData> calculatedDataList;
       @SuppressWarnings("unchecked")
       List<RawData> rawDataList;

       try {
               rawDataList = rawCriteria.list();
               calculatedDataList = calculatedCriteria.list();
       } catch (OutOfMemoryError error) {
           throw new ResponseExceedsSizeLimitException().withMessage(
                    "The observation response is to big for the maximal heap size of %d Byte of the "
                            + "virtual machine! Please either refine your getObservation request to reduce the "
                            + "number of observations in the response or ask the administrator of this SOS to "
                            + "increase the maximum heap size of the virtual machine!", Runtime.getRuntime().maxMemory());
       }

       final List<SeriesObservation> seriesObservations = new ArrayList<SeriesObservation>();
 
       if (calculatedDataList.isEmpty() && rawDataList.isEmpty()) {
           return seriesObservations;
       }

       final Offering offering = new OfferingDAO().getOfferingForIdentifier(sosConfiguration.getOfferingIdentifierPrefix() + sosConfiguration.getOfferingName(), session);

       final Map<ObservedPropertyInstance, List<ValueData<? extends Number>>> valueMap = new HashMap<ObservedPropertyInstance, List<ValueData<? extends Number>>>();

       for (final RawData rawData: rawDataList) {
           final ValueData<? extends Number> value = rawData;
           List<ValueData<? extends Number>> thisValueDataList = valueMap.get(value.getObservedPropertyInstance());

           if (thisValueDataList == null) {
               thisValueDataList = new ArrayList<ValueData<? extends Number>>();
               valueMap.put(value.getObservedPropertyInstance(), thisValueDataList);
           }

           thisValueDataList.add(value);
           session.evict(value);
       }

       for (final CalculatedData calculatedData: calculatedDataList) {
           final ValueData<? extends Number> value = calculatedData;
           List<ValueData<? extends Number>> thisValueDataList = valueMap.get(value.getObservedPropertyInstance());

           if (thisValueDataList == null) {
               thisValueDataList = new ArrayList<ValueData<? extends Number>>();
               valueMap.put(value.getObservedPropertyInstance(), thisValueDataList);
           }

           thisValueDataList.add(value);
           session.evict(value);
      }

       for (final Map.Entry<ObservedPropertyInstance, List<ValueData<? extends Number>>> vdEntry: valueMap.entrySet()) {
           final ObservedPropertyInstance observedPropertyInstance = vdEntry.getKey();
           final List<ValueData<? extends Number>> values = vdEntry.getValue();
           final SeriesSweDataArrayObservation seriesObservation = new SeriesSweDataArrayObservation();
           final Unit unit = new UnitDAO().getUnitFromObservedPropertyInstance(observedPropertyInstance);
           final Date lastResultTime = values.get(values.size() - 1).getDate();
           final Date firstPhenomenonTime = values.get(0).getDate();

           seriesObservation.setPhenomenonTimeStart(firstPhenomenonTime);
           seriesObservation.setPhenomenonTimeEnd(lastResultTime);
           seriesObservation.setResultTime(lastResultTime);
           seriesObservation.setUnit(unit);
           seriesObservation.setDeleted(false);
           seriesObservation.setOfferings(Collections.singleton(offering));

           final Series series = new Series();

           series.setProcedure(ProcedureDAO.createTProcedure(observedPropertyInstance.getSensor(), session));
           series.setFeatureOfInterest(new FeatureOfInterestDAO().getFeatureOfInterest(sosConfiguration.getFeatureOfInterestIdentifierPrefix() + sosConfiguration.getFeatureOfInterestName(), session));
           series.setObservableProperty(ObservablePropertyDAO.createObservableProperty(observedPropertyInstance, session));
           series.setPublished(true);

           final SweQuantity sweQuantity = new SweQuantity();

           if (seriesObservation.getUnit() != null) {
               series.setUnit(seriesObservation.getUnit());
               sweQuantity.setUom(seriesObservation.getUnit().getUnit());
           }

           seriesObservation.setSeries(series);

           final SweDataArray sweDataArray = new SweDataArray();
           final SweDataRecord sweDataRecord = new SweDataRecord();
           final SweTextEncoding sweTextEncoding = new SweTextEncoding();
           final String tupleSeparator = ServiceConfiguration.getInstance().getTupleSeparator();
           final String tokenSeparator = ServiceConfiguration.getInstance().getTokenSeparator();
           final String decimalSeparator = ServiceConfiguration.getInstance().getDecimalSeparator();

           sweTextEncoding.setBlockSeparator(tupleSeparator);
           sweTextEncoding.setTokenSeparator(tokenSeparator);
           sweTextEncoding.setDecimalSeparator(decimalSeparator);
           sweDataArray.setEncoding(sweTextEncoding);
           sweDataArray.setElementType(sweDataRecord);

           if (observedPropertyInstance.getUseInterval()) {
               sweDataRecord.addField(new SweField("average", sweQuantity));
               sweDataRecord.addField(new SweField("min", sweQuantity));
               sweDataRecord.addField(new SweField("max", sweQuantity));
               sweDataRecord.addField(new SweField("median", sweQuantity));
               sweDataRecord.addField(new SweField("stddev", sweQuantity));
           } else {
               sweDataRecord.addField(new SweField("value", sweQuantity));
           }

           for (final ValueData<? extends Number> value: values) {
               final List<String> list;

               if (value.getObservedPropertyInstance().getUseInterval()) {
                   list = new ArrayList<String>(5);

                   list.add(value.getAverage().toString());
                   list.add(value.getMin().toString());
                   list.add(value.getMax().toString());
                   list.add(value.getMedian().toString());
                   list.add(value.getStddev().toString());
               } else {
                   list = Collections.singletonList(value.getValue().toString());
               }

               sweDataArray.add(list);
           }

           seriesObservation.setValue(sweDataArray);
           seriesObservations.add(seriesObservation);
       }

       return seriesObservations;
    }

    /**
     * Query series observations for GetObservation request, features, and
     * filter criterion (typically a temporal filter) or an indeterminate time
     * (first/latest). This method is private and accepts all possible arguments
     * for request-based getSeriesObservationFor. Other public methods overload
     * this method with sensible combinations of arguments.
     * 
     * @param request
     *            GetObservation request
     * @param features
     *            Collection of feature identifiers resolved from the request
     * @param filterCriterion
     *            Criterion to apply to criteria query (typically a temporal
     *            filter)
     * @param sosIndeterminateTime
     *            Indeterminate time to use in a temporal filter (first/latest)
     * @param session
     * @return Series observations that fit
     * @throws OwsExceptionReport
     */
    protected List<SeriesObservation> getSeriesObservationsFor(GetObservationRequest request, Collection<String> features,
            Criterion filterCriterion, SosIndeterminateTime sosIndeterminateTime, Session session) throws OwsExceptionReport {
    	// TODO: check in called function if AND or OR must be used
    	return getSeriesObservationsFor(request, features, request.getOfferings(), request.getProcedures(), request.getObservedProperties(), filterCriterion, sosIndeterminateTime, session);
    }

    public List<SeriesObservation> getSeriesObservationsFor(Series series, GetObservationRequest request,
            SosIndeterminateTime sosIndeterminateTime, Session session) throws OwsExceptionReport {
    	// TODO: check in called function if AND or OR must be used
    	return getSeriesObservationsFor(request,
    			Collections.singletonList(series.getFeatureOfInterest().getIdentifier()),
    			request.getOfferings(),
    			Collections.singletonList(series.getProcedure().getIdentifier()),
    			Collections.singletonList(series.getObservableProperty().getIdentifier()),
    			null, sosIndeterminateTime, session);
    }

    @Override
    protected Class<?> getObservationClass() {
        return SeriesObservation.class;
    }

    @Override
    protected Class<?> getObservationTimeClass() {
        return SeriesObservationTime.class;
    }

    @Override
    protected Class<?> getObservationInfoClass() {
        return SeriesObservationInfo.class;
    }

    @Override
    protected Class<?> getBlobObservationClass() {
        return SeriesBlobObservation.class;
    }

    @Override
    protected Class<?> getBooleanObservationClass() {
        return SeriesBooleanObservation.class;
    }

    @Override
    protected Class<?> getCategoryObservationClass() {
        return SeriesCategoryObservation.class;
    }

    @Override
    protected Class<?> getCountObservationClass() {
        return SeriesCountObservation.class;
    }

    @Override
    protected Class<?> getGeometryObservationClass() {
        return SeriesGeometryObservation.class;
    }

    @Override
    protected Class<?> getNumericObservationClass() {
        return SeriesNumericObservation.class;
    }

    @Override
    protected Class<?> getSweDataArrayObservationClass() {
        return SeriesSweDataArrayObservation.class;
    }

    @Override
    protected Class<?> getTextObservationClass() {
        return SeriesTextObservation.class;
    }

}
