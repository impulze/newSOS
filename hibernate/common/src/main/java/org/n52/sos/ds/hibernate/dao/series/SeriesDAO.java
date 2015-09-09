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
import java.util.List;

import org.hibernate.Session;
import org.n52.sos.ds.hibernate.dao.FeatureOfInterestDAO;
import org.n52.sos.ds.hibernate.dao.ObservablePropertyDAO;
import org.n52.sos.ds.hibernate.dao.ProcedureDAO;
import org.n52.sos.ds.hibernate.dao.UnitDAO;
import org.n52.sos.ds.hibernate.entities.FeatureOfInterest;
import org.n52.sos.ds.hibernate.entities.series.Series;
import org.n52.sos.service.SosContextListener;

import de.hzg.common.SOSConfiguration;
import de.hzg.measurement.ObservedPropertyInstance;
import de.hzg.measurement.Sensor;

/**
 * Hibernate data access class for series
 * 
 * @since 4.0.0
 * 
 */
public class SeriesDAO extends AbstractSeriesDAO {
    private Series getSeriesFromInstance(ObservedPropertyInstance observedPropertyInstance, Session session) {
    	final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;
		final Series series = new Series();

		series.setProcedure(ProcedureDAO.createTProcedure(observedPropertyInstance.getSensor(), session));
		series.setFeatureOfInterest(new FeatureOfInterestDAO().getFeatureOfInterest(sosConfiguration.getFeatureOfInterestIdentifierPrefix() + sosConfiguration.getFeatureOfInterestName(), session));
		series.setObservableProperty(ObservablePropertyDAO.createObservableProperty(observedPropertyInstance,  session));
		series.setDeleted(false);
		series.setPublished(true);

		if (!observedPropertyInstance.getIsRaw()) {
			series.setUnit(new UnitDAO().getUnitFromObservedPropertyInstance(observedPropertyInstance));
		}

    	return series;
    }

    @Override
    public List<Series> getSeries(Collection<String> procedures, Collection<String> observedProperties,
            Collection<String> features, Session session) {
    	// make sure query is applicable to our database model
    	// for now quit on null, unsure how to handle it (depends on call)
    	if (procedures == null || observedProperties == null || features == null) {
    		throw new RuntimeException("Parameters to getSeries cannot be null for now: " + procedures + observedProperties + features);
    	}

    	final List<ObservedPropertyInstance> allObservedPropertyInstances = new ArrayList<ObservedPropertyInstance>();
   		final List<ObservedPropertyInstance> observedPropertyInstances = new ObservablePropertyDAO().getObservedPropertyInstancesForIdentifiers(observedProperties, session);

   		if (observedPropertyInstances != null) {
   			allObservedPropertyInstances.addAll(observedPropertyInstances);
    	}

   		final List<Sensor> sensors = new ProcedureDAO().getSensorsForIdentifiers(procedures, session);

   		if (sensors != null) {
   			for (final Sensor sensor: sensors) {
   				allObservedPropertyInstances.addAll(sensor.getObservedPropertyInstances());
  			}
    	}

   		final List<FeatureOfInterest> fois = new FeatureOfInterestDAO().getFeatureOfInterestObject(features, session);

   		if (fois == null || fois.isEmpty()) {
   			allObservedPropertyInstances.clear();
   		}

   		final List<Series> series = new ArrayList<Series>();

   		for (final ObservedPropertyInstance observedPropertyInstance: allObservedPropertyInstances) {
   			series.add(getSeriesFromInstance(observedPropertyInstance, session));
   		}

   		return series;
    }

    @Override
    protected Class <?>getSeriesClass() {
        return Series.class;
    }
}
