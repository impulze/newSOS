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

import java.util.Date;
import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.n52.sos.ds.hibernate.entities.ObservableProperty;
import org.n52.sos.ds.hibernate.util.HibernateHelper;
import org.n52.sos.service.SosContextListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import de.hzg.common.SOSConfiguration;
import de.hzg.measurement.ObservedPropertyInstance;
import de.hzg.values.CalculatedData;
import de.hzg.values.RawData;

/**
 * Hibernate data access class for observable properties
 *
 * @author CarstenHollmann
 * @since 4.0.0
 */
public class ObservablePropertyDAO extends AbstractIdentifierNameDescriptionDAO {

    private static final Logger LOGGER = LoggerFactory.getLogger(ObservablePropertyDAO.class);

    public List<ObservedPropertyInstance> getObservedPropertyInstances(Session session) {
    	final Criteria criteria = session.createCriteria(ObservedPropertyInstance.class);
    	@SuppressWarnings("unchecked")
		final List<ObservedPropertyInstance> instances = criteria.list();

    	return instances;
    }

    public ObservableProperty createObservablePropertyWithInstance(ObservedPropertyInstance instance, Session session) {
    	final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;
    	final ObservableProperty obsProp = new ObservableProperty();

    	obsProp.setDisabled(false);
    	obsProp.setIdentifier(sosConfiguration.getObservablePropertyIdentifierPrefix() + instance.getName());

    	return obsProp;
    }

    public class ObservedPropertyInstanceTimeExtrema {
    	public Date minPhenomenonStart;
    	public Date maxPhenomenonEnd;
    };

    public ObservedPropertyInstanceTimeExtrema getValuesExtrema(List<ObservedPropertyInstance> instances, Session session) {
    	final List<Integer> rawInstanceIds = Lists.newArrayList();
    	final List<Integer> calcInstanceIds = Lists.newArrayList();

    	for (final ObservedPropertyInstance instance: instances) {
    		if (instance.getIsRaw()) {
    			rawInstanceIds.add(instance.getId());
    		} else {
    			calcInstanceIds.add(instance.getId());
    		}
    	}

    	final Criteria rawCriteria = session.createCriteria(RawData.class)
    			.add(Restrictions.in("observedPropertyInstance", rawInstanceIds))
    			.setProjection(Projections.projectionList()
    					.add(Projections.min("date"))
    					.add(Projections.max("date")));
    	@SuppressWarnings("unchecked")
		final List<Date[]> rawResults = rawCriteria.list();

    	final Criteria calcCriteria = session.createCriteria(CalculatedData.class)
    			.add(Restrictions.in("observedPropertyInstance", calcInstanceIds))
    			.setProjection(Projections.projectionList()
    					.add(Projections.min("date"))
    					.add(Projections.max("date")));
    	@SuppressWarnings("unchecked")
		final List<Date[]> calcResults = calcCriteria.list();

		final ObservedPropertyInstanceTimeExtrema obite = new ObservedPropertyInstanceTimeExtrema();

		obite.minPhenomenonStart = obite.maxPhenomenonEnd = null;

		if (!rawResults.isEmpty()) {
			for (final Date[] result: rawResults) {
				if (obite.minPhenomenonStart == null || result[0].before(obite.minPhenomenonStart)) {
					obite.minPhenomenonStart = result[0];
				}

				if (obite.maxPhenomenonEnd == null || result[1].after(obite.maxPhenomenonEnd)) {
					obite.maxPhenomenonEnd = result[1];
				}
			}
		}

		if (!calcResults.isEmpty()) {
			for (final Date[] result: calcResults) {
				if (obite.minPhenomenonStart == null || result[0].before(obite.minPhenomenonStart)) {
					obite.minPhenomenonStart = result[0];
				}

				if (obite.maxPhenomenonEnd == null || result[1].after(obite.maxPhenomenonEnd)) {
					obite.maxPhenomenonEnd = result[1];
				}
			}
		}

		if (obite.minPhenomenonStart == null || obite.maxPhenomenonEnd == null) {
			return null;
		}

		return obite;
    }

    /**
     * Get observable property by identifier
     *
     * @param identifier
     *            The observable property's identifier
     * @param session
     *            Hibernate session
     * @return Observable property object
     */
    public ObservableProperty getObservablePropertyForIdentifier(final String identifier, final Session session) {
        Criteria criteria = session.createCriteria(ObservableProperty.class)
                .add(Restrictions.eq(ObservableProperty.IDENTIFIER, identifier));
        LOGGER.debug("QUERY getObservablePropertyForIdentifier(identifier): {}",
                HibernateHelper.getSqlString(criteria));
        return (ObservableProperty) criteria.uniqueResult();
    }

    /**
     * Get all observable property objects
     *
     * @param session
     *            Hibernate session
     * @return Observable property objects
     */
    @SuppressWarnings("unchecked")
    public List<ObservableProperty> getObservablePropertyObjects(final Session session) {
        Criteria criteria = session.createCriteria(ObservableProperty.class);
        LOGGER.debug("QUERY getObservablePropertyObjects(): {}", HibernateHelper.getSqlString(criteria));
        return criteria.list();
    }
}
