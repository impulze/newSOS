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

import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.n52.sos.ds.hibernate.entities.ObservableProperty;
import org.n52.sos.ds.hibernate.util.HibernateHelper;
import org.n52.sos.service.SosContextListener;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import de.hzg.common.SOSConfiguration;
import de.hzg.measurement.ObservedPropertyInstance;

/**
 * Hibernate data access class for observable properties
 *
 * @author CarstenHollmann
 * @since 4.0.0
 */
public class ObservablePropertyDAO extends AbstractIdentifierNameDescriptionDAO {

    private static final Logger LOGGER = LoggerFactory.getLogger(ObservablePropertyDAO.class);

    public List<ObservedPropertyInstance> getObservedPropertyInstances(Iterable<String> identifiers, Session session) {
    	final Criteria criteria = session.createCriteria(ObservedPropertyInstance.class);

    	if (identifiers != null && !Iterables.isEmpty(identifiers)) {
    		final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;
    		final Iterable<String> names = Iterables.transform(identifiers, new Function<String, String>() {
				public String apply(String identifier) {
					if (identifier.startsWith(sosConfiguration.getObservablePropertyIdentifierPrefix())) {
						return identifier.substring(sosConfiguration.getObservablePropertyIdentifierPrefix().length());
					}

					return identifier;
				}
    			
    		});

    		criteria.add(Restrictions.in("name", Lists.newArrayList(names)));
    	}

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