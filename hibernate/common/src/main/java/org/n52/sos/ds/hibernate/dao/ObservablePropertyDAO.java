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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;
import org.n52.sos.ds.hibernate.entities.ObservableProperty;
import org.n52.sos.exception.CodedException;
import org.n52.sos.ogc.om.OmObservableProperty;
import org.n52.sos.ogc.ows.OwsExceptionReport;
import org.n52.sos.service.SosContextListener;

import com.google.common.collect.Lists;

import de.hzg.common.SOSConfiguration;
import de.hzg.measurement.ObservedPropertyInstance;
import de.hzg.measurement.Sensor;

/**
 * Hibernate data access class for observable properties
 *
 * @author CarstenHollmann
 * @since 4.0.0
 */
public class ObservablePropertyDAO extends AbstractIdentifierNameDescriptionDAO {
    private static ObservableProperty createObservableProperty(String name, Session session) {
    	final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;
        final ObservableProperty obsProp = new ObservableProperty();

        obsProp.setIdentifier(sosConfiguration.getObservablePropertyIdentifierPrefix() + name);

        return obsProp;
    }

    public static ObservableProperty createObservableProperty(ObservedPropertyInstance observedPropertyInstance, Session session) {
        return createObservableProperty(observedPropertyInstance.getName(), session);
    }

    public List<ObservedPropertyInstance> getObservedPropertyInstancesForIdentifiers(Collection<String> identifiers, Session session) {
    	final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;
        final List<String> names = Lists.newArrayListWithCapacity(identifiers.size());

        for (final String identifier: identifiers) {
            if (identifier.startsWith(sosConfiguration.getObservablePropertyIdentifierPrefix())) {
                names.add(identifier.substring(sosConfiguration.getObservablePropertyIdentifierPrefix().length()));
            }
        }

        final Criteria criteria = session.createCriteria(ObservedPropertyInstance.class)
                .add(Restrictions.in("name", names));
        @SuppressWarnings("unchecked")
		final List<ObservedPropertyInstance> observedPropertyInstances = criteria.list();

        return observedPropertyInstances;
    }

    public List<ObservedPropertyInstance> getObservedPropertyInstances(Session session) {
        final Criteria criteria = session.createCriteria(ObservedPropertyInstance.class);

        @SuppressWarnings("unchecked")
		final List<ObservedPropertyInstance> observedPropertyInstances = criteria.list();

        return observedPropertyInstances;
    }

    /**
     * Get observable property objects for observable property identifiers
     *
     * @param identifiers
     *            Observable property identifiers
     * @param session
     *            Hibernate session
     * @return Observable property objects
     */
    public List<ObservableProperty> getObservableProperties(final List<String> identifiers, final Session session) {
        return getObservablePropertiesForIdentifiers(identifiers, session);
    }

    /**
     * Get observable property identifiers for offering identifier
     *
     * @param offeringIdentifier
     *            Offering identifier
     * @param session
     *            Hibernate session
     * @return Observable property identifiers
     * @throws CodedException
     *             If an error occurs
     */
    @SuppressWarnings("unchecked")
    public List<String> getObservablePropertyIdentifiersForOffering(final String offeringIdentifier,
            final Session session) throws OwsExceptionReport {
    	final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;

        if (!offeringIdentifier.equals(sosConfiguration.getOfferingIdentifierPrefix() + sosConfiguration.getOfferingName())) {
            return Collections.emptyList();
        }

        final Criteria criteria = session.createCriteria(ObservedPropertyInstance.class);
        final List<ObservedPropertyInstance> observedPropertyInstances = criteria.list();
        final List<String> observedPropertyInstanceStrings = Lists.newArrayList();

        for (final ObservedPropertyInstance observedPropertyInstance: observedPropertyInstances) {
            observedPropertyInstanceStrings.add(sosConfiguration.getObservablePropertyIdentifierPrefix() + observedPropertyInstance.getName());
        }

        return observedPropertyInstanceStrings;
    }

    /**
     * Get observable property identifiers for procedure identifier
     *
     * @param procedureIdentifier
     *            Procedure identifier
     * @param session
     *            Hibernate session
     * @return Observable property identifiers
     */
    public List<String> getObservablePropertyIdentifiersForProcedure(final String procedureIdentifier,
            final Session session) {
    	final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;
    	final List<Sensor> sensors = new ProcedureDAO().getSensorsForIdentifiers(Collections.singletonList(procedureIdentifier), session);

        if (sensors == null) {
        	return Collections.emptyList();
        }

        final List<String> observablePropertyIdentifiers = new ArrayList<String>();

        for (final ObservedPropertyInstance observedPropertyInstance: sensors.get(0).getObservedPropertyInstances()) {
        	observablePropertyIdentifiers.add(sosConfiguration.getObservablePropertyIdentifierPrefix() + observedPropertyInstance.getName());
        }

        return observablePropertyIdentifiers;
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
    	final List<ObservableProperty> observableProperties = getObservablePropertiesForIdentifiers(Collections.singletonList(identifier), session);

    	if (observableProperties == null) {
    		return null;
    	}

    	return observableProperties.get(0);
    }

    /**
     * Get observable properties by identifiers
     *
     * @param identifiers
     *            The observable property identifiers
     * @param session
     *            Hibernate session
     * @return Observable property objects
     */
    public List<ObservableProperty> getObservablePropertiesForIdentifiers(final Collection<String> identifiers,
            final Session session) {
        final List<ObservedPropertyInstance> observedPropertyInstances = getObservedPropertyInstancesForIdentifiers(identifiers, session);
        final List<ObservableProperty> observableProperties= Lists.newArrayList();

        for (final ObservedPropertyInstance observedPropertyInstance: observedPropertyInstances) {
            observableProperties.add(createObservableProperty(observedPropertyInstance, session));
        }

        return observableProperties;
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
        final Criteria criteria = session.createCriteria(ObservedPropertyInstance.class);
        final List<ObservedPropertyInstance> observedPropertyInstances = criteria.list();
        final List<ObservableProperty> observableProperties = Lists.newArrayList();

        for (final ObservedPropertyInstance observedPropertyInstance: observedPropertyInstances) {
            observableProperties.add(createObservableProperty(observedPropertyInstance, session));
        }

        return observableProperties;
    }

    /**
     * Insert and/or get observable property objects for SOS observable
     * properties
     *
     * @param observableProperty
     *            SOS observable properties
     * @param session
     *            Hibernate session
     * @return Observable property objects
     */
    public List<ObservableProperty> getOrInsertObservableProperty(final List<OmObservableProperty> observableProperty,
            final Session session) {
        final List<String> identifiers = new ArrayList<String>(observableProperty.size());

        for (final OmObservableProperty sosObservableProperty : observableProperty) {
            identifiers.add(sosObservableProperty.getIdentifierCodeWithAuthority().getValue());
        }

        final List<ObservableProperty> obsProps = getObservableProperties(identifiers, session);

        for (final OmObservableProperty sosObsProp : observableProperty) {
            boolean exists = false;
            for (final ObservableProperty obsProp : obsProps) {
                if (obsProp.getIdentifier().equals(sosObsProp.getIdentifierCodeWithAuthority().getValue())) {
                    exists = true;
                    break;
                }
            }
            if (!exists) {
                throw new RuntimeException("Insertion of observable properties is not supported yet.");
            }
        }

        return obsProps;
    }
}
