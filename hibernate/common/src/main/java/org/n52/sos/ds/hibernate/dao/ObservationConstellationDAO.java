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

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;
import org.n52.sos.ds.hibernate.entities.ObservableProperty;
import org.n52.sos.ds.hibernate.entities.ObservationConstellation;
import org.n52.sos.ds.hibernate.entities.Offering;
import org.n52.sos.ds.hibernate.entities.Procedure;
import org.n52.sos.ds.hibernate.util.ObservationConstellationInfo;
import org.n52.sos.exception.ows.InvalidParameterValueException;
import org.n52.sos.ogc.om.OmObservationConstellation;
import org.n52.sos.ogc.ows.OwsExceptionReport;
import org.n52.sos.ogc.sos.Sos2Constants;
import org.n52.sos.service.SosContextListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import de.hzg.common.SOSConfiguration;
import de.hzg.measurement.ObservedPropertyInstance;
import de.hzg.values.CalculatedData;

/**
 * Hibernate data access class for observation constellation
 * 
 * @author CarstenHollmann
 * @since 4.0.0
 */
public class ObservationConstellationDAO {

    private static final Logger LOGGER = LoggerFactory.getLogger(ObservationConstellationDAO.class);

    private static ObservationConstellation createObservationConstellation(ObservedPropertyInstance observedPropertyInstance, Session session) {
        final ObservationConstellation obsConst = new ObservationConstellation();
        final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;
        final Offering offering = new OfferingDAO().getOfferingForIdentifier(sosConfiguration.getOfferingIdentifierPrefix() + sosConfiguration.getOfferingName(), session);

        obsConst.setObservableProperty(ObservablePropertyDAO.createObservableProperty(observedPropertyInstance, session));
        obsConst.setProcedure(ProcedureDAO.createTProcedure(observedPropertyInstance.getSensor(), session));
        obsConst.setOffering(offering);
        obsConst.setObservationType(new ObservationTypeDAO().getObservationTypeObject(ObservationTypeDAO.HZG_OBSERVATION_TYPE, session));
        obsConst.setDeleted(false);
        obsConst.setDisabled(false);
        obsConst.setHiddenChild(false);

        return obsConst;
    }

    /**
     * Get observation constellation objects for procedure and observable
     * property object and offering identifiers
     * 
     * @param procedure
     *            Procedure object
     * @param observableProperty
     *            Observable property object
     * @param offerings
     *            Offering identifiers
     * @param session
     *            Hibernate session
     * @return Observation constellation objects
     */
    public List<ObservationConstellation> getObservationConstellation(Procedure procedure,
            ObservableProperty observableProperty, Collection<String> offerings, Session session) {
        final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;

        for (final String offering: offerings) {
            if (offering.equals(sosConfiguration.getOfferingIdentifierPrefix() + sosConfiguration.getOfferingName())) {
                return getObservationConstellations(procedure.getIdentifier(), observableProperty.getIdentifier(), session);
            }
        }

        return Collections.emptyList();
    }

    /**
     * Get ObservationConstellations for procedure, observableProperty and
     * offerings
     * 
     * @param procedure
     *            Procedure to get ObservaitonConstellation for
     * @param observableProperty
     *            observableProperty to get ObservaitonConstellation for
     * @param offerings
     *            Offerings to get ObservaitonConstellation for
     * @param session
     *            Hibernate session
     * @return ObservationConstellations
     */
    public List<ObservationConstellation> getObservationConstellationsForOfferings(Procedure procedure,
            ObservableProperty observableProperty, Collection<Offering> offerings, Session session) {
        final List<String> offeringIdentifiers = Lists.newArrayListWithCapacity(offerings.size());

        for (final Offering offering: offerings) {
            offeringIdentifiers.add(offering.getIdentifier());
        }

        return getObservationConstellation(procedure, observableProperty, offeringIdentifiers, session);
    }

    /**
     * Get first ObservationConstellation for procedure, observableProperty and
     * offerings
     * 
     * @param p
     *            Procedure to get ObservaitonConstellation for
     * @param op
     *            ObservedProperty to get ObservaitonConstellation for
     * @param o
     *            Offerings to get ObservaitonConstellation for
     * @param session
     *            Hibernate session
     * @return First ObservationConstellation
     */
    public ObservationConstellation getFirstObservationConstellationForOfferings(Procedure p, ObservableProperty op,
            Collection<Offering> o, Session session) {
        final List<ObservationConstellation> oc = getObservationConstellationsForOfferings(p, op, o, session);
        return oc.isEmpty() ? null : oc.get(0);
    }

    /**
     * Get ObservationConstellations for procedure and observableProperty
     * 
     * @param procedure
     *            Procedure to get ObservaitonConstellation for
     * @param observableProperty
     *            observableProperty to get ObservaitonConstellation for
     * @param session
     *            Hibernate session
     * @return ObservationConstellations
     */
    @SuppressWarnings("unchecked")
    public List<ObservationConstellation> getObservationConstellations(String procedure, String observableProperty,
            Session session) {
    	final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;

        if (!procedure.startsWith(sosConfiguration.getProcedureIdentifierPrefix())) {
            return Collections.emptyList();
        }

        if (!observableProperty.startsWith(sosConfiguration.getObservablePropertyIdentifierPrefix())) {
        	return Collections.emptyList();
        }

        final Criteria criteria = session.createCriteria(ObservedPropertyInstance.class);
        final Criteria sensorCriteria = criteria.createCriteria("sensor");
        final String instanceName = observableProperty.substring(sosConfiguration.getObservablePropertyIdentifierPrefix().length());
        final String sensorName = procedure.substring(sosConfiguration.getProcedureIdentifierPrefix().length());

        criteria.add(Restrictions.eq("name", instanceName));
        sensorCriteria.add(Restrictions.eq("name", sensorName));

        final List<ObservedPropertyInstance> results = criteria.list();
        final List<ObservationConstellation> obsConsts = Lists.newArrayListWithCapacity(results.size());

        for (final ObservedPropertyInstance observedPropertyInstance: results) {
            obsConsts.add(createObservationConstellation(observedPropertyInstance, session));
        }

        return obsConsts;
    }

    /**
     * Get all observation constellation objects
     * 
     * @param session
     *            Hibernate session
     * @return Observation constellation objects
     */
    public List<ObservationConstellation> getObservationConstellations(Session session) {
    	// TODO: only add if they have calculated/raw data assigned
    	final List<ObservedPropertyInstance> observedPropertyInstances = new ObservablePropertyDAO().getObservedPropertyInstances(session);
        final List<ObservationConstellation> obsConsts = Lists.newArrayListWithCapacity(observedPropertyInstances.size());
        final List<ObservedPropertyInstance> observedPropertyInstancesAdded = Lists.newArrayListWithCapacity(observedPropertyInstances.size());

        for (final ObservedPropertyInstance observedPropertyInstance: observedPropertyInstances) {
            if (!observedPropertyInstancesAdded.contains(observedPropertyInstance)) {
                // TODOHZG: projections? how to add?
                obsConsts.add(createObservationConstellation(observedPropertyInstance, session));
                observedPropertyInstancesAdded.add(observedPropertyInstance);
            }
        }

        return obsConsts;
    }

    /**
     * Get info for all observation constellation objects
     * 
     * @param session
     *            Hibernate session
     * @return Observation constellation info objects
     */
    public List<ObservationConstellationInfo> getObservationConstellationInfo(Session session) {
        final List<ObservationConstellation> obsConsts = getObservationConstellations(session);
        final List<ObservationConstellationInfo> obsConstInfos = Lists.newArrayListWithCapacity(obsConsts.size());

        for (final ObservationConstellation obsConst: obsConsts) {
            final ObservationConstellationInfo obsConstInfo = new ObservationConstellationInfo();

            obsConstInfo.setOffering(obsConst.getOffering().getIdentifier());
            obsConstInfo.setProcedure(obsConst.getProcedure().getIdentifier());
            obsConstInfo.setObservableProperty(obsConst.getObservableProperty().getIdentifier());
            obsConstInfo.setObservationType(obsConst.getObservationType().getObservationType());
            obsConstInfo.setHiddenChild(false);

            obsConstInfos.add(obsConstInfo);
        }

        return obsConstInfos;
    }    
    
    /**
     * Insert or update and get observation constellation for procedure,
     * observable property and offering
     * 
     * @param procedure
     *            Procedure object
     * @param observableProperty
     *            Observable property object
     * @param offering
     *            Offering object
     * @param hiddenChild
     *            Is observation constellation hidden child
     * @param session
     *            Hibernate session
     * @return Observation constellation object
     */
    public ObservationConstellation checkOrInsertObservationConstellation(Procedure procedure,
            ObservableProperty observableProperty, Offering offering, boolean hiddenChild, Session session) {
        final List<String> offerings = Lists.newArrayList(offering.getIdentifier());
        final List<ObservationConstellation> obsConsts = getObservationConstellation(procedure, observableProperty, offerings, session);

        if (!obsConsts.isEmpty()) {
            return obsConsts.get(0);
        }

        throw new RuntimeException("Insertion of observation constellations is not supported yet.");
    }

    /**
     * Check and Update and/or get observation constellation objects
     * 
     * @param sosObservationConstellation
     *            SOS observation constellation
     * @param offering
     *            Offering identifier
     * @param session
     *            Hibernate session
     * @param parameterName
     *            Parameter name for exception
     * @return Observation constellation object
     * @throws OwsExceptionReport
     *             If the requested observation type is invalid
     */
    public ObservationConstellation checkObservationConstellation(
            OmObservationConstellation sosObservationConstellation, String offering, Session session,
            String parameterName) throws OwsExceptionReport {
        final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;

        if (!offering.equals(sosConfiguration.getOfferingIdentifierPrefix() + sosConfiguration.getOfferingName())) {
            return null;
        }

        final String observableProperty = sosObservationConstellation.getObservableProperty().getIdentifier();
        final String procedure = sosObservationConstellation.getProcedure().getIdentifier();

        if (!observableProperty.startsWith(sosConfiguration.getObservablePropertyIdentifierPrefix())) {
            return null;
        }

        if (!procedure.startsWith(sosConfiguration.getProcedureIdentifierPrefix())) {
            return null;
        }

        final String instanceName = observableProperty.substring(sosConfiguration.getObservablePropertyIdentifierPrefix().length());
        final String sensorName = procedure.substring(sosConfiguration.getProcedureIdentifierPrefix().length());

        final Criteria criteria = session.createCriteria(ObservedPropertyInstance.class)
        	.add(Restrictions.eq("name", instanceName));
        final Criteria sensorCriteria = criteria.createCriteria("sensor")
        	.add(Restrictions.eq("name", sensorName));

        @SuppressWarnings("unchecked")
		final List<ObservedPropertyInstance> results = criteria.list();

        if (results.isEmpty()) {
            throw new InvalidParameterValueException()
            .at(Sos2Constants.InsertObservationParams.observation)
            .withMessage(
                    "The requested observation constellation (procedure=%s, observedProperty=%s and offering=%s) is invalid!",
                    procedure, observableProperty, sosObservationConstellation.getOfferings());
        }

        final List<ObservedPropertyInstance> observedPropertyInstancesAdded = Lists.newArrayListWithCapacity(results.size());

        
        for (final ObservedPropertyInstance observedPropertyInstance: results) {
        	final ObservationConstellation obsConst = createObservationConstellation(observedPropertyInstance, session);

            if (observedPropertyInstancesAdded.contains(observedPropertyInstance)) {
                continue;
            }

            observedPropertyInstancesAdded.add(observedPropertyInstance);

            if (obsConst.getObservationType().getObservationType()
                    .equals(sosObservationConstellation.getObservationType())) {
                return obsConst;
            } else {
                throw new InvalidParameterValueException()
                        .at(parameterName)
                        .withMessage(
                                "The requested observationType (%s) is invalid for procedure = %s, observedProperty = %s and offering = %s! The valid observationType is '%s'!",
                                sosObservationConstellation.getObservationType(), procedure,
                                observableProperty, sosObservationConstellation.getOfferings(),
                                obsConst.getObservationType().getObservationType());
            }
        }

        return null;
    }

    /**
     * Update ObservationConstellation for procedure and set deleted flag
     * 
     * @param procedure
     *            Procedure for which the ObservationConstellations should be
     *            changed
     * @param deleteFlag
     *            New deleted flag value
     * @param session
     *            Hibernate session
     */
    public void updateObservatioConstellationSetAsDeletedForProcedure(String procedure, boolean deleteFlag,
            Session session) {
        throw new RuntimeException("Setting the deleted flag on observation constellations isn't supported yet.");
    }

    /**
     * Get ObservationCollection entities for procedures, observableProperties
     * and offerings where observationType is not null;
     * 
     * @param procedures
     *            Procedures to get ObservationCollection entities for
     * @param observedProperties
     *            ObservableProperties to get ObservationCollection entities for
     * @param offerings
     *            Offerings to get ObservationCollection entities for
     * @param session
     *            Hibernate session
     * @return Resulting ObservationCollection entities
     */
    public List<ObservationConstellation> getObservationConstellations(List<String> procedures,
            List<String> observedProperties, List<String> offerings, Session session) {
        final List<ObservationConstellation> obsConsts = getObservationConstellations(session);
        final Iterator<ObservationConstellation> iterator = obsConsts.iterator();

        while (iterator.hasNext()) {
            final ObservationConstellation obsConst = iterator.next();

            if (!offerings.contains(obsConst.getOffering().getIdentifier())) {
                iterator.remove();
            } else if (!procedures.contains(obsConst.getProcedure().getIdentifier())) {
                iterator.remove();
            } else if (!observedProperties.contains(obsConst.getObservableProperty().getIdentifier())) {
                iterator.remove();
            }
        }

        return obsConsts;
    }
    
    protected Set<ObservationConstellation> getObservationConstellations(Session session, Procedure procedure) {
        final List<ObservationConstellation> list = getObservationConstellations(procedure.getIdentifier(), procedure.getIdentifier(), session);

        return Sets.newHashSet(list);
    }    
}
