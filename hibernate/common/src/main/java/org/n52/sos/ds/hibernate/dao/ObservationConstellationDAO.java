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
import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;
import org.n52.sos.ds.hibernate.entities.ObservableProperty;
import org.n52.sos.ds.hibernate.entities.ObservationConstellation;
import org.n52.sos.ds.hibernate.entities.Offering;
import org.n52.sos.ds.hibernate.entities.Procedure;
import org.n52.sos.ds.hibernate.util.HibernateHelper;
import org.n52.sos.ds.hibernate.util.ObservationConstellationInfo;
import org.n52.sos.service.SosContextListener;
import org.n52.sos.util.CollectionHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import de.hzg.common.SOSConfiguration;
import de.hzg.measurement.ObservedPropertyInstance;

/**
 * Hibernate data access class for observation constellation
 * 
 * @author CarstenHollmann
 * @since 4.0.0
 */
public class ObservationConstellationDAO {

    private static final Logger LOGGER = LoggerFactory.getLogger(ObservationConstellationDAO.class);

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
    @SuppressWarnings("unchecked")
    public List<ObservationConstellation> getObservationConstellationsForOfferings(Procedure procedure,
            ObservableProperty observableProperty, Collection<Offering> offerings, Session session) {
        return session.createCriteria(ObservationConstellation.class)
                .add(Restrictions.eq(ObservationConstellation.DELETED, false))
                .add(Restrictions.eq(ObservationConstellation.PROCEDURE, procedure))
                .add(Restrictions.in(ObservationConstellation.OFFERING, offerings))
                .add(Restrictions.eq(ObservationConstellation.OBSERVABLE_PROPERTY, observableProperty)).list();
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
        Criteria criteria =
                session.createCriteria(ObservationConstellation.class)
                        .setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY)
                        .add(Restrictions.eq(ObservationConstellation.DELETED, false));
        criteria.createCriteria(ObservationConstellation.PROCEDURE).add(
                Restrictions.eq(Procedure.IDENTIFIER, procedure));
        criteria.createCriteria(ObservationConstellation.OBSERVABLE_PROPERTY).add(
                Restrictions.eq(ObservableProperty.IDENTIFIER, observableProperty));
        LOGGER.debug("QUERY getObservationConstellation(procedure, observableProperty): {}",
                HibernateHelper.getSqlString(criteria));
        return criteria.list();
    }

    /**
     * Get all observation constellation objects
     * 
     * @param session
     *            Hibernate session
     * @return Observation constellation objects
     */
    public List<ObservationConstellation> getObservationConstellations(Session session) {
    	final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;
    	final List<ObservedPropertyInstance> instances = new ObservablePropertyDAO().getObservedPropertyInstances(null, session);
    	final List<ObservationConstellation> obsConsts = Lists.newArrayList();

    	for (final ObservedPropertyInstance instance: instances) {
    		final ObservationConstellation obsConst = new ObservationConstellation();

    		obsConst.setDeleted(false);
    		obsConst.setDisabled(false);
    		obsConst.setHiddenChild(false);
    		obsConst.setObservableProperty(new ObservablePropertyDAO().createObservablePropertyWithInstance(instance, session));
    		obsConst.setObservationType(new ObservationTypeDAO().getObservationTypeObject(ObservationTypeDAO.HZG_OBSERVATION_TYPE_STRING, session));
    		obsConst.setOffering(new OfferingDAO().getOfferingForIdentifier(sosConfiguration.getOfferingIdentifierPrefix() + sosConfiguration.getOfferingName(), session));
    		obsConst.setProcedure(new ProcedureDAO().createProceduresWithSensors(Lists.newArrayList(instance.getSensor()), session).get(0));

    		obsConsts.add(obsConst);
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
    @SuppressWarnings("unchecked")
    public List<ObservationConstellation> getObservationConstellations(List<String> procedures,
            List<String> observedProperties, List<String> offerings, Session session) {
        final Criteria c =
                session.createCriteria(ObservationConstellation.class)
                        .add(Restrictions.eq(ObservationConstellation.DELETED, false))
                        .setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
        if (CollectionHelper.isNotEmpty(offerings)) {
            c.createCriteria(ObservationConstellation.OFFERING).add(Restrictions.in(Offering.IDENTIFIER, offerings));
        }
        if (CollectionHelper.isNotEmpty(observedProperties)) {
            c.createCriteria(ObservationConstellation.OBSERVABLE_PROPERTY).add(
                    Restrictions.in(ObservableProperty.IDENTIFIER, observedProperties));
        }
        if (CollectionHelper.isNotEmpty(procedures)) {
            c.createCriteria(ObservationConstellation.PROCEDURE)
                    .add(Restrictions.in(Procedure.IDENTIFIER, procedures));
        }
        c.add(Restrictions.isNotNull(ObservationConstellation.OBSERVATION_TYPE));
        LOGGER.debug("QUERY getObservationConstellations(): {}", HibernateHelper.getSqlString(c));
        return c.list();

    }
}
