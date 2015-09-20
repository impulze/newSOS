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
package org.n52.sos.ds.hibernate.dao.ereporting;

import java.util.Collection;
import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;
import org.hibernate.internal.util.collections.CollectionHelper;
import org.n52.sos.ds.hibernate.dao.FeatureOfInterestDAO;
import org.n52.sos.ds.hibernate.dao.ObservablePropertyDAO;
import org.n52.sos.ds.hibernate.dao.ProcedureDAO;
import org.n52.sos.ds.hibernate.dao.series.AbstractSeriesDAO;
import org.n52.sos.ds.hibernate.dao.series.SeriesIdentifiers;
import org.n52.sos.ds.hibernate.entities.FeatureOfInterest;
import org.n52.sos.ds.hibernate.entities.ereporting.EReportingSamplingPoint;
import org.n52.sos.ds.hibernate.entities.ereporting.EReportingSeries;
import org.n52.sos.ds.hibernate.entities.series.Series;
import org.n52.sos.exception.CodedException;
import org.n52.sos.request.GetObservationRequest;
import org.n52.sos.service.SosContextListener;

import com.google.common.collect.Lists;

import de.hzg.common.SOSConfiguration;
import de.hzg.measurement.ObservedPropertyInstance;

public class EReportingSeriesDAO extends AbstractSeriesDAO {
	public List<EReportingSeries> getAllSeries(Session session) {
		// TODOHZG: get all series
		final ObservablePropertyDAO obsPropDAO = new ObservablePropertyDAO();
		final ProcedureDAO procedureDAO = new ProcedureDAO();
		final EReportingSamplingPoint samplingPoint = new EReportingSamplingPointDAO().createSamplingPoint(session);
		final List<ObservedPropertyInstance> instances = obsPropDAO.getObservedPropertyInstances(session);
		final FeatureOfInterest foi = new FeatureOfInterestDAO().createFeatureOfInterest(session);
		final List<EReportingSeries> allSeries = Lists.newArrayListWithCapacity(instances.size());

		for (final ObservedPropertyInstance instance: instances) {
			final EReportingSeries series = new EReportingSeries();

			series.setDeleted(false);
			series.setFeatureOfInterest(foi);
			series.setObservableProperty(obsPropDAO.createObservablePropertyWithInstance(instance, session));
			series.setProcedure(procedureDAO.createProcedureWithSensor(instance.getSensor(), session));
			series.setPublished(true);
			series.setSamplingPoint(samplingPoint);

			allSeries.add(series);
		}

		return allSeries;
	}

    @Override
    protected Class<?> getSeriesClass() {
        return EReportingSeries.class;
    }

    @Override
    public List<Series> getSeries(GetObservationRequest request, Collection<String> features, Session session) throws CodedException {
    	return getSeries(request.getProcedures(), request.getObservedProperties(), features, session);
    }

    @Override
    public List<Series> getSeries(String observedProperty, Collection<String> features, Session session) {
    	return getSeries(null, Lists.newArrayList(observedProperty), features, session);
    }

    @Override
    public List<Series> getSeries(Collection<String> procedures, Collection<String> observedProperties,
            Collection<String> features, Session session) {
    	final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;
    	final List<String> observedPropertyNames = Lists.newArrayListWithCapacity(observedProperties.size());
    	final List<String> procedureNames = Lists.newArrayListWithCapacity(procedures.size());

    	for (final String observedProperty: observedProperties) {
    		if (observedProperty.startsWith(sosConfiguration.getObservablePropertyIdentifierPrefix())) {
    			observedPropertyNames.add(observedProperty.substring(sosConfiguration.getObservablePropertyIdentifierPrefix().length()));
    		}
    	}

    	for (final String procedure: procedures) {
    		if (procedure.startsWith(sosConfiguration.getProcedureIdentifierPrefix())) {
    			procedureNames.add(procedure.substring(sosConfiguration.getProcedureIdentifierPrefix().length()));
    		}
    	}

    	final Criteria criteria = session.createCriteria(ObservedPropertyInstance.class);

    	if (CollectionHelper.isNotEmpty(observedPropertyNames)) {
    		criteria.add(Restrictions.in("name", observedPropertyNames));
    	}

    	if (CollectionHelper.isNotEmpty(procedureNames)) {
    		criteria
    			.createAlias("sensor", "s")
    			.add(Restrictions.in("s.name", procedureNames));
    	}

    	@SuppressWarnings("unchecked")
		final List<ObservedPropertyInstance> instances = criteria.list();

    	if (instances == null) {
    		return Lists.newArrayList();
    	}

    	final List<Series> allSeries = Lists.newArrayListWithCapacity(instances.size());
    	final FeatureOfInterest foi = new FeatureOfInterestDAO().createFeatureOfInterest(session);
    	final ObservablePropertyDAO observablePropertyDAO = new ObservablePropertyDAO();
    	final ProcedureDAO procedureDAO = new ProcedureDAO();

    	for (final ObservedPropertyInstance instance: instances) {
    		final EReportingSeries series = new EReportingSeries();

    		series.setDeleted(false);
    		series.setFeatureOfInterest(foi);
    		series.setObservableProperty(observablePropertyDAO.createObservablePropertyWithInstance(instance, session));
    		series.setProcedure(procedureDAO.createProcedureWithSensor(instance.getSensor(), session));
    		series.setPublished(true);

    		allSeries.add(series);
    	}

    	return allSeries;
    }

    @Override
    public EReportingSeries getOrInsertSeries(SeriesIdentifiers identifiers, Session session) throws CodedException {
        return (EReportingSeries) super.getOrInsert(identifiers, session);
    }
    
    /**
     * Add EReportingSamplingPoint restriction to Hibernate Criteria
     * 
     * @param c
     *            Hibernate Criteria to add restriction
     * @param samplingPoint
     *            EReportingSamplingPoint identifier to add
     */
    public void addEReportingSamplingPointToCriteria(Criteria c, String samplingPoint) {
        c.createCriteria(EReportingSeries.SAMPLING_POINT).add(Restrictions.eq(EReportingSamplingPoint.IDENTIFIER, samplingPoint));

    }
    
    /**
     * Add EReportingSamplingPoint restriction to Hibernate Criteria
     * 
     * @param c
     *            Hibernate Criteria to add restriction
     * @param samplingPoint
     *            EReportingSamplingPoint to add
     */
    public void addEReportingSamplingPointToCriteria(Criteria c, EReportingSamplingPoint samplingPoint) {
        c.add(Restrictions.eq(EReportingSeries.SAMPLING_POINT, samplingPoint));
    }

    /**
     * Add EReportingSamplingPoint restriction to Hibernate Criteria
     * 
     * @param c
     *            Hibernate Criteria to add restriction
     * @param samplingPoints
     *            EReportingSamplingPoint identifiers to add
     */
    public void addEReportingSamplingPointToCriteria(Criteria c, Collection<String> samplingPoints) {
        c.createCriteria(EReportingSeries.SAMPLING_POINT).add(Restrictions.in(EReportingSamplingPoint.IDENTIFIER, samplingPoints));
    }

    /* TODOHZG: add those when needed
    @Override
    protected void addSpecificRestrictions(Criteria c, GetObservationRequest request) throws CodedException {
        if (request.isSetResponseFormat() && AqdConstants.NS_AQD.equals(request.getResponseFormat())) {
            ReportObligationType flow = AqdHelper.getInstance().getFlow(request.getExtensions());
            if (ReportObligationType.E1A.equals(flow) || ReportObligationType.E2A.equals(flow)) {
                addAssessmentType(c, AqdConstants.AssessmentType.Fixed.name());
            } else if (ReportObligationType.E1B.equals(flow)) {
                addAssessmentType(c, AqdConstants.AssessmentType.Model.name());
            } else {
                throw new OptionNotSupportedException().withMessage("The requested e-Reporting flow %s is not supported!", flow.name());
            }
        }
    }
    
    private void addAssessmentType(Criteria c, String assessmentType) {
        c.createCriteria(EReportingSeries.SAMPLING_POINT).createCriteria(EReportingSamplingPoint.ASSESSMENTTYPE).
        add(Restrictions.ilike(EReportingAssessmentType.ASSESSMENT_TYPE, assessmentType));
    }
    */
    
}
