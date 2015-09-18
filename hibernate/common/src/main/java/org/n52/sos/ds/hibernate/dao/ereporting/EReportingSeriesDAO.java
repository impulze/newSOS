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
import org.n52.sos.ds.hibernate.entities.FeatureOfInterest;
import org.n52.sos.ds.hibernate.entities.Procedure;
import org.n52.sos.ds.hibernate.entities.ereporting.EReportingSamplingPoint;
import org.n52.sos.ds.hibernate.entities.ereporting.EReportingSeries;
import org.n52.sos.ds.hibernate.entities.series.Series;
import org.n52.sos.exception.CodedException;
import org.n52.sos.request.GetObservationRequest;

import com.google.common.collect.Lists;

import de.hzg.measurement.ObservedPropertyInstance;

public class EReportingSeriesDAO extends AbstractSeriesDAO {
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
        final ObservablePropertyDAO obsPropDAO = new ObservablePropertyDAO();
        final ProcedureDAO procedureDAO = new ProcedureDAO();
        final FeatureOfInterestDAO featureDAO = new FeatureOfInterestDAO();
        final EReportingSamplingPoint samplingPoint = new EReportingSamplingPointDAO().createSamplingPoint(session);
        FeatureOfInterest foi = null;

        if (!CollectionHelper.isEmpty(features)) {
            for (final String feature: features) {
                foi = featureDAO.getFeatureOfInterest(feature, session);

                if (foi != null) {
                    break;
                }
            }
        } else {
            foi = new FeatureOfInterestDAO().createFeatureOfInterest(session);
        }

        if (foi == null) {
            return Lists.newArrayList();
        }

        final List<ObservedPropertyInstance> instances = obsPropDAO.getObservedPropertyInstances(observedProperties, session);
        final List<Series> result = Lists.newArrayListWithCapacity(instances.size());

        for (final ObservedPropertyInstance instance: instances) {
            final Procedure procedure = procedureDAO.createProcedureWithSensor(instance.getSensor(), session);

            if (CollectionHelper.isNotEmpty(procedures)) {
                if (!procedures.contains(procedure.getIdentifier())) {
                    continue;
                }
            }

            final EReportingSeries series = new EReportingSeries();

            series.setDeleted(false);
            series.setFeatureOfInterest(foi);
            series.setObservableProperty(obsPropDAO.createObservablePropertyWithInstance(instance, session));
            series.setProcedure(procedure);
            series.setPublished(true);
            series.setSamplingPoint(samplingPoint);

            result.add(series);
        }

        return result;
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

    /*
     * TODOHZG: use this when needed
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
