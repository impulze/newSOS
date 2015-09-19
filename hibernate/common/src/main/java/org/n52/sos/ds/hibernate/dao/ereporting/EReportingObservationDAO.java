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
import java.util.Map;

import org.hibernate.Criteria;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.criterion.DetachedCriteria;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernate.criterion.Subqueries;
import org.n52.sos.aqd.AqdConstants;
import org.n52.sos.aqd.AqdHelper;
import org.n52.sos.aqd.ReportObligationType;
import org.n52.sos.ds.hibernate.dao.series.AbstractSeriesObservationDAO;
import org.n52.sos.ds.hibernate.entities.ereporting.EReportingAssessmentType;
import org.n52.sos.ds.hibernate.entities.ereporting.EReportingBlobObservation;
import org.n52.sos.ds.hibernate.entities.ereporting.EReportingBooleanObservation;
import org.n52.sos.ds.hibernate.entities.ereporting.EReportingCategoryObservation;
import org.n52.sos.ds.hibernate.entities.ereporting.EReportingCountObservation;
import org.n52.sos.ds.hibernate.entities.ereporting.EReportingGeometryObservation;
import org.n52.sos.ds.hibernate.entities.ereporting.EReportingNumericObservation;
import org.n52.sos.ds.hibernate.entities.ereporting.EReportingObservation;
import org.n52.sos.ds.hibernate.entities.ereporting.EReportingObservationInfo;
import org.n52.sos.ds.hibernate.entities.ereporting.EReportingObservationTime;
import org.n52.sos.ds.hibernate.entities.ereporting.EReportingSamplingPoint;
import org.n52.sos.ds.hibernate.entities.ereporting.EReportingSeries;
import org.n52.sos.ds.hibernate.entities.ereporting.EReportingSweDataArrayObservation;
import org.n52.sos.ds.hibernate.entities.ereporting.EReportingTextObservation;
import org.n52.sos.ds.hibernate.entities.series.Series;
import org.n52.sos.ds.hibernate.entities.series.SeriesObservation;
import org.n52.sos.ds.hibernate.util.TimeCriterion;
import org.n52.sos.exception.CodedException;
import org.n52.sos.exception.ows.OptionNotSupportedException;
import org.n52.sos.ogc.ows.OwsExceptionReport;
import org.n52.sos.ogc.sos.SosConstants.SosIndeterminateTime;
import org.n52.sos.request.GetObservationRequest;

public class EReportingObservationDAO extends AbstractSeriesObservationDAO {
    @Override
    public List<SeriesObservation> getSeriesObservationsFor(GetObservationRequest request,
            Collection<String> features, Session session) throws OwsExceptionReport {
        return getSeriesObservationsFor(request, features, null, null, session);
    }

    @Override
    public List<SeriesObservation> getSeriesObservationsFor(GetObservationRequest request,
            Collection<String> features, Map<String, Collection<TimeCriterion>> temporalFilterDisjunctions, Session session) throws OwsExceptionReport {
        return getSeriesObservationsFor(request, features, temporalFilterDisjunctions, null, session);
    }

    @Override
    public List<SeriesObservation> getSeriesObservationsFor(GetObservationRequest request,
            Collection<String> features, SosIndeterminateTime sosIndeterminateTime, Session session)
            throws OwsExceptionReport {
        return getSeriesObservationsFor(request, features, null, sosIndeterminateTime, session);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected List<SeriesObservation> getSeriesObservationsFor(GetObservationRequest request,
            Collection<String> features, Map<String, Collection<TimeCriterion>> temporalFilterDisjunctions, SosIndeterminateTime sosIndeterminateTime,
            Session session) throws HibernateException, OwsExceptionReport {
        return getSeriesObservationCriteriaFor(request, features, temporalFilterDisjunctions, sosIndeterminateTime, session)
                .list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<SeriesObservation> getSeriesObservationsFor(Series series, GetObservationRequest request,
            SosIndeterminateTime sosIndeterminateTime, Session session) throws OwsExceptionReport {
        return getSeriesObservationCriteriaFor(series, request, sosIndeterminateTime, session).list();
    }

    @Override
    protected void addSpecificRestrictions(Criteria c, GetObservationRequest request)
            throws CodedException {
        if (request.isSetResponseFormat() && AqdConstants.NS_AQD.equals(request.getResponseFormat())) {
            ReportObligationType flow = AqdHelper.getInstance().getFlow(request.getExtensions());
            if (ReportObligationType.E1A.equals(flow) || ReportObligationType.E2A.equals(flow)) {
                addAssessmentType(c, AqdConstants.AssessmentType.Fixed.name());
            } else if (ReportObligationType.E1B.equals(flow)) {
                addAssessmentType(c, AqdConstants.AssessmentType.Model.name());
            } else {
                throw new OptionNotSupportedException().withMessage("The requested e-Reporting flow %s is not supported!",
                        flow.name());
            }
         // add quality restrictions
            EReportingDaoHelper.addValidityAndVerificationRestrictions(c, request);
        }
    }

    private void addAssessmentType(Criteria c, String assessmentType) {
        final DetachedCriteria detachedCriteria = DetachedCriteria.forClass(EReportingSeries.class);
        detachedCriteria.add(Restrictions.eq(Series.DELETED, false));
        detachedCriteria.createCriteria(EReportingSeries.SAMPLING_POINT).createCriteria(EReportingSamplingPoint.ASSESSMENTTYPE).
        add(Restrictions.ilike(EReportingAssessmentType.ASSESSMENT_TYPE, assessmentType));
        detachedCriteria.setProjection(Projections.distinct(Projections.property(Series.ID)));
        c.add(Subqueries.propertyIn(SeriesObservation.SERIES, detachedCriteria));
    }

    @Override
    protected Class<?> getObservationClass() {
        return EReportingObservation.class;
    }

    @Override
    protected Class<?> getObservationInfoClass() {
        return EReportingObservationInfo.class;
    }

    @Override
    protected Class<?> getObservationTimeClass() {
        return EReportingObservationTime.class;
    }

    @Override
    protected Class<?> getBlobObservationClass() {
        return EReportingBlobObservation.class;
    }

    @Override
    protected Class<?> getBooleanObservationClass() {
        return EReportingBooleanObservation.class;
    }

    @Override
    protected Class<?> getCategoryObservationClass() {
        return EReportingCategoryObservation.class;
    }

    @Override
    protected Class<?> getCountObservationClass() {
        return EReportingCountObservation.class;
    }

    @Override
    protected Class<?> getGeometryObservationClass() {
        return EReportingGeometryObservation.class;
    }

    @Override
    protected Class<?> getNumericObservationClass() {
        return EReportingNumericObservation.class;
    }

    @Override
    protected Class<?> getSweDataArrayObservationClass() {
        return EReportingSweDataArrayObservation.class;
    }

    @Override
    protected Class<?> getTextObservationClass() {
        return EReportingTextObservation.class;
    }
}
