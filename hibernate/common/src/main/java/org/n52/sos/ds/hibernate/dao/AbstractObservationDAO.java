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

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.hibernate.Criteria;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Projection;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.spatial.criterion.SpatialProjections;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.n52.sos.ds.hibernate.entities.AbstractObservation;
import org.n52.sos.ds.hibernate.entities.AbstractObservationTime;
import org.n52.sos.ds.hibernate.entities.interfaces.BlobObservation;
import org.n52.sos.ds.hibernate.entities.interfaces.CategoryObservation;
import org.n52.sos.ds.hibernate.entities.Codespace;
import org.n52.sos.ds.hibernate.entities.interfaces.CountObservation;
import org.n52.sos.ds.hibernate.entities.FeatureOfInterest;
import org.n52.sos.ds.hibernate.entities.interfaces.GeometryObservation;
import org.n52.sos.ds.hibernate.entities.interfaces.NumericObservation;
import org.n52.sos.ds.hibernate.entities.ObservableProperty;
import org.n52.sos.ds.hibernate.entities.Offering;
import org.n52.sos.ds.hibernate.entities.Procedure;
import org.n52.sos.ds.hibernate.entities.interfaces.SweDataArrayObservation;
import org.n52.sos.ds.hibernate.entities.interfaces.TextObservation;
import org.n52.sos.ds.hibernate.entities.Unit;
import org.n52.sos.ds.hibernate.entities.interfaces.BooleanObservation;
import org.n52.sos.ds.hibernate.util.HibernateConstants;
import org.n52.sos.ds.hibernate.util.HibernateHelper;
import org.n52.sos.ds.hibernate.util.SpatialRestrictions;
import org.n52.sos.exception.CodedException;
import org.n52.sos.exception.ows.NoApplicableCodeException;
import org.n52.sos.ogc.gml.time.TimePeriod;
import org.n52.sos.ogc.om.OmConstants;
import org.n52.sos.ogc.om.OmObservation;
import org.n52.sos.ogc.om.values.BooleanValue;
import org.n52.sos.ogc.om.values.CategoryValue;
import org.n52.sos.ogc.om.values.CountValue;
import org.n52.sos.ogc.om.values.GeometryValue;
import org.n52.sos.ogc.om.values.QuantityValue;
import org.n52.sos.ogc.om.values.SweDataArrayValue;
import org.n52.sos.ogc.om.values.TextValue;
import org.n52.sos.ogc.om.values.UnknownValue;
import org.n52.sos.ogc.om.values.Value;
import org.n52.sos.ogc.ows.OwsExceptionReport;
import org.n52.sos.ogc.sos.SosConstants.SosIndeterminateTime;
import org.n52.sos.ogc.sos.SosEnvelope;
import org.n52.sos.request.GetObservationRequest;
import org.n52.sos.util.CollectionHelper;
import org.n52.sos.util.GeometryHandler;
import org.n52.sos.util.StringHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

/**
 * Abstract Hibernate data access class for observations.
 *
 * @author Carsten Hollmann <c.hollmann@52north.org>
 * @since 4.0.0
 *
 */
public abstract class AbstractObservationDAO extends AbstractIdentifierNameDescriptionDAO {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractObservationDAO.class);

    /**
     * Get Hibernate Criteria for result model
     *
     * @param resultModel
     *            Result model
     * @param session
     *            Hibernate session
     * @return Hibernate Criteria
     */
    public Criteria getObservationClassCriteriaForResultModel(String resultModel, Session session) {
        if (StringHelper.isNotEmpty(resultModel)) {
            if (resultModel.equals(OmConstants.OBS_TYPE_MEASUREMENT)) {
                return createCriteriaForObservationClass(getNumericObservationClass(), session);
            } else if (resultModel.equals(OmConstants.OBS_TYPE_COUNT_OBSERVATION)) {
                return createCriteriaForObservationClass(getCategoryObservationClass(), session);
            } else if (resultModel.equals(OmConstants.OBS_TYPE_CATEGORY_OBSERVATION)) {
                return createCriteriaForObservationClass(getCategoryObservationClass(), session);
            } else if (resultModel.equals(OmConstants.OBS_TYPE_TRUTH_OBSERVATION)) {
                return createCriteriaForObservationClass(getBooleanObservationClass(), session);
            } else if (resultModel.equals(OmConstants.OBS_TYPE_TEXT_OBSERVATION)) {
                return createCriteriaForObservationClass(getTextObservationClass(), session);
            } else if (resultModel.equals(OmConstants.OBS_TYPE_GEOMETRY_OBSERVATION)) {
                return createCriteriaForObservationClass(getGeometryObservationClass(), session);
            } else if (resultModel.equals(OmConstants.OBS_TYPE_COMPLEX_OBSERVATION)) {
                return createCriteriaForObservationClass(getBlobObservationClass(), session);
            }
        }
        return createCriteriaForObservationClass(getObservationClass(), session);
    }

    /**
     * Create an observation object from SOS value
     *
     * @param value
     *            SOS value
     * @param session
     *            Hibernate session
     * @return Observation object
     * @throws CodedException 
     */
    public AbstractObservation createObservationFromValue(Value<?> value, Session session) throws CodedException {
        try {

            if (value instanceof BooleanValue) {
                AbstractObservation observation = (AbstractObservation) getBooleanObservationClass().newInstance();
                ((BooleanObservation) observation).setValue(((BooleanValue) value).getValue());
                return observation;
            } else if (value instanceof UnknownValue) {
                AbstractObservation observation = (AbstractObservation) getBlobObservationClass().newInstance();
                ((BlobObservation) observation).setValue(((UnknownValue) value).getValue());
                return observation;
            } else if (value instanceof CategoryValue) {
                AbstractObservation observation = (AbstractObservation) getCategoryObservationClass().newInstance();
                ((CategoryObservation) observation).setValue(((CategoryValue) value).getValue());
                return observation;
            } else if (value instanceof CountValue) {
                AbstractObservation observation = (AbstractObservation) getCountObservationClass().newInstance();
                ((CountObservation) observation).setValue(((CountValue) value).getValue());
                return observation;
            } else if (value instanceof GeometryValue) {
                AbstractObservation observation = (AbstractObservation) getGeometryObservationClass().newInstance();
                ((GeometryObservation) observation).setValue(((GeometryValue) value).getValue());
                return observation;
            } else if (value instanceof QuantityValue) {
                AbstractObservation observation = (AbstractObservation) getNumericObservationClass().newInstance();
                ((NumericObservation) observation).setValue(((QuantityValue) value).getValue());
                return observation;
            } else if (value instanceof TextValue) {
                AbstractObservation observation = (AbstractObservation) getTextObservationClass().newInstance();
                ((TextObservation) observation).setValue(((TextValue) value).getValue());
                return observation;
            } else if (value instanceof SweDataArrayValue) {
                AbstractObservation observation = (AbstractObservation) getSweDataArrayObservationClass().newInstance();
                ((SweDataArrayObservation) observation).setValue(((SweDataArrayValue) value).getValue().getXml());
                return observation;
            }
            return (AbstractObservation) getObservationClass().newInstance();
        } catch (IllegalAccessException | InstantiationException e) {
            throw new NoApplicableCodeException().causedBy(e).withMessage("Error while creating observation instance for %S", value.getClass().getCanonicalName());
        }
    }

    /**
     * Get default Hibernate Criteria to query observations, default flag ==
     * <code>false</code>
     *
     * @param session
     *            Hiberante session
     * @return Default Criteria
     */
    public Criteria getDefaultObservationCriteria(Session session) {
       return getDefaultCriteria(getObservationClass(), session);
    }

    /**
     * Get default Hibernate Criteria to query observation info, default flag ==
     * <code>false</code>
     *
     * @param session
     *            Hiberante session
     * @return Default Criteria
     */
    public Criteria getDefaultObservationInfoCriteria(Session session) {
        return getDefaultCriteria(getObservationInfoClass(), session);
    }
    
    /**
     * Get default Hibernate Criteria to query observation time, default flag ==
     * <code>false</code>
     *
     * @param session
     *            Hiberante session
     * @return Default Criteria
     */
    public Criteria getDefaultObservationTimeCriteria(Session session) {
        return getDefaultCriteria(getObservationTimeClass(), session);
    }
    
    @SuppressWarnings("rawtypes")
    private Criteria getDefaultCriteria(Class clazz, Session session) {
        return session.createCriteria(clazz).add(Restrictions.eq(AbstractObservation.DELETED, false))
                .setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
    }

    /**
     * If the local codespace cache isn't null, use it when retrieving
     * codespaces.
     * 
     * @param codespace
     *            Codespace
     * @param localCache
     *            Cache (possibly null)
     * @param session
     * @return Codespace
     */
    protected Codespace getCodespace(String codespace, Map<String, Codespace> localCache, Session session) {
        if (localCache != null && localCache.containsKey(codespace)) {
            return localCache.get(codespace);
        } else {
            // query codespace and set cache
            Codespace hCodespace = new CodespaceDAO().getOrInsertCodespace(codespace, session);
            if (localCache != null) {
                localCache.put(codespace, hCodespace);
            }
            return hCodespace;
        }
    }

    /**
     * If the local unit cache isn't null, use it when retrieving unit.
     * 
     * @param unit
     *            Unit
     * @param localCache
     *            Cache (possibly null)
     * @param session
     * @return Unit
     */
    protected Unit getUnit(String unit, Map<String, Unit> localCache, Session session) {
        if (localCache != null && localCache.containsKey(unit)) {
            return localCache.get(unit);
        } else {
            // query unit and set cache
            Unit hUnit = new UnitDAO().getOrInsertUnit(unit, session);
            if (localCache != null) {
                localCache.put(unit, hUnit);
            }
            return hUnit;
        }
    }

    /**
     * Add observation identifier (gml:identifier) to Hibernate Criteria
     *
     * @param criteria
     *            Hibernate Criteria
     * @param identifier
     *            Observation identifier (gml:identifier)
     * @param session
     *            Hibernate session
     */
    protected void addObservationIdentifierToCriteria(Criteria criteria, String identifier, Session session) {
        criteria.add(Restrictions.eq(AbstractObservation.IDENTIFIER, identifier));
    }

//    /**
//     * Add offerings to observation and return the observation identifiers
//     * procedure and observableProperty
//     *
//     * @param hObservation
//     *            Observation to add offerings
//     * @param hObservationConstellations
//     *            Observation constellation with offerings, procedure and
//     *            observableProperty
//     * @return ObservaitonIdentifiers object with procedure and
//     *         observableProperty
//     */
//    protected ObservationIdentifiers addOfferingsToObaservationAndGetProcedureObservableProperty(
//            AbstractObservation hObservation, Set<ObservationConstellation> hObservationConstellations) {
//        Iterator<ObservationConstellation> iterator = hObservationConstellations.iterator();
//        boolean firstObsConst = true;
//        ObservationIdentifiers observationIdentifiers = new ObservationIdentifiers();
//        while (iterator.hasNext()) {
//            ObservationConstellation observationConstellation = iterator.next();
//            if (firstObsConst) {
//                observationIdentifiers.setObservableProperty(observationConstellation.getObservableProperty());
//                observationIdentifiers.setProcedure(observationConstellation.getProcedure());
//                firstObsConst = false;
//            }
//            hObservation.getOfferings().add(observationConstellation.getOffering());
//        }
//        return observationIdentifiers;
//    }

    protected void finalizeObservationInsertion(OmObservation sosObservation, AbstractObservation hObservation,
            Session session) throws OwsExceptionReport {
        // TODO if this observation is a deleted=true, how to set deleted=false
        // instead of insert

    }

    /**
     * Check if there are observations for the offering
     *
     * @param clazz
     *            Observation sub class
     * @param offeringIdentifier
     *            Offering identifier
     * @param session
     *            Hibernate session
     * @return If there are observations or not
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected boolean checkObservationFor(Class clazz, String offeringIdentifier, Session session) {
        Criteria c = session.createCriteria(clazz).add(Restrictions.eq(AbstractObservation.DELETED, false));
        c.createCriteria(AbstractObservation.OFFERINGS).add(Restrictions.eq(Offering.IDENTIFIER, offeringIdentifier));
        c.setMaxResults(1);
        LOGGER.debug("QUERY checkObservationFor(clazz, offeringIdentifier): {}", HibernateHelper.getSqlString(c));
        return CollectionHelper.isNotEmpty(c.list());
    }

    /**
     * Get min phenomenon time from observations
     *
     * @param session
     *            Hibernate session Hibernate session
     * @return min time
     */
    public DateTime getMinPhenomenonTime(Session session) {
        Criteria criteria =
                session.createCriteria(getObservationTimeClass())
                        .setProjection(Projections.min(AbstractObservation.PHENOMENON_TIME_START))
                        .add(Restrictions.eq(AbstractObservation.DELETED, false));
        LOGGER.debug("QUERY getMinPhenomenonTime(): {}", HibernateHelper.getSqlString(criteria));
        Object min = criteria.uniqueResult();
        if (min != null) {
            return new DateTime(min, DateTimeZone.UTC);
        }
        return null;
    }

    /**
     * Get max phenomenon time from observations
     *
     * @param session
     *            Hibernate session Hibernate session
     *
     * @return max time
     */
    public DateTime getMaxPhenomenonTime(Session session) {

        Criteria criteriaStart =
                session.createCriteria(getObservationTimeClass())
                        .setProjection(Projections.max(AbstractObservation.PHENOMENON_TIME_START))
                        .add(Restrictions.eq(AbstractObservation.DELETED, false));
        LOGGER.debug("QUERY getMaxPhenomenonTime() start: {}", HibernateHelper.getSqlString(criteriaStart));
        Object maxStart = criteriaStart.uniqueResult();

        Criteria criteriaEnd =
                session.createCriteria(getObservationTimeClass())
                        .setProjection(Projections.max(AbstractObservation.PHENOMENON_TIME_END))
                        .add(Restrictions.eq(AbstractObservation.DELETED, false));
        LOGGER.debug("QUERY getMaxPhenomenonTime() end: {}", HibernateHelper.getSqlString(criteriaEnd));
        Object maxEnd = criteriaEnd.uniqueResult();
        if (maxStart == null && maxEnd == null) {
            return null;
        } else {
            DateTime start = new DateTime(maxStart, DateTimeZone.UTC);
            if (maxEnd != null) {
                DateTime end = new DateTime(maxEnd, DateTimeZone.UTC);
                if (end.isAfter(start)) {
                    return end;
                }
            }
            return start;
        }
    }

    /**
     * Get min result time from observations
     *
     * @param session
     *            Hibernate session Hibernate session
     *
     * @return min time
     */
    public DateTime getMinResultTime(Session session) {

        Criteria criteria =
                session.createCriteria(getObservationTimeClass())
                        .setProjection(Projections.min(AbstractObservation.RESULT_TIME))
                        .add(Restrictions.eq(AbstractObservation.DELETED, false));
        LOGGER.debug("QUERY getMinResultTime(): {}", HibernateHelper.getSqlString(criteria));
        Object min = criteria.uniqueResult();
        if (min != null) {
            return new DateTime(min, DateTimeZone.UTC);
        }
        return null;
    }

    /**
     * Get max phenomenon time from observations
     *
     * @param session
     *            Hibernate session Hibernate session
     *
     * @return max time
     */
    public DateTime getMaxResultTime(Session session) {

        Criteria criteria =
                session.createCriteria(getObservationTimeClass())
                        .setProjection(Projections.max(AbstractObservation.RESULT_TIME))
                        .add(Restrictions.eq(AbstractObservation.DELETED, false));
        LOGGER.debug("QUERY getMaxResultTime(): {}", HibernateHelper.getSqlString(criteria));
        Object max = criteria.uniqueResult();
        if (max == null) {
            return null;
        } else {
            return new DateTime(max, DateTimeZone.UTC);
        }
    }

    /**
     * Get global temporal bounding box
     *
     * @param session
     *            Hibernate session the session
     *
     * @return the global getEqualRestiction bounding box over all observations,
     *         or <tt>null</tt>
     */
    public TimePeriod getGlobalTemporalBoundingBox(Session session) {
        if (session != null) {
            Criteria criteria = session.createCriteria(getObservationTimeClass());
            criteria.add(Restrictions.eq(AbstractObservation.DELETED, false));
            criteria.setProjection(Projections.projectionList()
                    .add(Projections.min(AbstractObservation.PHENOMENON_TIME_START))
                    .add(Projections.max(AbstractObservation.PHENOMENON_TIME_START))
                    .add(Projections.max(AbstractObservation.PHENOMENON_TIME_END)));
            LOGGER.debug("QUERY getGlobalTemporalBoundingBox(): {}", HibernateHelper.getSqlString(criteria));
            Object temporalBoundingBox = criteria.uniqueResult();
            if (temporalBoundingBox instanceof Object[]) {
                Object[] record = (Object[]) temporalBoundingBox;
                TimePeriod bBox =
                        createTimePeriod((Timestamp) record[0], (Timestamp) record[1], (Timestamp) record[2]);
                return bBox;
            }
        }
        return null;
    }

    /**
     * Get order for {@link SosIndeterminateTime} value
     *
     * @param indetTime
     *            Value to get order for
     * @return Order
     */
    protected Order getOrder(final SosIndeterminateTime indetTime) {
        if (indetTime.equals(SosIndeterminateTime.first)) {
            return Order.asc(AbstractObservation.PHENOMENON_TIME_START);
        } else if (indetTime.equals(SosIndeterminateTime.latest)) {
            return Order.desc(AbstractObservation.PHENOMENON_TIME_END);
        }
        return null;
    }

    /**
     * Get projection for {@link SosIndeterminateTime} value
     *
     * @param indetTime
     *            Value to get projection for
     * @return Projection to use to determine indeterminate time extrema
     */
    protected Projection getIndeterminateTimeExtremaProjection(final SosIndeterminateTime indetTime) {
        if (indetTime.equals(SosIndeterminateTime.first)) {
            return Projections.min(AbstractObservation.PHENOMENON_TIME_START);
        } else if (indetTime.equals(SosIndeterminateTime.latest)) {
            return Projections.max(AbstractObservation.PHENOMENON_TIME_END);
        }
        return null;
    }

    /**
     * Get the AbstractObservation property to filter on for an
     * {@link SosIndeterminateTime}
     *
     * @param indetTime
     *            Value to get property for
     * @return String property to filter on
     */
    protected String getIndeterminateTimeFilterProperty(final SosIndeterminateTime indetTime) {
        if (indetTime.equals(SosIndeterminateTime.first)) {
            return AbstractObservation.PHENOMENON_TIME_START;
        } else if (indetTime.equals(SosIndeterminateTime.latest)) {
            return AbstractObservation.PHENOMENON_TIME_END;
        }
        return null;
    }

    /**
     * Add an indeterminate time restriction to a criteria. This allows for
     * multiple results if more than one observation has the extrema time (max
     * for latest, min for first). Note: use this method *after* adding all
     * other applicable restrictions so that they will apply to the min/max
     * observation time determination.
     *
     * @param c
     *            Criteria to add the restriction to
     * @param sosIndeterminateTime
     *            Indeterminate time restriction to add
     * @return Modified criteria
     */
    protected Criteria addIndeterminateTimeRestriction(Criteria c, SosIndeterminateTime sosIndeterminateTime) {
        // get extrema indeterminate time
        c.setProjection(getIndeterminateTimeExtremaProjection(sosIndeterminateTime));
        Timestamp indeterminateExtremaTime = (Timestamp) c.uniqueResult();
        return addIndeterminateTimeRestriction(c, sosIndeterminateTime, indeterminateExtremaTime);
    }

    /**
     * Add an indeterminate time restriction to a criteria. This allows for
     * multiple results if more than one observation has the extrema time (max
     * for latest, min for first). Note: use this method *after* adding all
     * other applicable restrictions so that they will apply to the min/max
     * observation time determination.
     * 
     * @param c
     *            Criteria to add the restriction to
     * @param sosIndeterminateTime
     *            Indeterminate time restriction to add
     * @param indeterminateExtremaTime
     *            Indeterminate time extrema
     * @return Modified criteria
     */
    protected Criteria addIndeterminateTimeRestriction(Criteria c, SosIndeterminateTime sosIndeterminateTime,
            Date indeterminateExtremaTime) {
        // reset criteria
        // see http://stackoverflow.com/a/1472958/193435
        c.setProjection(null);
        c.setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);

        // get observations with exactly the extrema time
        c.add(Restrictions.eq(getIndeterminateTimeFilterProperty(sosIndeterminateTime), indeterminateExtremaTime));

        // not really necessary to return the Criteria object, but useful if we
        // want to chain
        return c;
    }

    /**
     * Create Hibernate Criteria for Class
     *
     * @param clazz
     *            Class
     * @param session
     *            Hibernate session
     * @return Hibernate Criteria for Class
     */
    @SuppressWarnings("rawtypes")
    protected Criteria createCriteriaForObservationClass(Class clazz, Session session) {
        return session.createCriteria(clazz).add(Restrictions.eq(AbstractObservation.DELETED, false))
                .setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
    }

    /**
     * Check if a Spatial Filtering Profile filter is requested and add to
     * criteria
     * 
     * @param c
     *            Criteria to add crtierion
     * @param request
     *            GetObservation request
     * @param session
     *            Hiberante Session
     * @throws OwsExceptionReport
     *             If Spatial Filteirng Profile is not supported or an error
     *             occurs.
     */
    protected void checkAndAddSpatialFilteringProfileCriterion(Criteria c, GetObservationRequest request,
            Session session) throws OwsExceptionReport {
        if (request.hasSpatialFilteringProfileSpatialFilter()) {
            c.add(SpatialRestrictions.filter(
                    AbstractObservation.SAMPLING_GEOMETRY,
                    request.getSpatialFilter().getOperator(),
                    GeometryHandler.getInstance().switchCoordinateAxisFromToDatasourceIfNeeded(
                            request.getSpatialFilter().getGeometry())));
        }

    }

    public SosEnvelope getSpatialFilteringProfileEnvelopeForOfferingId(String offeringID,
            Session session) throws OwsExceptionReport {
        try {
            // XXX workaround for Hibernate Spatial's lack of support for
            // GeoDB's extent aggregate see
            // http://www.hibernatespatial.org/pipermail/hibernatespatial-users/2013-August/000876.html
            Dialect dialect = ((SessionFactoryImplementor) session.getSessionFactory()).getDialect();
            if (GeometryHandler.getInstance().isSpatialDatasource()
                    && HibernateHelper.supportsFunction(dialect, HibernateConstants.FUNC_EXTENT)) {
                Criteria criteria = getDefaultObservationInfoCriteria(session);
                criteria.setProjection(SpatialProjections.extent(AbstractObservationTime.SAMPLING_GEOMETRY));
                criteria.createCriteria(AbstractObservation.OFFERINGS).add(
                        Restrictions.eq(Offering.IDENTIFIER, offeringID));
                LOGGER.debug("QUERY getSpatialFilteringProfileEnvelopeForOfferingId(offeringID): {}", HibernateHelper.getSqlString(criteria));
                Geometry geom = (Geometry) criteria.uniqueResult();
                geom = GeometryHandler.getInstance().switchCoordinateAxisFromToDatasourceIfNeeded(geom);
                if (geom != null) {
                    return new SosEnvelope(geom.getEnvelopeInternal(), GeometryHandler.getInstance().getStorageEPSG());
                }
            } else {
                final Envelope envelope = new Envelope();
                Criteria criteria = getDefaultObservationInfoCriteria(session);
                criteria.createCriteria(AbstractObservation.OFFERINGS).add(
                        Restrictions.eq(Offering.IDENTIFIER, offeringID));
                LOGGER.debug("QUERY getSpatialFilteringProfileEnvelopeForOfferingId(offeringID): {}", HibernateHelper.getSqlString(criteria));
                @SuppressWarnings("unchecked")
                final List<AbstractObservationTime> observationTimes = criteria.list();
                if (CollectionHelper.isNotEmpty(observationTimes)) {
                    for (final AbstractObservationTime observationTime : observationTimes) {
                        if (observationTime.hasSamplingGeometry()) {
                            final Geometry geom = observationTime.getSamplingGeometry();
                            if (geom != null && geom.getEnvelopeInternal() != null) {
                                envelope.expandToInclude(geom.getEnvelopeInternal());
                            }
                        }
                    }
                    if (!envelope.isNull()) {
                        return new SosEnvelope(envelope, GeometryHandler.getInstance().getStorageEPSG());
                    }
                }
            }
        } catch (final HibernateException he) {
            throw new NoApplicableCodeException().causedBy(he).withMessage(
                    "Exception thrown while requesting feature envelope for observation ids");
        }
        return null;
    }

    public abstract List<Geometry> getSamplingGeometries(String feature, Session session);

    protected abstract Class<?> getObservationClass();

    protected abstract Class<?> getObservationInfoClass();

    protected abstract Class<?> getObservationTimeClass();

    protected abstract Class<?> getBlobObservationClass();

    protected abstract Class<?> getBooleanObservationClass();

    protected abstract Class<?> getCategoryObservationClass();

    protected abstract Class<?> getCountObservationClass();

    protected abstract Class<?> getGeometryObservationClass();

    protected abstract Class<?> getNumericObservationClass();

    protected abstract Class<?> getSweDataArrayObservationClass();

    protected abstract Class<?> getTextObservationClass();

    /**
     * Inner class to carry observation identifiers (featureOfInterest,
     * observableProperty, procedure)
     *
     * @author Carsten Hollmann <c.hollmann@52north.org>
     * @since 4.0.0
     *
     */
    protected class ObservationIdentifiers {
    
        FeatureOfInterest featureOfInterest;
    
        ObservableProperty observableProperty;
    
        Procedure procedure;
    
        /**
         * @return the featureOfInterest
         */
        public FeatureOfInterest getFeatureOfInterest() {
            return featureOfInterest;
        }
    
        /**
         * @param featureOfInterest
         *            the featureOfInterest to set
         */
        public void setFeatureOfInterest(FeatureOfInterest featureOfInterest) {
            this.featureOfInterest = featureOfInterest;
        }
    
        /**
         * @return the observableProperty
         */
        public ObservableProperty getObservableProperty() {
            return observableProperty;
        }
    
        /**
         * @param observableProperty
         *            the observableProperty to set
         */
        public void setObservableProperty(ObservableProperty observableProperty) {
            this.observableProperty = observableProperty;
        }
    
        /**
         * @return the procedure
         */
        public Procedure getProcedure() {
            return procedure;
        }
    
        /**
         * @param procedure
         *            the procedure to set
         */
        public void setProcedure(Procedure procedure) {
            this.procedure = procedure;
        }
    
    }

    /**
     * Check if the observation table contains samplingGeometries with values
     * 
     * @param session
     *            Hibernate session
     * @return <code>true</code>, if the observation table contains samplingGeometries with values
     */
    public boolean containsSamplingGeometries(Session session) {
    	// TODOHZG: do observations have sampling geometry?
    	return false;
    }
}
