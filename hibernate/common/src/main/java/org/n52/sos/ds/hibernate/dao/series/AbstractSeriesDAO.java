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

import java.util.Collection;
import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.joda.time.DateTime;
import org.n52.sos.ds.hibernate.dao.ObservationValueFK;
import org.n52.sos.ds.hibernate.entities.AbstractObservation;
import org.n52.sos.ds.hibernate.entities.FeatureOfInterest;
import org.n52.sos.ds.hibernate.entities.ObservableProperty;
import org.n52.sos.ds.hibernate.entities.Procedure;
import org.n52.sos.ds.hibernate.entities.interfaces.NumericObservation;
import org.n52.sos.ds.hibernate.entities.series.Series;
import org.n52.sos.ds.hibernate.entities.series.SeriesObservation;
import org.n52.sos.ds.hibernate.util.HibernateHelper;
import org.n52.sos.ds.hibernate.util.TimeExtrema;
import org.n52.sos.exception.CodedException;
import org.n52.sos.exception.ows.NoApplicableCodeException;
import org.n52.sos.request.GetObservationRequest;
import org.n52.sos.service.SosContextListener;
import org.n52.sos.util.DateTimeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.hzg.common.SOSConfiguration;
import de.hzg.values.CalculatedData;
import de.hzg.values.RawData;

public abstract class AbstractSeriesDAO {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSeriesDAO.class);

    protected abstract Class<?> getSeriesClass();
    
    /**
     * Get series for GetObservation request and featuresOfInterest
     * 
     * @param request
     *            GetObservation request to get series for
     * @param features
     *            FeaturesOfInterest to get series for
     * @param session
     *            Hibernate session
     * @return Series that fit
     * @throws CodedException 
     */
    public abstract List<Series> getSeries(GetObservationRequest request, Collection<String> features, Session session) throws CodedException;
    
    /**
     * Query series for observedProiperty and featuresOfInterest
     * 
     * @param observedProperty
     *            ObservedProperty to get series for
     * @param features
     *            FeaturesOfInterest to get series for
     * @param session
     *            Hibernate session
     * @return Series list
     */
    public abstract List<Series> getSeries(String observedProperty, Collection<String> features, Session session);
    
    /**
     * Create series for parameter
     * 
     * @param procedures
     *            Procedures to get series for
     * @param observedProperties
     *            ObservedProperties to get series for
     * @param features
     *            FeaturesOfInterest to get series for
     * @param session
     *            Hibernate session
     * @return Series that fir
     */
    public abstract List<Series> getSeries(Collection<String> procedures, Collection<String> observedProperties,
            Collection<String> features, Session session);
    
    /**
     * Insert or update and get series for procedure, observable property and
     * featureOfInterest
     * 
     * @param feature
     *            FeatureOfInterest object
     * @param observableProperty
     *            ObservableProperty object
     * @param procedure
     *            Procedure object
     * @param session
     *            Hibernate session
     * @return Series object
     * @throws CodedException 
     */
    public abstract Series getOrInsertSeries(SeriesIdentifiers identifiers, final Session session) throws CodedException; 

    protected Series getOrInsert(SeriesIdentifiers identifiers, final Session session) throws CodedException {
        Criteria criteria = getDefaultAllSeriesCriteria(session);
        identifiers.addIdentifierRestrictionsToCritera(criteria);
        LOGGER.debug("QUERY getOrInsertSeries(feature, observableProperty, procedure): {}",
                HibernateHelper.getSqlString(criteria));
        Series series = (Series) criteria.uniqueResult();
        if (series == null) {
            series = getSeriesImpl();
            identifiers.addValuesToSeries(series);
            series.setDeleted(false);
            series.setPublished(true);
            session.save(series);
            session.flush();
            session.refresh(series);
        } else if (series.isDeleted()) {
            series.setDeleted(false);
            session.update(series);
            session.flush();
            session.refresh(series);
        }
        return series;
    }

    
    private Series getSeriesImpl() throws CodedException {
        try {
            return (Series)getSeriesClass().newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new NoApplicableCodeException().causedBy(e).withMessage("Error while creating an instance of %s", getSeriesClass().getCanonicalName());
        }
    }

    /**
     * Add featureOfInterest restriction to Hibernate Criteria
     * 
     * @param c
     *            Hibernate Criteria to add restriction
     * @param feature
     *            FeatureOfInterest identifier to add
     */
    public void addFeatureOfInterestToCriteria(Criteria c, String feature) {
        c.createCriteria(Series.FEATURE_OF_INTEREST).add(Restrictions.eq(FeatureOfInterest.IDENTIFIER, feature));

    }
    
    /**
     * Add featureOfInterest restriction to Hibernate Criteria
     * 
     * @param c
     *            Hibernate Criteria to add restriction
     * @param feature
     *            FeatureOfInterest to add
     */
    public void addFeatureOfInterestToCriteria(Criteria c, FeatureOfInterest feature) {
        c.add(Restrictions.eq(Series.FEATURE_OF_INTEREST, feature));

    }

    /**
     * Add featuresOfInterest restriction to Hibernate Criteria
     * 
     * @param c
     *            Hibernate Criteria to add restriction
     * @param features
     *            FeatureOfInterest identifiers to add
     */
    public void addFeatureOfInterestToCriteria(Criteria c, Collection<String> features) {
        c.createCriteria(Series.FEATURE_OF_INTEREST).add(Restrictions.in(FeatureOfInterest.IDENTIFIER, features));
    
    }

    /**
     * Add observedProperty restriction to Hibernate Criteria
     * 
     * @param c
     *            Hibernate Criteria to add restriction
     * @param observedProperty
     *            ObservableProperty identifier to add
     */
    public void addObservablePropertyToCriteria(Criteria c, String observedProperty) {
        c.createCriteria(Series.OBSERVABLE_PROPERTY).add(
                Restrictions.eq(ObservableProperty.IDENTIFIER, observedProperty));
    }
    
    /**
     * Add observedProperty restriction to Hibernate Criteria
     * 
     * @param c
     *            Hibernate Criteria to add restriction
     * @param observedProperty
     *            ObservableProperty to add
     */
    public void addObservablePropertyToCriteria(Criteria c, ObservableProperty observedProperty) {
        c.add(Restrictions.eq(Series.OBSERVABLE_PROPERTY, observedProperty));
    }

    /**
     * Add observedProperties restriction to Hibernate Criteria
     * 
     * @param c
     *            Hibernate Criteria to add restriction
     * @param observedProperties
     *            ObservableProperty identifiers to add
     */
    public void addObservablePropertyToCriteria(Criteria c, Collection<String> observedProperties) {
        c.createCriteria(Series.OBSERVABLE_PROPERTY).add(
                Restrictions.in(ObservableProperty.IDENTIFIER, observedProperties));
    }

    /**
     * Add procedure restriction to Hibernate Criteria
     * 
     * @param c
     *            Hibernate Criteria to add restriction
     * @param procedure
     *            Procedure identifier to add
     */
    public void addProcedureToCriteria(Criteria c, String procedure) {
        c.createCriteria(Series.PROCEDURE).add(Restrictions.eq(Procedure.IDENTIFIER, procedure));
    }
    
    /**
     * Add procedure restriction to Hibernate Criteria
     * 
     * @param c
     *            Hibernate Criteria to add restriction
     * @param procedure
     *            Procedure to add
     */
    public void addProcedureToCriteria(Criteria c, Procedure procedure) {
        c.add(Restrictions.eq(Series.PROCEDURE, procedure));

    }

    /**
     * Add procedures restriction to Hibernate Criteria
     * 
     * @param c
     *            Hibernate Criteria to add restriction
     * @param procedures
     *            Procedure identifiers to add
     */
    public void addProcedureToCriteria(Criteria c, Collection<String> procedures) {
        c.createCriteria(Series.PROCEDURE).add(Restrictions.in(Procedure.IDENTIFIER, procedures));
    
    }

    /**
     * Get default Hibernate Criteria for querying all series
     * 
     * @param session
     *            Hibernate Session
     * @return Default criteria
     */
    public Criteria getDefaultAllSeriesCriteria(Session session) {
        return session.createCriteria(getSeriesClass()).setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
    }
    
    /**
     * Update Series for procedure by setting deleted flag and return changed
     * series
     * 
     * @param procedure
     *            Procedure for which the series should be changed
     * @param deleteFlag
     *            New deleted flag value
     * @param session
     *            Hibernate session
     * @return Updated Series
     */
    @SuppressWarnings("unchecked")
    public List<Series> updateSeriesSetAsDeletedForProcedureAndGetSeries(String procedure, boolean deleteFlag,
            Session session) {
        Criteria criteria = getDefaultAllSeriesCriteria(session);
        addProcedureToCriteria(criteria, procedure);
        List<Series> hSeries = criteria.list();
        for (Series series : hSeries) {
            series.setDeleted(deleteFlag);
            session.saveOrUpdate(series);
            session.flush();
        }
        return hSeries;
    }
    
    /**
     * Update series values which will be used by the Timeseries API.
     * Can be later used by the SOS.
     * 
     * @param series Series object
     * @param hObservation Observation object
     * @param session Hibernate session
     */
    public void updateSeriesWithFirstLatestValues(Series series, AbstractObservation hObservation, Session session) {
        boolean minChanged = false;
        boolean maxChanged = false;
        if (!series.isSetFirstTimeStamp() || (series.isSetFirstTimeStamp() && series.getFirstTimeStamp().after(hObservation.getPhenomenonTimeStart()))) {
            minChanged = true;
            series.setFirstTimeStamp(hObservation.getPhenomenonTimeStart());
        }
        if (!series.isSetLastTimeStamp() || (series.isSetLastTimeStamp() && series.getLastTimeStamp().before(hObservation.getPhenomenonTimeEnd()))) {
            maxChanged = true;
            series.setLastTimeStamp(hObservation.getPhenomenonTimeEnd());
        }

        if (hObservation instanceof NumericObservation) {
            if (minChanged) {
                series.setFirstNumericValue(((NumericObservation) hObservation).getValue());
            }
            if (maxChanged) {
                series.setLastNumericValue(((NumericObservation) hObservation).getValue());
            }
            if (!series.isSetUnit() && hObservation.isSetUnit()) {
                // TODO check if both unit are equal. If not throw exception?
                series.setUnit(hObservation.getUnit());
            }
        }
        session.saveOrUpdate(series);
        session.flush();
    }
    
	/**
	 * Check {@link Series} if the deleted observation time stamp corresponds to
	 * the first/last series time stamp
	 * 
	 * @param series
	 *            Series to update
	 * @param observation
	 *            Deleted observation
	 * @param session
	 *            Hibernate session
	 */
	public void updateSeriesAfterObservationDeletion(Series series, SeriesObservation observation, Session session) {
		SeriesObservationDAO seriesObservationDAO = new SeriesObservationDAO();
		if (series.getFirstTimeStamp().equals(observation.getPhenomenonTimeStart())) {
			SeriesObservation firstObservation = seriesObservationDAO.getFirstObservationFor(series, session);
			series.setFirstTimeStamp(firstObservation.getPhenomenonTimeStart());
			if (firstObservation instanceof NumericObservation) {
				series.setFirstNumericValue(((NumericObservation) firstObservation).getValue());
			}
		} else if (series.getLastTimeStamp().equals(observation.getPhenomenonTimeEnd())) {
			SeriesObservation latestObservation = seriesObservationDAO.getLastObservationFor(series, session);
			series.setLastTimeStamp(latestObservation.getPhenomenonTimeEnd());
			if (latestObservation instanceof NumericObservation) {
				series.setLastNumericValue(((NumericObservation) latestObservation).getValue());
			}
		}
		session.saveOrUpdate(series);
	}
	
	private Criteria addRestrictionsToTimeExtremaCriteria(Criteria criteria, String procedureName) {
		return criteria
			.createAlias("observedPropertyInstance", "opi")
			.createAlias("opi.sensor", "sensor")
			.add(Restrictions.eq("sensor.name", procedureName))
			.setProjection(Projections.projectionList()
				.add(Projections.min("date"))
				.add(Projections.max("date")));
	}

	public TimeExtrema getProcedureTimeExtrema(Session session, String procedure) {
		final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;

		if (procedure.startsWith(sosConfiguration.getProcedureIdentifierPrefix())) {
			return new TimeExtrema();
		}

		final String procedureName = procedure.substring(sosConfiguration.getProcedureIdentifierPrefix().length());
		final Criteria rawCriteria = session.createCriteria(RawData.class);
		final Criteria calcCriteria = session.createCriteria(CalculatedData.class);

		addRestrictionsToTimeExtremaCriteria(rawCriteria, procedureName);
		addRestrictionsToTimeExtremaCriteria(calcCriteria, procedureName);

		final Object[] rawResult = (Object[]) rawCriteria.uniqueResult();
		final Object[] calcResult = (Object[]) calcCriteria.uniqueResult();

		DateTime min = null;
		DateTime max = null;

		if (rawResult != null) {
			min = DateTimeHelper.makeDateTime(rawResult[0]);
			max = DateTimeHelper.makeDateTime(rawResult[1]);
		}

		if (calcResult != null) {
			final DateTime calcMin = DateTimeHelper.makeDateTime(rawResult[0]);
			final DateTime calcMax = DateTimeHelper.makeDateTime(rawResult[1]);

			if (calcMin.isBefore(min)) {
				min = calcMin;
			}

			if (calcMax.isAfter(max)) {
				max = calcMax;
			}
		}

		final TimeExtrema pte = new TimeExtrema();

		if (min != null && max != null) {
			pte.setMinTime(min);
			pte.setMaxTime(max);
		}

		return pte;
	}

	public abstract <T extends Series> void setValueFK(T series, ObservationValueFK valueFK);
}
