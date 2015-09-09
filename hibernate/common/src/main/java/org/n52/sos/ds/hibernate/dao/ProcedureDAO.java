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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.hibernate.Criteria;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.n52.sos.ds.I18NDAO;
import org.n52.sos.ds.hibernate.entities.FeatureOfInterest;
import org.n52.sos.ds.hibernate.entities.ObservableProperty;
import org.n52.sos.ds.hibernate.entities.Procedure;
import org.n52.sos.ds.hibernate.entities.ProcedureDescriptionFormat;
import org.n52.sos.ds.hibernate.entities.TProcedure;
import org.n52.sos.ds.hibernate.entities.Unit;
import org.n52.sos.ds.hibernate.entities.ValidProcedureTime;
import org.n52.sos.ds.hibernate.util.TimeExtrema;
import org.n52.sos.exception.CodedException;
import org.n52.sos.exception.ows.concrete.UnsupportedOperatorException;
import org.n52.sos.exception.ows.concrete.UnsupportedTimeException;
import org.n52.sos.exception.ows.concrete.UnsupportedValueReferenceException;
import org.n52.sos.i18n.I18NDAORepository;
import org.n52.sos.i18n.LocalizedString;
import org.n52.sos.i18n.metadata.I18NProcedureMetadata;
import org.n52.sos.ogc.OGCConstants;
import org.n52.sos.ogc.gml.CodeWithAuthority;
import org.n52.sos.ogc.gml.time.Time;
import org.n52.sos.ogc.ows.OwsExceptionReport;
import org.n52.sos.ogc.sensorML.SensorML20Constants;
import org.n52.sos.ogc.sensorML.elements.SmlIdentifier;
import org.n52.sos.ogc.sensorML.elements.SmlIo;
import org.n52.sos.ogc.sensorML.v20.PhysicalSystem;
import org.n52.sos.ogc.swe.SweAbstractDataComponent;
import org.n52.sos.ogc.swe.SweDataArray;
import org.n52.sos.ogc.swe.SweDataRecord;
import org.n52.sos.ogc.swe.SweField;
import org.n52.sos.ogc.swe.simpleType.SweQuantity;
import org.n52.sos.ogc.swe.simpleType.SweText;
import org.n52.sos.ogc.swe.simpleType.SweTime;
import org.n52.sos.service.ServiceConfiguration;
import org.n52.sos.service.SosContextListener;
import org.n52.sos.util.CodingHelper;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import de.hzg.common.SOSConfiguration;
import de.hzg.measurement.CalibrationSet;
import de.hzg.measurement.ObservedPropertyInstance;
import de.hzg.measurement.Sensor;
import de.hzg.values.CalculatedData;

/**
 * Hibernate data access class for procedure
 *
 * @author CarstenHollmann
 * @since 4.0.0
 */
public class ProcedureDAO extends AbstractIdentifierNameDescriptionDAO implements HibernateSqlQueryConstants {
    //public class ProcedureDAO extends TimeCreator implements HibernateSqlQueryConstants {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcedureDAO.class);

	public static void fillProcedureDescriptionXML(TProcedure tprocedure, Sensor sensor, Session session) {
		for (final ValidProcedureTime validProcedureTime: tprocedure.getValidProcedureTimes()) {
			fillProcedureDescriptionXML(tprocedure, validProcedureTime, sensor, session);
		}
	}

	public static void fillProcedureDescriptionXML(TProcedure tprocedure, ValidProcedureTime validProcedureTime, Sensor sensor, Session session) {
        final PhysicalSystem physicalSystem = new PhysicalSystem();
        final CodeWithAuthority codeWithAuthority = new CodeWithAuthority(tprocedure.getIdentifier());

        if (tprocedure.isSetCodespace()) {
        	codeWithAuthority.setCodeSpace(tprocedure.getCodespace().getCodespace());
        } else {
        	codeWithAuthority.setCodeSpace(OGCConstants.UNIQUE_ID);
        }

        final I18NDAO<I18NProcedureMetadata> i18nDAO = I18NDAORepository.getInstance().getDAO(I18NProcedureMetadata.class);
        final Locale defaultLocale = ServiceConfiguration.getInstance().getDefaultLanguage();
        final I18NProcedureMetadata i18n;

        try{
            if (ServiceConfiguration.getInstance().isShowAllLanguageValues()) {
                // load all names
                i18n = i18nDAO.getMetadata(tprocedure.getIdentifier());
            } else {
                // load only name in default locale
                i18n = i18nDAO.getMetadata(tprocedure.getIdentifier(), defaultLocale);
            }

            for (LocalizedString name : i18n.getName()) {
                // either all or default only
                physicalSystem.addName(name.asCodeType());
            }

            // choose always the description in the default locale
            Optional<LocalizedString> description = i18n.getDescription().getLocalization(defaultLocale);
            if (description.isPresent()) {
                physicalSystem.setDescription(description.get().getText());
            }
        } catch (OwsExceptionReport e) {
        }

        physicalSystem.setIdentifier(tprocedure.getIdentifier());
        // TODO: set position?

        final List<SmlIdentifier> identifications = new ArrayList<SmlIdentifier>();
        final SmlIdentifier identification = new SmlIdentifier(OGCConstants.URN_UNIQUE_IDENTIFIER_END, OGCConstants.URN_UNIQUE_IDENTIFIER, tprocedure.getIdentifier());

        identifications.add(identification);

        physicalSystem.setIdentifications(identifications);

    	final List<ObservedPropertyInstance> observedPropertyInstances = sensor.getObservedPropertyInstances();
        final List<SmlIo<?>> outputs = new ArrayList<SmlIo<?>>();
        int i = 0;

        final TimeZone utcTimeZone = TimeZone.getTimeZone("UTC");
        final DateFormat isoDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");

        isoDateFormat.setTimeZone(utcTimeZone);

    	for (final ObservedPropertyInstance observedPropertyInstance: observedPropertyInstances) {
    		final ObservableProperty observableProperty = ObservablePropertyDAO.createObservableProperty(observedPropertyInstance, session);
			final SweDataRecord outputRecord = new SweDataRecord();
			int j = 0;

			for (final CalibrationSet calibrationSet: observedPropertyInstance.getCalibrationSets()) {
				final SweDataRecord calibrationArrayElement = new SweDataRecord();

				calibrationArrayElement.addField(new SweField("parameter 1", new SweText().setValue(calibrationSet.getParameter1().toString())));
				calibrationArrayElement.addField(new SweField("parameter 2", new SweText().setValue(calibrationSet.getParameter2().toString())));
				calibrationArrayElement.addField(new SweField("parameter 3", new SweText().setValue(calibrationSet.getParameter3().toString())));
				calibrationArrayElement.addField(new SweField("parameter 4", new SweText().setValue(calibrationSet.getParameter4().toString())));
				calibrationArrayElement.addField(new SweField("parameter 5", new SweText().setValue(calibrationSet.getParameter5().toString())));
				calibrationArrayElement.addField(new SweField("parameter 6", new SweText().setValue(calibrationSet.getParameter6().toString())));
				calibrationArrayElement.addField(new SweField("valid start", new SweText().setValue(isoDateFormat.format(calibrationSet.getValidStart()))));
				calibrationArrayElement.addField(new SweField("valid end", new SweText().setValue(isoDateFormat.format(calibrationSet.getValidEnd()))));
				//calibrationArrayElement.addField(new SweField("valid start", new SweTime().setValue(new DateTime(calibrationSet.getValidEnd()))));
				//calibrationArrayElement.addField(new SweField("valid end", new SweTime().setValue(new DateTime(calibrationSet.getValidEnd()))));
				

				outputRecord.addField(new SweField("calibration set " + j++, calibrationArrayElement));
			}

			final Unit unit = new UnitDAO().getUnitFromObservedPropertyInstance(observedPropertyInstance);

			if (unit != null) {
				outputRecord.addField(new SweField("unit", new SweText().setValue(unit.getUnit())));
			}

			outputRecord.addField(new SweField("identifier", new SweText().setValue(observableProperty.getIdentifier())));
			outputRecord.setDefinition("output data record " + i);
    		final SmlIo<?> output = new SmlIo<>(outputRecord);//String>(new SweText().setValue("hi").setDefinition(observableProperty.getIdentifier()));//outputRecord);
    		output.setIoName("output#" + i++);
    		outputs.add(output);
    	}

    	physicalSystem.setOutputs(outputs);

        /*
        try {
			final List<String> lines = Files.readAllLines(Paths.get("/home/impulze/procedure.xml"), StandardCharsets.UTF_8);
			final StringBuilder builder = new StringBuilder();

			for (final String line: lines) {
				builder.append(line);
				builder.append('\n');
			}

			validProcedureTime.setDescriptionXml(builder.toString());
		} catch (IOException e) {
		}*/
        try {
			validProcedureTime.setDescriptionXml(CodingHelper.encodeObjectToXmlText(SensorML20Constants.NS_SML_20, physicalSystem));
		} catch (OwsExceptionReport e) {
			throw new RuntimeException("Setting XML description of procedure failed." + e);

		}
		
	}

	public static TProcedure createTProcedureNoDescriptionXML(Sensor sensor, Session session) {
        final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;
        final TProcedure tprocedure = new TProcedure();
        final ValidProcedureTime validProcedureTime = new ValidProcedureTime();

        validProcedureTime.setStartTime(new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC).toDate());
        validProcedureTime.setEndTime(new DateTime(2016, 1, 1, 0, 0, DateTimeZone.UTC).toDate());
        validProcedureTime.setProcedure(tprocedure);
        validProcedureTime.setProcedureDescriptionFormat(new ProcedureDescriptionFormatDAO().getProcedureDescriptionFormatObject(ProcedureDescriptionFormatDAO.HZG_PROCEDURE_DESCRIPTION_FORMAT, session));

        tprocedure.setValidProcedureTimes(Sets.newHashSet(validProcedureTime));
        tprocedure.setIdentifier(sosConfiguration.getProcedureIdentifierPrefix() + sensor.getName());
        tprocedure.setProcedureDescriptionFormat(new ProcedureDescriptionFormatDAO().getProcedureDescriptionFormatObject(ProcedureDescriptionFormatDAO.HZG_PROCEDURE_DESCRIPTION_FORMAT, session));
        tprocedure.setDeleted(false);
        tprocedure.setDisabled(false);

        return tprocedure;
	}

    public static TProcedure createTProcedure(Sensor sensor, Session session) {
    	final TProcedure procedure = createTProcedureNoDescriptionXML(sensor, session);

    	fillProcedureDescriptionXML(procedure, sensor, session);

    	return procedure;
    }

	public List<Sensor> getSensorsForIdentifiers(Collection<String> identifiers, Session session) {
		final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;
        final List<String> names = Lists.newArrayListWithCapacity(identifiers.size());

        for (final String identifier: identifiers) {
            if (identifier.startsWith(sosConfiguration.getProcedureIdentifierPrefix())) {
                names.add(identifier.substring(sosConfiguration.getProcedureIdentifierPrefix().length()));
            }
        }

        final Criteria criteria = session.createCriteria(Sensor.class)
                .add(Restrictions.in("name", names));
        @SuppressWarnings("unchecked")
		final List<Sensor> sensors = criteria.list();

		return sensors;
	}

    /**
     * Get all procedure objects
     *
     * @param session
     *            Hibernate session
     * @return Procedure objects
     */
    @SuppressWarnings("unchecked")
    public List<Procedure> getProcedureObjects(final Session session) {
        final Criteria criteria = session.createCriteria(Sensor.class);
        final List<Sensor> sensors = criteria.list();
        final List<Procedure> procedures = Lists.newArrayList();

        for (final Sensor sensor: sensors) {
            procedures.add(createTProcedure(sensor, session));
        }

        return procedures;
    }

    /**
     * Get map keyed by undeleted procedure identifiers with
     * collections of parent procedures (if supported) as values
     * @param session
     * @return Map keyed by procedure identifier with values of parent procedure identifier collections
     */
    public Map<String,Collection<String>> getProcedureIdentifiers(final Session session) {
        final List<Procedure> procedures = getProcedureObjects(session);
        final Map<String, Collection<String>> map = Maps.newHashMap();

        for (final Procedure procedure: procedures) {
            map.put(procedure.getIdentifier(), null);
        }

        return map;
    }

    /**
     * Get Procedure object for procedure identifier
     *
     * @param identifier
     *            Procedure identifier
     * @param session
     *            Hibernate session
     * @return Procedure object
     */
    public Procedure getProcedureForIdentifier(final String identifier, final Session session) {
        return getTProcedureForIdentifier(identifier, session);
    }

    /**
     * Get Procedure object for procedure identifier inclusive deleted procedure
     *
     * @param identifier
     *            Procedure identifier
     * @param session
     *            Hibernate session
     * @return Procedure object
     */
    public Procedure getProcedureForIdentifierIncludeDeleted(final String identifier, final Session session) {
        return getProcedureForIdentifier(identifier, session);
    }

    /**
     * Get Procedure object for procedure identifier
     *
     * @param identifier
     *            Procedure identifier
     * @param session
     *            Hibernate session
     * @return Procedure object
     */
    public Procedure getProcedureForIdentifier(final String identifier, Time time, final Session session) {
        return getProcedureForIdentifier(identifier, session);
    }

    /**
     * Get Procedure objects for procedure identifiers
     *
     * @param identifiers
     *            Procedure identifiers
     * @param session
     *            Hibernate session
     * @return Procedure objects
     */
    public List<Procedure> getProceduresForIdentifiers(final Collection<String> identifiers, final Session session) {
    	final List<Sensor> sensors = getSensorsForIdentifiers(identifiers, session);
        final List<Procedure> procedures = Lists.newArrayList();

        for (final Sensor sensor: sensors) {
            procedures.add(createTProcedure(sensor, session));
        }

        return procedures;
    }

    /**
     * Get procedure identifiers for all FOIs
     *
     * @param session
     *            Hibernate session
     *
     * @return Map of foi identifier to procedure identifier collection
     * @throws HibernateException 
     * @throws CodedException
     */
    public Map<String,Collection<String>> getProceduresForAllFeaturesOfInterest(final Session session) {
        final List<Object[]> results = getFeatureProcedureResult(session);

        // we only have one foi
        final Map<String, Collection<String>> map = Maps.newHashMap();

        for (final Object[] result: results) {
            final String foiIdentifier = (String)result[0];
            final String procedureIdentifier = (String)result[1];
            Collection<String> procedureIdentifiersForFOI = map.get(foiIdentifier);

            if (procedureIdentifiersForFOI == null) {
                procedureIdentifiersForFOI = Lists.newArrayList();
                map.put(foiIdentifier, procedureIdentifiersForFOI);
            }

            procedureIdentifiersForFOI.add(procedureIdentifier);
        }

        return map;
    }
    
    /**
     * Get FOIs for all procedure identifiers
     *
     * @param session
     *            Hibernate session
     *
     * @return Map of procedure identifier to foi identifier collection
     * @throws CodedException
     */
    public Map<String,Collection<String>> getFeaturesOfInterestsForAllProcedures(final Session session) {
        final List<Object[]> results = getFeatureProcedureResult(session);

        // we only have one foi
        final Map<String, Collection<String>> map = Maps.newHashMap();

        for (final Object[] result: results) {
            final String foiIdentifier = (String)result[0];
            final String procedureIdentifier = (String)result[1];
            Collection<String> foiIdentifiersForProcedure = map.get(procedureIdentifier);

            if (foiIdentifiersForProcedure == null) {
                foiIdentifiersForProcedure = Lists.newArrayList();
                map.put(procedureIdentifier, foiIdentifiersForProcedure);
            }

            foiIdentifiersForProcedure.add(foiIdentifier);
        }

        return map;
    }
    
    private List<Object[]> getFeatureProcedureResult(Session session) {
        // if the calculations are empty, this returns an empty list
        final Criteria calculatedDataCriteria = session.createCriteria(CalculatedData.class)
                .setProjection(Projections.rowCount());
        final Long calculatedDataCount = (Long)calculatedDataCriteria.uniqueResult();

        if (calculatedDataCount == 0) {
            return Collections.emptyList();
        }

        
        // otherwise return one foi to all procedures
        final List<Procedure> procedures = getProcedureObjects(session);
        final String foiIdentifier = new FeatureOfInterestDAO().getFeatureOfInterestObjects(session).get(0).getIdentifier();
        final List<Object[]> list = Lists.newArrayList();

        for (final Procedure procedure: procedures) {
            final Object[] objects = new Object[2];

            objects[0] = foiIdentifier;
            objects[1] = procedure.getIdentifier();

            list.add(objects);
        }

        return list;
    }

    /**
     * Get procedure identifiers for FOI
     *
     * @param session
     *            Hibernate session
     * @param feature
     *            FOI object
     *
     * @return Related procedure identifiers
     * @throws CodedException
     */
    public List<String> getProceduresForFeatureOfInterest(final Session session, final FeatureOfInterest feature)
            throws OwsExceptionReport {
        final List<Object[]> results = getFeatureProcedureResult(session);
        final List<String> procedureIdentifiers = Lists.newArrayList();

        for (final Object[] result: results) {
            procedureIdentifiers.add((String)result[1]);
        }

        return procedureIdentifiers;
    }

    /**
     * Get procedure identifiers for offering identifier
     *
     * @param offeringIdentifier
     *            Offering identifier
     * @param session
     *            Hibernate session
     * @return Procedure identifiers
     * @throws CodedException
     *             If an error occurs
     */
    public List<String> getProcedureIdentifiersForOffering(final String offeringIdentifier, final Session session)
            throws OwsExceptionReport {
        final List<Object[]> results = getFeatureProcedureResult(session);
        final List<String> procedureIdentifiers = Lists.newArrayList();

        for (final Object[] result: results) {
            procedureIdentifiers.add((String)result[1]);
        }

        return procedureIdentifiers;
    }

    /**
     * Get procedure identifiers for observable property identifier
     *
     * @param observablePropertyIdentifier
     *            Observable property identifier
     * @param session
     *            Hibernate session
     * @return Procedure identifiers
     * @throws CodedException 
     */
    public Collection<String> getProcedureIdentifiersForObservableProperty(final String observablePropertyIdentifier,
            final Session session) throws CodedException {
        // observable properties are procedures right now
        final List<Procedure> procedures = getProcedureObjects(session);
        final List<String> procedureIdentifiers = Lists.newArrayList();

        for (final Procedure procedure: procedures) {
            procedureIdentifiers.add(procedure.getIdentifier());
        }

        return procedureIdentifiers;
    }

    /**
     * Get transactional procedure object for procedure identifier
     *
     * @param identifier
     *            Procedure identifier
     * @param session
     *            Hibernate session
     * @return Transactional procedure object
     */
    public TProcedure getTProcedureForIdentifier(final String identifier, final Session session) {
    	final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;

        if (!identifier.startsWith(sosConfiguration.getProcedureIdentifierPrefix())) {
            return null;
        }

        final String name = identifier.substring(sosConfiguration.getProcedureIdentifierPrefix().length());

        final Criteria criteria = session.createCriteria(Sensor.class)
                .add(Restrictions.eq("name", name));
        final Sensor sensor = (Sensor)criteria.uniqueResult();

        if (sensor != null) {
            return createTProcedure(sensor, session);
        }

        return null;
    }

    /**
     * Get transactional procedure object for procedure identifier, include deleted
     *
     * @param identifier
     *            Procedure identifier
     * @param session
     *            Hibernate session
     * @return Transactional procedure object
     */
    public TProcedure getTProcedureForIdentifierIncludeDeleted(String identifier, Session session) {
        return getTProcedureForIdentifier(identifier, session);
    }

    /**
     * Get transactional procedure object for procedure identifier and
     * procedureDescriptionFormat
     *
     * @param identifier
     *            Procedure identifier
     * @param procedureDescriptionFormat
     *            ProcedureDescriptionFormat identifier
     * @param session
     *            Hibernate session
     * @return Transactional procedure object
     * @throws UnsupportedOperatorException
     * @throws UnsupportedValueReferenceException
     * @throws UnsupportedTimeException
     */
    public TProcedure getTProcedureForIdentifier(final String identifier, String procedureDescriptionFormat,
            Time validTime, final Session session) throws UnsupportedTimeException,
            UnsupportedValueReferenceException, UnsupportedOperatorException {
        // we don't switch pdfs for procedures and if the format is sensor ml 2.0 we're good
        if (!procedureDescriptionFormat.equals(ProcedureDescriptionFormatDAO.HZG_PROCEDURE_DESCRIPTION_FORMAT)) {
            return null;
        }

        // we don't check valid time either
        return getTProcedureForIdentifier(identifier, session);
    }

    /**
     * Get procedure for identifier, possible procedureDescriptionFormats and
     * valid time
     *
     * @param identifier
     *            Identifier of the procedure
     * @param possibleProcedureDescriptionFormats
     *            Possible procedureDescriptionFormats
     * @param validTime
     *            Valid time of the procedure
     * @param session
     *            Hibernate Session
     * @return Procedure entity that match the parameters
     * @throws UnsupportedTimeException
     *             If the time is not supported
     * @throws UnsupportedValueReferenceException
     *             If the valueReference is not supported
     * @throws UnsupportedOperatorException
     *             If the temporal operator is not supported
     */
    public TProcedure getTProcedureForIdentifier(String identifier, Set<String> possibleProcedureDescriptionFormats,
            Time validTime, Session session) throws UnsupportedTimeException, UnsupportedValueReferenceException,
            UnsupportedOperatorException {
        for (final String possibleProcedureDescriptionFormat: possibleProcedureDescriptionFormats) {
            if (possibleProcedureDescriptionFormat.equals(ProcedureDescriptionFormatDAO.HZG_PROCEDURE_DESCRIPTION_FORMAT)) {
                return getTProcedureForIdentifier(identifier, session);
            }
        }

        return null;
    }

    public boolean isProcedureTimeExtremaNamedQuerySupported(Session session) {
        return false;
    }
    
    public TimeExtrema getProcedureTimeExtremaFromNamedQuery(Session session, String procedureIdentifier) {
        return null;
    }

    /**
     * Query procedure time extrema for the provided procedure identifier
     *
     * @param session
     * @param procedureIdentifier
     * @return ProcedureTimeExtrema
     * @throws CodedException
     */
    public TimeExtrema getProcedureTimeExtrema(final Session session, String procedureIdentifier)
            throws OwsExceptionReport {
        // same as in offering time extrema, minimum to future
        final DateTime minTime = getMinDate4Procedure(procedureIdentifier, session);
        final DateTime maxTime = getMaxDate4Procedure(procedureIdentifier, session);

        if (minTime != null && maxTime != null) {
            final TimeExtrema timeExtrema = new TimeExtrema();

            timeExtrema.setMinTime(minTime);
            timeExtrema.setMaxTime(maxTime);

            return timeExtrema;
        }

        return null;
    }

    /**
     * Get min time from observations for procedure
     *
     * @param procedure
     *            Procedure identifier
     * @param session
     *            Hibernate session
     * @return min time for procedure
     * @throws CodedException
     */
    public DateTime getMinDate4Procedure(final String procedure, final Session session) throws OwsExceptionReport {
        final Procedure procedureType = getProcedureForIdentifier(procedure, session);

        if (procedureType == null) {
            return null;
        }

        final Criteria criteria = session.createCriteria(CalculatedData.class)
                .setProjection(Projections.projectionList()
                        .add(Projections.min("date")));
        final Timestamp timestamp = (Timestamp)criteria.uniqueResult();

        if (timestamp == null) {
            return null;
        }

        return new DateTime(timestamp, DateTimeZone.UTC);
    }

    /**
     * Get max time from observations for procedure
     *
     * @param procedure
     *            Procedure identifier
     * @param session
     *            Hibernate session
     * @return max time for procedure
     * @throws CodedException
     */
    public DateTime getMaxDate4Procedure(final String procedure, final Session session) throws OwsExceptionReport {
        final Procedure procedureType = getProcedureForIdentifier(procedure, session);

        if (procedureType == null) {
            return null;
        }

        return new DateTime(2025, 1, 1, 0, 0, DateTimeZone.UTC);
    }

    /**
     * Insert and get procedure object
     *
     * @param identifier
     *            Procedure identifier
     * @param procedureDecriptionFormat
     *            Procedure description format object
     * @param parentProcedures
     *            Parent procedure identifiers
     * @param session
     *            Hibernate session
     * @return Procedure object
     */
    public Procedure getOrInsertProcedure(final String identifier,
            final ProcedureDescriptionFormat procedureDecriptionFormat, final Collection<String> parentProcedures,
            final Session session) {
        final Procedure procedure = getProcedureForIdentifierIncludeDeleted(identifier, session);

        if (procedure != null) {
            return procedure;
        }

        throw new RuntimeException("Insertion of procedures is not yet supported.");
    }

    public Map<String,String> getProcedureFormatMap(Session session) {
        return new ValidProcedureTimeDAO().getTProcedureFormatMap(session);
    }
}
