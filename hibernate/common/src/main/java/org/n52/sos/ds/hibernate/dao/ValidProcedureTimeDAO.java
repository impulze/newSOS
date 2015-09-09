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
import java.util.Map;
import java.util.Set;

import org.hibernate.Session;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.n52.sos.ds.hibernate.entities.Procedure;
import org.n52.sos.ds.hibernate.entities.ProcedureDescriptionFormat;
import org.n52.sos.ds.hibernate.entities.TProcedure;
import org.n52.sos.ds.hibernate.entities.ValidProcedureTime;
import org.n52.sos.exception.ows.concrete.UnsupportedOperatorException;
import org.n52.sos.exception.ows.concrete.UnsupportedTimeException;
import org.n52.sos.exception.ows.concrete.UnsupportedValueReferenceException;
import org.n52.sos.ogc.gml.time.Time;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Hibernate data access class for valid procedure time
 * 
 * @author CarstenHollmann
 * @since 4.0.0
 */
public class ValidProcedureTimeDAO {
    /**
     * Insert valid procedure time for procedrue
     * 
     * @param procedure
     *            Procedure object
     * @param xmlDescription
     *            Procedure XML description
     * @param validStartTime
     *            Valid start time
     * @param session
     *            Hibernate session
     */
    public void insertValidProcedureTime(Procedure procedure, ProcedureDescriptionFormat procedureDescriptionFormat,
            String xmlDescription, DateTime validStartTime, Session session) {
        throw new RuntimeException("Insertion of valid procedure times is not supported yet.");
    }

    /**
     * Update valid procedure time object
     * 
     * @param validProcedureTime
     *            Valid procedure time object
     * @param session
     *            Hibernate session
     */
    public void updateValidProcedureTime(ValidProcedureTime validProcedureTime, Session session) {
        throw new RuntimeException("Updating valid procedure times is not supported yet.");
    }

    /**
     * Set valid end time to valid procedure time object for procedure
     * identifier
     * 
     * @param procedureIdentifier
     *            Procedure identifier
     * @param session
     *            Hibernate session
     * @throws UnsupportedOperatorException
     * @throws UnsupportedValueReferenceException
     * @throws UnsupportedTimeException
     */
    public void setValidProcedureDescriptionEndTime(String procedureIdentifier, String procedureDescriptionFormat,
            Session session) throws UnsupportedTimeException, UnsupportedValueReferenceException,
            UnsupportedOperatorException {
        TProcedure procedure =
                new ProcedureDAO().getTProcedureForIdentifier(procedureIdentifier, procedureDescriptionFormat, null,
                        session);
        Set<ValidProcedureTime> validProcedureTimes = procedure.getValidProcedureTimes();
        for (ValidProcedureTime validProcedureTime : validProcedureTimes) {
            if (validProcedureTime.getEndTime() == null) {
                validProcedureTime.setEndTime(new DateTime(DateTimeZone.UTC).toDate());
            }
        }
    }

    /**
     * Set valid end time to valid procedure time object for procedure
     * identifier
     * 
     * @param procedureIdentifier
     *            Procedure identifier
     * @param session
     *            Hibernate session
     */
    public void setValidProcedureDescriptionEndTime(String procedureIdentifier, Session session) {
        TProcedure procedure = new ProcedureDAO().getTProcedureForIdentifierIncludeDeleted(procedureIdentifier, session);
        Set<ValidProcedureTime> validProcedureTimes = procedure.getValidProcedureTimes();
        for (ValidProcedureTime validProcedureTime : validProcedureTimes) {
            if (validProcedureTime.getEndTime() == null) {
                validProcedureTime.setEndTime(new DateTime(DateTimeZone.UTC).toDate());
            }
        }
    }

    /**
     * Get ValidProcedureTimes for requested parameters
     * 
     * @param procedure
     *            Requested Procedure
     * @param procedureDescriptionFormat
     *            Requested procedureDescriptionFormat
     * @param validTime
     *            Requested validTime (optional)
     * @param session
     *            Hibernate session
     * @return List with ValidProcedureTime objects
     * @throws UnsupportedTimeException
     *             If validTime time value is invalid
     * @throws UnsupportedValueReferenceException
     *             If valueReference is not supported
     * @throws UnsupportedOperatorException
     *             If temporal operator is not supported
     */
    public List<ValidProcedureTime> getValidProcedureTimes(Procedure procedure, String procedureDescriptionFormat,
            Time validTime, Session session) throws UnsupportedTimeException, UnsupportedValueReferenceException,
            UnsupportedOperatorException {
        throw new RuntimeException("Getting valid procedure times of non-transactional procedures is not supported yet.");
    }

    public List<ValidProcedureTime> getValidProcedureTimes(TProcedure procedure,
            Set<String> possibleProcedureDescriptionFormats, Time validTime, Session session)
            throws UnsupportedTimeException, UnsupportedValueReferenceException, UnsupportedOperatorException {
        // don't check time
        final Set<ValidProcedureTime> validProcedureTimes = procedure.getValidProcedureTimes();
        final List<ValidProcedureTime> validProcedureTimesList = Lists.newArrayListWithCapacity(validProcedureTimes.size());

        validProcedureTimesList.addAll(validProcedureTimes);

        return validProcedureTimesList;
    }

    public Map<String,String> getTProcedureFormatMap(Session session) {
        final List<Procedure> procedures = new ProcedureDAO().getProcedureObjects(session);
        final Map<String, String> map = Maps.newHashMapWithExpectedSize(procedures.size());

        for (final Procedure procedure: procedures) {
            map.put(procedure.getIdentifier(), procedure.getProcedureDescriptionFormat().getProcedureDescriptionFormat());
        }

        return map;
    }
}
