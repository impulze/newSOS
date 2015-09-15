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
package org.n52.sos.ds.hibernate.util;

import java.util.Collection;
import java.util.Map;

import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Disjunction;
import org.n52.sos.ds.hibernate.entities.Observation;
import org.n52.sos.ds.hibernate.entities.ValidProcedureTime;
import org.n52.sos.ds.hibernate.util.TemporalRestriction.AfterRestriction;
import org.n52.sos.ds.hibernate.util.TemporalRestriction.BeforeRestriction;
import org.n52.sos.ds.hibernate.util.TemporalRestriction.BeginsRestriction;
import org.n52.sos.ds.hibernate.util.TemporalRestriction.BegunByRestriction;
import org.n52.sos.ds.hibernate.util.TemporalRestriction.ContainsRestriction;
import org.n52.sos.ds.hibernate.util.TemporalRestriction.DuringRestriction;
import org.n52.sos.ds.hibernate.util.TemporalRestriction.EndedByRestriction;
import org.n52.sos.ds.hibernate.util.TemporalRestriction.EndsRestriction;
import org.n52.sos.ds.hibernate.util.TemporalRestriction.MeetsRestriction;
import org.n52.sos.ds.hibernate.util.TemporalRestriction.MetByRestriction;
import org.n52.sos.ds.hibernate.util.TemporalRestriction.OverlappedByRestriction;
import org.n52.sos.ds.hibernate.util.TemporalRestriction.OverlapsRestriction;
import org.n52.sos.ds.hibernate.util.TemporalRestriction.TEqualsRestriction;
import org.n52.sos.exception.ows.concrete.UnsupportedOperatorException;
import org.n52.sos.exception.ows.concrete.UnsupportedTimeException;
import org.n52.sos.exception.ows.concrete.UnsupportedValueReferenceException;
import org.n52.sos.ogc.filter.FilterConstants.TimeOperator;
import org.n52.sos.ogc.filter.TemporalFilter;
import org.n52.sos.ogc.gml.time.Time;
import org.n52.sos.util.CollectionHelper;

import com.google.common.collect.Maps;

/**
 * Factory methods to create {@link Criterion Criterions} for
 * {@link TemporalFilter TemporalFilters}.
 * 
 * @see AfterRestriction
 * @see BeforeRestriction
 * @see BeginsRestriction
 * @see BegunByRestriction
 * @see ContainsRestriction
 * @see DuringRestriction
 * @see EndedByRestriction
 * @see EndsRestriction
 * @see TEqualsRestriction
 * @see MeetsRestriction
 * @see MetByRestriction
 * @see OverlappedByRestriction
 * @see OverlapsRestriction
 * @author Christian Autermann <c.autermann@52north.org>
 * @since 4.0.0
 */
public class TemporalRestrictions {
    /**
     * Marker for a value reference referencing the phenomenon time ({@value} ).
     * 
     * @see #PHENOMENON_TIME_FIELDS
     */
    public static final String PHENOMENON_TIME_VALUE_REFERENCE = "phenomenonTime";

    /**
     * Marker for a value reference referencing the result time ({@value} ).
     * 
     * @see #RESULT_TIME_FIELDS
     */
    public static final String RESULT_TIME_VALUE_REFERENCE = "resultTime";

    /**
     * Marker for a value reference referencing the valid time ({@value} ).
     * 
     * @see #VALID_TIME_FIELDS
     */
    public static final String VALID_TIME_VALUE_REFERENCE = "validTime";

    /**
     * Marker for a value reference referencing the valid time ({@value} ).
     * 
     * @see #VALID_TIME_FIELDS
     */
    public static final String VALID_DESCRIBE_SENSOR_TIME_VALUE_REFERENCE = "validDescribeSensorTime";

    /**
     * Fields describing the phenomenon time of a <tt>Observation</tt>.
     * 
     * @see Observation#PHENOMENON_TIME_START
     * @see Observation#PHENOMENON_TIME_END
     */
    public static final TimePrimitiveFieldDescriptor PHENOMENON_TIME_FIELDS = new TimePrimitiveFieldDescriptor(
            Observation.PHENOMENON_TIME_START, Observation.PHENOMENON_TIME_END);

    /**
     * Fields describing the result time of a <tt>Observation</tt>.
     * 
     * @see Observation#RESULT_TIME
     */
    public static final TimePrimitiveFieldDescriptor RESULT_TIME_FIELDS = new TimePrimitiveFieldDescriptor(
            Observation.RESULT_TIME);

    /**
     * Fields describing the valid time of a <tt>Observation</tt>.
     * 
     * @see Observation#VALID_TIME_START
     * @see Observation#VALID_TIME_END
     */
    public static final TimePrimitiveFieldDescriptor VALID_TIME_FIELDS = new TimePrimitiveFieldDescriptor(
            Observation.VALID_TIME_START, Observation.VALID_TIME_END);

    /**
     * Fields describing the valid time of a <tt>ValidProcedureTime</tt>.
     * 
     * @see ValidProcedureTime#START_TIME
     * @see ValidProcedureTime#END_TIME
     */
    public static final TimePrimitiveFieldDescriptor VALID_TIME_DESCRIBE_SENSOR_FIELDS =
            new TimePrimitiveFieldDescriptor(ValidProcedureTime.START_TIME, ValidProcedureTime.END_TIME);

    /**
     * Create a new <tt>Criterion</tt> using the specified filter.
     * 
     * @param filter
     *            the filter
     * 
     * @return the <tt>Criterion</tt>
     * 
     * @throws UnsupportedTimeException
     *             if the value and property combination is not applicable for
     *             this restriction
     * @throws UnsupportedValueReferenceException
     *             if the {@link TemporalFilter#getValueReference() value
     *             reference} can not be decoded
     * @throws UnsupportedOperatorException
     *             if no restriction definition for the {@link TimeOperator} is
     *             found
     */
    public static TimeCriterion filter(TemporalFilter filter) throws UnsupportedTimeException,
            UnsupportedValueReferenceException, UnsupportedOperatorException {
        TimePrimitiveFieldDescriptor property = getFields(filter.getValueReference());
        Time value = filter.getTime();
        switch (filter.getOperator()) {
        case TM_Before:
        case TM_After:
        case TM_Begins:
        case TM_Ends:
        case TM_EndedBy:
        case TM_BegunBy:
        case TM_During:
        case TM_Equals:
        case TM_Contains:
        case TM_Overlaps:
        case TM_Meets:
        case TM_MetBy:
        case TM_OverlappedBy:
        	return new TimeCriterion(property, value, filter.getOperator());
        default:
            throw new UnsupportedOperatorException(filter.getOperator());
        }
    }

    /**
     * Creates {@link Disjunction}s for the specified temporal filters with the
     * same valueReference.
     * 
     * @param temporalFilters
     *            the filters
     * @return {@link Collection} of {@link Disjunction}
     * @throws UnsupportedTimeException
     *             if the value and property combination is not applicable for
     *             this restriction
     * @throws UnsupportedValueReferenceException
     *             if the {@link TemporalFilter#getValueReference() value
     *             reference} can not be decoded
     * @throws UnsupportedOperatorException
     *             if no restriction definition for the {@link TimeOperator} is
     *             found
     */
    public static Map<String, Collection<TimeCriterion>> getDisjunctions(Iterable<TemporalFilter> temporalFilters)
            throws UnsupportedTimeException, UnsupportedValueReferenceException, UnsupportedOperatorException {
        Map<String, Collection<TimeCriterion>> map = Maps.newHashMap();
        for (TemporalFilter temporalFilter : temporalFilters) {
        	CollectionHelper.addToCollectionMap(temporalFilter.getValueReference(), filter(temporalFilter), map);
        }
        return map;
    }

    /**
     * Gets the field descriptor for the specified value reference.
     * 
     * @param valueReference
     *            the value reference
     * 
     * @return the property descriptor
     * 
     * @see #PHENOMENON_TIME_VALUE_REFERENCE
     * @see #RESULT_TIME_VALUE_REFERENCE
     * @see #VALID_TIME_VALUE_REFERENCE
     * @see #PHENOMENON_TIME_FIELDS
     * @see #RESULT_TIME_FIELDS
     * @see #VALID_TIME_FIELDS
     * 
     * @throws UnsupportedValueReferenceException
     *             if the <tt>valueReference</tt> can not be decoded
     */
    private static TimePrimitiveFieldDescriptor getFields(String valueReference)
            throws UnsupportedValueReferenceException {
        if (valueReference.contains(PHENOMENON_TIME_VALUE_REFERENCE)) {
            return PHENOMENON_TIME_FIELDS;
        } else if (valueReference.contains(RESULT_TIME_VALUE_REFERENCE)) {
            return RESULT_TIME_FIELDS;
        } else if (valueReference.contains(VALID_TIME_VALUE_REFERENCE)) {
            return VALID_TIME_FIELDS;
        } else if (valueReference.contains(VALID_DESCRIBE_SENSOR_TIME_VALUE_REFERENCE)) {
            return VALID_TIME_DESCRIBE_SENSOR_FIELDS;
        } else {
            throw new UnsupportedValueReferenceException(valueReference);
        }
    }

    /**
     * Private constructor due to static access.
     */
    private TemporalRestrictions() {
        // noop
    }
}
