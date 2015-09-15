package org.n52.sos.ds.hibernate.util;

import org.n52.sos.ogc.filter.FilterConstants.TimeOperator;
import org.n52.sos.ogc.gml.time.Time;

public class TimeCriterion {
	private TimePrimitiveFieldDescriptor property;
	private Time value;
	private TimeOperator operator;

	public TimeCriterion(TimePrimitiveFieldDescriptor property, Time value, TimeOperator operator) {
		this.property = property;
		this.value = value;
		this.operator = operator;
	}

	public TimePrimitiveFieldDescriptor getProperty() {
		return property;
	}

	public Time getValue() {
		return value;
	}

	public TimeOperator getOperator() {
		return operator;
	}
}