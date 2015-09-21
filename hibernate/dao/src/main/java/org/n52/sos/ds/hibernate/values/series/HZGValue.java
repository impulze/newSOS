package org.n52.sos.ds.hibernate.values.series;

import org.n52.sos.ds.hibernate.entities.series.values.SeriesValue;

import de.hzg.values.ValueData;

public class HZGValue extends SeriesValue {
	private static final long serialVersionUID = -1136758627248795857L;
	private ValueData<?> valueData;

	public ValueData<?> getValueData() {
		return valueData;
	}

	public void setValueData(ValueData<?> valueData) {
		this.valueData = valueData;
	}
}