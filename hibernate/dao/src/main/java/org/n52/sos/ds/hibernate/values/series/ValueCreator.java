package org.n52.sos.ds.hibernate.values.series;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.n52.sos.ds.hibernate.entities.ereporting.values.EReportingSweDataArrayValue;
import org.n52.sos.ds.hibernate.entities.values.AbstractValue;
import org.n52.sos.ogc.ows.OwsExceptionReport;
import org.n52.sos.ogc.sos.SosConstants.HelperValues;
import org.n52.sos.ogc.swe.SweConstants;
import org.n52.sos.ogc.swe.SweDataArray;
import org.n52.sos.ogc.swe.SweDataRecord;
import org.n52.sos.ogc.swe.SweField;
import org.n52.sos.ogc.swe.encoding.SweTextEncoding;
import org.n52.sos.ogc.swe.simpleType.SweCount;
import org.n52.sos.ogc.swe.simpleType.SweQuantity;
import org.n52.sos.service.ServiceConfiguration;
import org.n52.sos.util.CodingHelper;

import com.google.common.collect.Lists;

import de.hzg.values.CalculatedData;
import de.hzg.values.RawData;
import de.hzg.values.ValueData;

public class ValueCreator<T> {
	private class HZGValue extends EReportingSweDataArrayValue {
		private static final long serialVersionUID = -3524086155502576424L;
	};

	private static SweDataArray fullArray = null;
	private static SweDataArray intervalArray = null;
	private final Map<HelperValues, String> additionalValues = createAdditionalValues();

	private static Map<HelperValues, String> createAdditionalValues() {
		final Map<HelperValues, String> additionalValues = new HashMap<HelperValues, String>();

		additionalValues.put(HelperValues.FOR_OBSERVATION, null);

		return additionalValues;
	}

	public AbstractValue createValue(Collection<T> instances) throws OwsExceptionReport {
		final Iterator<T> iterator = instances.iterator();
		ValueData<?> valueData;
		final HZGValue value = new HZGValue();
		final SweDataArray dataArray;

		if (iterator.hasNext()) {
			final T nextValue = iterator.next();

			if (!(nextValue instanceof ValueData<?>)) {
				throw new RuntimeException("Other raw values besides ValueData not supported.");
			}

			valueData = (ValueData<?>) nextValue; 
		} else {
			return value;
		}

		final boolean usesInterval = valueData.getObservedPropertyInstance().getUseInterval();

		if (usesInterval) {
			if (intervalArray == null) {
				intervalArray = new SweDataArray();
				intervalArray.setElementCount(new SweCount().setValue(5));
				final SweTextEncoding encoding = new SweTextEncoding();
				encoding.setTokenSeparator(ServiceConfiguration.getInstance().getTokenSeparator());
				encoding.setDecimalSeparator(ServiceConfiguration.getInstance().getDecimalSeparator());
				encoding.setBlockSeparator(ServiceConfiguration.getInstance().getTupleSeparator());
				intervalArray.setEncoding(encoding);
				final SweDataRecord dataRecord = new SweDataRecord();
				dataRecord.addField(new SweField("average", new SweQuantity()));
				dataRecord.addField(new SweField("min", new SweQuantity()));
				dataRecord.addField(new SweField("max", new SweQuantity()));
				dataRecord.addField(new SweField("median", new SweQuantity()));
				dataRecord.addField(new SweField("stddev", new SweQuantity()));
				intervalArray.setElementType(dataRecord);
				final List<String> emptyValues = Lists.newArrayList("", "", "", "", "");
				intervalArray.setValues(new ArrayList<List<String>>());
				intervalArray.getValues().add(emptyValues);
			}

			dataArray = intervalArray;
		} else {
			if (fullArray == null) {
				fullArray = new SweDataArray();
				fullArray.setElementCount(new SweCount().setValue(1));
				final SweTextEncoding encoding = new SweTextEncoding();
				encoding.setTokenSeparator(ServiceConfiguration.getInstance().getTokenSeparator());
				encoding.setDecimalSeparator(ServiceConfiguration.getInstance().getDecimalSeparator());
				encoding.setBlockSeparator(ServiceConfiguration.getInstance().getTupleSeparator());
				fullArray.setEncoding(encoding);
				final SweDataRecord dataRecord = new SweDataRecord();
				dataRecord.addField(new SweField("value", new SweQuantity()));
				fullArray.setElementType(dataRecord);
				fullArray.setValues(new ArrayList<List<String>>());
				final List<String> emptyValues = Lists.newArrayList("");
				fullArray.setValues(new ArrayList<List<String>>());
				fullArray.getValues().add(emptyValues);
			}

			dataArray = fullArray;
		}

		final List<List<String>> fillValues = Lists.newArrayListWithCapacity(instances.size());

		for (final T instance: instances) {
			valueData = (ValueData<?>) instance;
			final List<String> fillValuesForThis;

			if (usesInterval) {
				fillValuesForThis = Lists.newArrayListWithCapacity(6);
				fillValuesForThis.add(valueData.getAverage().toString());
				fillValuesForThis.add(valueData.getMin().toString());
				fillValuesForThis.add(valueData.getMax().toString());
				fillValuesForThis.add(valueData.getMedian().toString());
				fillValuesForThis.add(valueData.getStddev().toString());
			} else {
				fillValuesForThis = Lists.newArrayListWithCapacity(6);
				fillValuesForThis.add(valueData.getValue().toString());
			}

			fillValues.add(fillValuesForThis);
		}

		dataArray.setValues(fillValues);
		value.setObservationId((valueData instanceof RawData) ? ((RawData)valueData).getId() : ((CalculatedData)valueData).getId()); 
		value.setPhenomenonTimeStart(valueData.getDate());
		value.setResultTime(valueData.getDate());
		value.setValue(CodingHelper.encodeObjectToXmlText(SweConstants.NS_SWE_20, dataArray, additionalValues));

		return value;
	}
}
