package org.n52.sos.ds.hibernate.dao;

import de.hzg.measurement.ObservedPropertyInstance;

/*
 * Foreign Key to carry around accross calls (iterating).
 */
public class ObservationValueFK {
	private Class<?> clazz;
	private ObservedPropertyInstance observedPropertyInstance;

	public ObservedPropertyInstance getObservedPropertyInstance() {
		return observedPropertyInstance;
	}

	public void setObservedPropertyInstance(ObservedPropertyInstance observedPropertyInstance) {
		this.observedPropertyInstance = observedPropertyInstance;
	}

	public Class<?> getForClass() {
		return clazz;
	}

	public void setForClass(Class<?> clazz) {
		this.clazz = clazz;
	}
}