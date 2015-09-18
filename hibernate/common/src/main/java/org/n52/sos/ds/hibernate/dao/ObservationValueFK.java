package org.n52.sos.ds.hibernate.dao;

/*
 * Foreign Key to carry around accross calls (iteratin). 
 */
public class ObservationValueFK {
	private long seriesID;

	public void setSeriesID(long seriesID) {
		this.seriesID = seriesID;
	}

	public long getSeriesID() {
		return seriesID;
	}
}