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
package org.n52.sos.ds.hibernate.dao.i18n;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.hibernate.Session;
import org.n52.sos.ds.I18NDAO;
import org.n52.sos.ds.hibernate.HibernateSessionHolder;
import org.n52.sos.ds.hibernate.dao.FeatureOfInterestDAO;
import org.n52.sos.ds.hibernate.dao.ObservablePropertyDAO;
import org.n52.sos.ds.hibernate.dao.OfferingDAO;
import org.n52.sos.ds.hibernate.dao.ProcedureDAO;
import org.n52.sos.ds.hibernate.entities.AbstractIdentifierNameDescriptionEntity;
import org.n52.sos.ds.hibernate.entities.FeatureOfInterest;
import org.n52.sos.ds.hibernate.entities.ObservableProperty;
import org.n52.sos.ds.hibernate.entities.Offering;
import org.n52.sos.ds.hibernate.entities.Procedure;
import org.n52.sos.ds.hibernate.entities.i18n.AbstractHibernateI18NMetadata;
import org.n52.sos.i18n.I18NSettings;
import org.n52.sos.i18n.LocaleHelper;
import org.n52.sos.i18n.LocalizedString;
import org.n52.sos.i18n.metadata.AbstractI18NMetadata;
import org.n52.sos.ogc.ows.OwsExceptionReport;
import org.n52.sos.service.SosContextListener;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;

import de.hzg.common.SOSConfiguration;
import de.hzg.measurement.Sensor;

public abstract class AbstractHibernateI18NDAO<T extends AbstractIdentifierNameDescriptionEntity,
                                               S extends AbstractI18NMetadata,
                                               H extends AbstractHibernateI18NMetadata>
        implements I18NDAO<S>, HibernateI18NDAO<S> {

    private final HibernateSessionHolder sessionHolder = new HibernateSessionHolder();

    private List<H> getHibernateObjects(Collection<String> ids, Locale locale, Session session) {
    	final SOSConfiguration sosConfiguration = SosContextListener.hzgSOSConfiguration;
    	final Locale defLocale = LocaleHelper.fromString(I18NSettings.I18N_DEFAULT_LANGUAGE_DEFINITION.getDefaultValue());

    	if (locale != null && !locale.equals(defLocale)) {
    		throw new RuntimeException("Only the default locale is supported for now.");
    	}

    	final List<H> hibernateObjects = new ArrayList<H>();

    	for (final String id: ids) {
        	final H object = createHibernateObject();

        	object.setLocale(defLocale);

        	if (id == null || id.startsWith(sosConfiguration.getProcedureIdentifierPrefix())) {
        		// don't use getTProcedureForIdentifier here, recursive call
        		final List<Sensor> sensors = new ProcedureDAO().getSensorsForIdentifiers(Collections.singletonList(id), session);

        		if (sensors != null && !sensors.isEmpty()) {
            		final Procedure procedure = ProcedureDAO.createTProcedureNoDescriptionXML(sensors.get(0), session);

        			object.setName(id.substring(sosConfiguration.getProcedureIdentifierPrefix().length()));
        			object.setObjectId(procedure);

        			hibernateObjects.add(object);
        		}
        	} else if (id == null || id.startsWith(sosConfiguration.getFeatureOfInterestIdentifierPrefix())) {
        		final FeatureOfInterest foi = new FeatureOfInterestDAO().getFeatureOfInterest(id, session);

        		object.setName(id.substring(sosConfiguration.getFeatureOfInterestIdentifierPrefix().length()));
        		object.setObjectId(foi);

        		hibernateObjects.add(object);
        	} else if (id == null || id.startsWith(sosConfiguration.getOfferingIdentifierPrefix())) {
        		final Offering offering = new OfferingDAO().getTOfferingForIdentifier(id, session);

        		object.setName(id.substring(sosConfiguration.getOfferingIdentifierPrefix().length()));
        		object.setObjectId(offering);

        		hibernateObjects.add(object);
        	} else if (id == null || id.startsWith(sosConfiguration.getObservablePropertyIdentifierPrefix())) {
        		final ObservableProperty observableProperty = new ObservablePropertyDAO().getObservablePropertyForIdentifier(id, session);

        		object.setName(id.substring(sosConfiguration.getObservablePropertyIdentifierPrefix().length()));
        		object.setObjectId(observableProperty);

        		hibernateObjects.add(object);
        	}
    	}

    	return hibernateObjects;
    }

    @Override
    public S getMetadata(String id)
            throws OwsExceptionReport {
        Session session = null;
        try {
            session = sessionHolder.getSession();
            return getMetadata(id, session);	
        } finally {
            sessionHolder.returnSession(session);
        }
    }

    @Override
    public Collection<S> getMetadata(Collection<String> id)
            throws OwsExceptionReport {
        Session session = null;
        try {
            session = sessionHolder.getSession();
            return getMetadata(id, session);
        } finally {
            sessionHolder.returnSession(session);
        }
    }

    @Override
    public S getMetadata(String id, Locale locale)
            throws OwsExceptionReport {
        Session session = null;
        try {
            session = sessionHolder.getSession();
            return getMetadata(id, locale, session);
        } finally {
            sessionHolder.returnSession(session);
        }
    }

    @Override
    public Collection<S> getMetadata(Collection<String> id, Locale locale)
            throws OwsExceptionReport {
        Session session = null;
        try {
            session = sessionHolder.getSession();
            return getMetadata(id, locale, session);
        } finally {
            sessionHolder.returnSession(session);
        }
    }

    @Override
    public Collection<S> getMetadata()
            throws OwsExceptionReport {
        Session session = null;
        try {
            session = sessionHolder.getSession();
            return getMetadata(session);
        } finally {
            sessionHolder.returnSession(session);
        }
    }

    @Override
    public void saveMetadata(S i18n)
            throws OwsExceptionReport {
        Session session = null;
        try {
            session = sessionHolder.getSession();
            saveMetadata(i18n, session);
        } finally {
            sessionHolder.returnSession(session);
        }
    }

    @Override
    public Collection<Locale> getAvailableLocales()
            throws OwsExceptionReport {
        Session session = null;
        try {
            session = sessionHolder.getSession();
            return getAvailableLocales(session);
        } finally {
            sessionHolder.returnSession(session);
        }
    }

    @Override
    public Collection<Locale> getAvailableLocales(Session session)
            throws OwsExceptionReport {
    	final Locale locale = LocaleHelper.fromString(I18NSettings.I18N_DEFAULT_LANGUAGE_DEFINITION.getDefaultValue());
    	return Collections.singletonList(locale);
    }

    @Override
    public S getMetadata(String id, Session session)
            throws OwsExceptionReport {
    	return getMetadata(Collections.singletonList(id), session).iterator().next();
    }

    @Override
    public Collection<S> getMetadata(Collection<String> id, Session session)
            throws OwsExceptionReport {
    	final Locale locale = LocaleHelper.fromString(I18NSettings.I18N_DEFAULT_LANGUAGE_DEFINITION.getDefaultValue());
    	return getMetadata(id, locale, session);
    }

    @Override
    public S getMetadata(String id, Locale locale, Session session)
            throws OwsExceptionReport {
    	return getMetadata(Collections.singletonList(id), locale, session).iterator().next();
    }

    @Override
    public Collection<S> getMetadata(Collection<String> id, Locale locale, Session session)
            throws OwsExceptionReport {
    	final List<H> hibernateObjects = getHibernateObjects(id, locale, session);

        return createSosObject(hibernateObjects);
    }

    @Override
    public Collection<S> getMetadata(Session session)
            throws OwsExceptionReport {
    	final Locale locale = LocaleHelper.fromString(I18NSettings.I18N_DEFAULT_LANGUAGE_DEFINITION.getDefaultValue());
    	return getMetadata((Collection<String>)null, locale, session);
    }

    @Override
    public void saveMetadata(S i18n, Session session)
            throws OwsExceptionReport {
    	throw new RuntimeException("Saving of i18n metadata not supported yet.");
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    protected Collection<S> createSosObject(List<H> hi18ns) {
        Map<String, S> map = Maps.newHashMap();
        for (H h18n : hi18ns) {
            String id = h18n.getObjectId().getIdentifier();
            S i18n = map.get(id);
            if (i18n == null) {
                i18n = createSosObject(id);
                map.put(id, i18n);
            }
            fillSosObject(h18n, i18n);
        }

        return map.values();
    }

    protected S createSosObject(String id, List<H> h18ns) {
        S i18n = createSosObject(id);
        for (H h18n : h18ns) {
            fillSosObject(h18n, i18n);
        }
        return i18n;
    }

    protected void deleteOldValues(String id, Session session) {
    	throw new RuntimeException("Deleting old i18n values not supported yet.");
    }

    protected void fillSosObject(H h18n, S i18n) {
        if (h18n.isSetName()) {
            i18n.getName().addLocalization(h18n.getLocale(),
                                           h18n.getName());
        }
        if (h18n.isSetDescription()) {
            i18n.getDescription()
                    .addLocalization(h18n.getLocale(),
                                     h18n.getDescription());
        }
    }

    protected void fillHibernateObject(S i18n, H h18n) {
        Optional<LocalizedString> name = i18n.getName()
                .getLocalization(h18n.getLocale());
        if (name.isPresent()) {
            h18n.setName(name.get().getText());
        }
        Optional<LocalizedString> description = i18n.getDescription()
                .getLocalization(h18n.getLocale());
        if (description.isPresent()) {
            h18n.setDescription(description.get().getText());
        }
    }

    protected abstract T getEntity(String id, Session session);
    protected abstract Class<H> getHibernateEntityClass();
    protected abstract H createHibernateObject();
    protected abstract S createSosObject(String id);
}
