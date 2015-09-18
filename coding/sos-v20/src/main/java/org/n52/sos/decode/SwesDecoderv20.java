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
package org.n52.sos.decode;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import net.opengis.swes.x20.DescribeSensorDocument;
import net.opengis.swes.x20.DescribeSensorType;

import org.apache.xmlbeans.XmlObject;
import org.n52.sos.exception.ows.InvalidParameterValueException;
import org.n52.sos.exception.ows.concrete.UnsupportedDecoderInputException;
import org.n52.sos.ogc.gml.time.Time;
import org.n52.sos.ogc.ows.OwsExceptionReport;
import org.n52.sos.ogc.sos.Sos2Constants;
import org.n52.sos.ogc.sos.SosConstants;
import org.n52.sos.ogc.swes.SwesConstants;
import org.n52.sos.request.AbstractServiceRequest;
import org.n52.sos.request.DescribeSensorRequest;
import org.n52.sos.service.AbstractServiceCommunicationObject;
import org.n52.sos.service.ServiceConstants.SupportedTypeKey;
import org.n52.sos.util.CodingHelper;
import org.n52.sos.util.CollectionHelper;
import org.n52.sos.util.XmlHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

/**
 * @since 4.0.0
 * 
 */
public class SwesDecoderv20 extends AbstractSwesDecoderv20 implements Decoder<AbstractServiceCommunicationObject, XmlObject> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SwesDecoderv20.class);

    @SuppressWarnings("unchecked")
    private static final Set<DecoderKey> DECODER_KEYS = CollectionHelper.union(CodingHelper.decoderKeysForElements(
            SwesConstants.NS_SWES_20, DescribeSensorDocument.class), CodingHelper
            .xmlDecoderKeysForOperation(SosConstants.SOS, Sos2Constants.SERVICEVERSION, SosConstants.Operations.DescribeSensor));

    public SwesDecoderv20() {
        LOGGER.debug("Decoder for the following keys initialized successfully: {}!", Joiner.on(", ")
                .join(DECODER_KEYS));
    }

    @Override
    public Set<DecoderKey> getDecoderKeyTypes() {
        return Collections.unmodifiableSet(DECODER_KEYS);
    }

    @Override
    public Map<SupportedTypeKey, Set<String>> getSupportedTypes() {
        return Collections.emptyMap();
    }

    @Override
    public Set<String> getConformanceClasses() {
        return Collections.emptySet();
    }

    @Override
    public AbstractServiceRequest<?> decode(final XmlObject xmlObject) throws OwsExceptionReport {
        LOGGER.debug("REQUESTTYPE:" + xmlObject.getClass());
        XmlHelper.validateDocument(xmlObject);
        if (xmlObject instanceof DescribeSensorDocument) {
            return parseDescribeSensor((DescribeSensorDocument) xmlObject);
        } else {
            throw new UnsupportedDecoderInputException(this, xmlObject);
        }
    }

    /**
     * parses the passes XmlBeans document and creates a SOS describeSensor
     * request
     * 
     * @param xbDescSenDoc
     *            XmlBeans document representing the describeSensor request
     * @return Returns SOS describeSensor request
     * 
     * 
     * @throws OwsExceptionReport
     *             * if validation of the request failed
     */
    private AbstractServiceRequest<?> parseDescribeSensor(final DescribeSensorDocument xbDescSenDoc)
            throws OwsExceptionReport {
        final DescribeSensorRequest descSensorRequest = new DescribeSensorRequest();
        final DescribeSensorType xbDescSensor = xbDescSenDoc.getDescribeSensor();
        descSensorRequest.setService(xbDescSensor.getService());
        descSensorRequest.setVersion(xbDescSensor.getVersion());
        descSensorRequest.setProcedure(xbDescSensor.getProcedure());
        descSensorRequest.setProcedureDescriptionFormat(xbDescSensor.getProcedureDescriptionFormat());
        if (xbDescSensor.isSetValidTime()) {
            descSensorRequest.setValidTime(getValidTime(xbDescSensor.getValidTime()));
        }
     // extensions
        descSensorRequest.setExtensions(parseExtensibleRequest(xbDescSensor));
        return descSensorRequest;
    }

    private Time getValidTime(net.opengis.swes.x20.DescribeSensorType.ValidTime validTime) throws OwsExceptionReport {
        Object decodeXmlElement = CodingHelper.decodeXmlElement(validTime.getAbstractTimeGeometricPrimitive());
        if (decodeXmlElement instanceof Time) {
            return (Time) decodeXmlElement;
        } else {
            throw new InvalidParameterValueException().at(Sos2Constants.DescribeSensorParams.validTime).withMessage(
                    "The validTime element ({}) is not supported",
                    validTime.getAbstractTimeGeometricPrimitive().schemaType());
        }
    }
}
