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

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.opengis.sos.x20.GetCapabilitiesDocument;
import net.opengis.sos.x20.GetCapabilitiesType;
import net.opengis.sos.x20.GetFeatureOfInterestDocument;
import net.opengis.sos.x20.GetFeatureOfInterestType;
import net.opengis.sos.x20.GetObservationByIdDocument;
import net.opengis.sos.x20.GetObservationByIdType;
import net.opengis.sos.x20.GetObservationDocument;
import net.opengis.sos.x20.GetObservationType;
import net.opengis.sos.x20.GetResultDocument;
import net.opengis.sos.x20.GetResultResponseDocument;
import net.opengis.sos.x20.GetResultResponseType;
import net.opengis.sos.x20.GetResultTemplateDocument;
import net.opengis.sos.x20.GetResultTemplateResponseDocument;
import net.opengis.sos.x20.GetResultTemplateResponseType;
import net.opengis.sos.x20.GetResultTemplateType;
import net.opengis.sos.x20.GetResultType;

import org.apache.xmlbeans.XmlObject;
import org.apache.xmlbeans.XmlString;
import org.n52.sos.exception.ows.InvalidParameterValueException;
import org.n52.sos.exception.ows.NoApplicableCodeException;
import org.n52.sos.exception.ows.concrete.MissingResultValuesException;
import org.n52.sos.exception.ows.concrete.UnsupportedDecoderInputException;
import org.n52.sos.ogc.filter.SpatialFilter;
import org.n52.sos.ogc.filter.TemporalFilter;
import org.n52.sos.ogc.ows.OwsExceptionReport;
import org.n52.sos.ogc.sos.Sos2Constants;
import org.n52.sos.ogc.sos.SosConstants;
import org.n52.sos.ogc.sos.SosResultEncoding;
import org.n52.sos.ogc.sos.SosResultStructure;
import org.n52.sos.ogc.swe.SweAbstractDataComponent;
import org.n52.sos.ogc.swe.encoding.SweAbstractEncoding;
import org.n52.sos.request.AbstractServiceRequest;
import org.n52.sos.request.GetCapabilitiesRequest;
import org.n52.sos.request.GetFeatureOfInterestRequest;
import org.n52.sos.request.GetObservationByIdRequest;
import org.n52.sos.request.GetObservationRequest;
import org.n52.sos.request.GetResultRequest;
import org.n52.sos.request.GetResultTemplateRequest;
import org.n52.sos.response.AbstractServiceResponse;
import org.n52.sos.response.GetResultResponse;
import org.n52.sos.response.GetResultTemplateResponse;
import org.n52.sos.service.AbstractServiceCommunicationObject;
import org.n52.sos.service.ServiceConstants.SupportedTypeKey;
import org.n52.sos.util.CodingHelper;
import org.n52.sos.util.CollectionHelper;
import org.n52.sos.util.XmlHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.google.common.base.Joiner;

/**
 * @since 4.0.0
 * 
 */
public class SosDecoderv20 extends AbstractSwesDecoderv20 implements Decoder<AbstractServiceCommunicationObject, XmlObject> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SosDecoderv20.class);

    @SuppressWarnings("unchecked")
    private static final Set<DecoderKey> DECODER_KEYS = CollectionHelper.union(CodingHelper.decoderKeysForElements(
            Sos2Constants.NS_SOS_20, GetCapabilitiesDocument.class, GetObservationDocument.class,
            GetFeatureOfInterestDocument.class, GetObservationByIdDocument.class,
            GetResultTemplateDocument.class,
            GetResultDocument.class, GetResultTemplateResponseDocument.class, GetResultResponseDocument.class),
            CodingHelper.xmlDecoderKeysForOperation(SosConstants.SOS, Sos2Constants.SERVICEVERSION,
                    SosConstants.Operations.GetCapabilities, SosConstants.Operations.GetObservation,
                    SosConstants.Operations.GetFeatureOfInterest, SosConstants.Operations.GetObservationById,
                    Sos2Constants.Operations.GetResultTemplate,
                    SosConstants.Operations.GetResult));

    public SosDecoderv20() {
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
    public AbstractServiceCommunicationObject decode(final XmlObject xml) throws OwsExceptionReport {
        LOGGER.debug("REQUESTTYPE:" + xml.getClass());
        // validate document
        XmlHelper.validateDocument(xml);
        if (xml instanceof GetCapabilitiesDocument) {
            return parseGetCapabilities((GetCapabilitiesDocument) xml);
        } else if (xml instanceof GetObservationDocument) {
            return parseGetObservation((GetObservationDocument) xml);
        } else if (xml instanceof GetFeatureOfInterestDocument) {
            return parseGetFeatureOfInterest((GetFeatureOfInterestDocument) xml);
        } else if (xml instanceof GetObservationByIdDocument) {
            return parseGetObservationById((GetObservationByIdDocument) xml);
        } else if (xml instanceof GetResultTemplateDocument) {
            return parseGetResultTemplate((GetResultTemplateDocument) xml);
        } else if (xml instanceof GetResultDocument) {
            return parseGetResult((GetResultDocument) xml);
        } else if (xml instanceof GetResultTemplateResponseDocument) {
            return parseGetResultTemplateResponse((GetResultTemplateResponseDocument) xml);
        } else if (xml instanceof GetResultResponseDocument) {
            return parseGetResultResponse((GetResultResponseDocument) xml);
        } else {
            throw new UnsupportedDecoderInputException(this, xml);
        }
    }

    /**
     * parses the XmlBean representing the getCapabilities request and creates a
     * SosGetCapabilities request
     * 
     * @param getCapsDoc
     *            XmlBean created from the incoming request stream
     * @return Returns SosGetCapabilitiesRequest representing the request
     * 
     * 
     * @throws OwsExceptionReport
     *             * If parsing the XmlBean failed
     */
    private AbstractServiceRequest<?> parseGetCapabilities(final GetCapabilitiesDocument getCapsDoc)
            throws OwsExceptionReport {
        final GetCapabilitiesRequest request = new GetCapabilitiesRequest();

        final GetCapabilitiesType getCapsType = getCapsDoc.getGetCapabilities2();

        request.setService(getCapsType.getService());

        if (getCapsType.getAcceptFormats() != null && getCapsType.getAcceptFormats().sizeOfOutputFormatArray() != 0) {
            request.setAcceptFormats(Arrays.asList(getCapsType.getAcceptFormats().getOutputFormatArray()));
        }

        if (getCapsType.getAcceptVersions() != null && getCapsType.getAcceptVersions().sizeOfVersionArray() != 0) {
            request.setAcceptVersions(Arrays.asList(getCapsType.getAcceptVersions().getVersionArray()));
        }

        if (getCapsType.getSections() != null && getCapsType.getSections().getSectionArray().length != 0) {
            request.setSections(Arrays.asList(getCapsType.getSections().getSectionArray()));
        }
        
        if (getCapsType.getExtensionArray() != null && getCapsType.getExtensionArray().length > 0) {
        	request.setExtensions(parseExtensibleRequestExtension(getCapsType.getExtensionArray()));
        }

        return request;
    }

    /**
     * parses the XmlBean representing the getObservation request and creates a
     * SoSGetObservation request
     * 
     * @param getObsDoc
     *            XmlBean created from the incoming request stream
     * @return Returns SosGetObservationRequest representing the request
     * 
     * 
     * @throws OwsExceptionReport
     *             * If parsing the XmlBean failed
     */
    private AbstractServiceRequest<?> parseGetObservation(final GetObservationDocument getObsDoc)
            throws OwsExceptionReport {
        final GetObservationRequest getObsRequest = new GetObservationRequest();
        final GetObservationType getObsType = getObsDoc.getGetObservation();
        // TODO: check
        getObsRequest.setService(getObsType.getService());
        getObsRequest.setVersion(getObsType.getVersion());
        getObsRequest.setOfferings(Arrays.asList(getObsType.getOfferingArray()));
        getObsRequest.setObservedProperties(Arrays.asList(getObsType.getObservedPropertyArray()));
        getObsRequest.setProcedures(Arrays.asList(getObsType.getProcedureArray()));
        getObsRequest.setTemporalFilters(parseTemporalFilters4GetObservation(getObsType.getTemporalFilterArray()));
        if (getObsType.isSetSpatialFilter()) {
            getObsRequest.setSpatialFilter(parseSpatialFilter4GetObservation(getObsType.getSpatialFilter()));
        }
        getObsRequest.setFeatureIdentifiers(Arrays.asList(getObsType.getFeatureOfInterestArray()));
        if (getObsType.isSetResponseFormat()) {
            try {
                final String responseFormat = URLDecoder.decode(getObsType.getResponseFormat(), "UTF-8");
                getObsRequest.setResponseFormat(responseFormat);
            } catch (final UnsupportedEncodingException e) {
                throw new NoApplicableCodeException().causedBy(e).withMessage("Error while encoding response format!");
            }
        }
        getObsRequest.setExtensions(parseExtensibleRequest(getObsType));
        return getObsRequest;
    }

//	private SwesExtensions parseSwesExtensions(final XmlObject[] extensionArray) throws OwsExceptionReport
//	{
//		final SwesExtensions extensions = new SwesExtensions();
//    	for (final XmlObject xbSwesExtension : extensionArray) {
//    		
//    		final Object obj = CodingHelper.decodeXmlElement(xbSwesExtension);
//			if (obj instanceof SwesExtension<?>) {
//				extensions.addSwesExtension((SwesExtension<?>) obj);
//    		}
//		}
//		return extensions;
//	}

    /**
     * parses the passes XmlBeans document and creates a SOS
     * getFeatureOfInterest request
     * 
     * @param getFoiDoc
     *            XmlBeans document representing the getFeatureOfInterest
     *            request
     * @return Returns SOS getFeatureOfInterest request
     * 
     * 
     * @throws OwsExceptionReport
     *             * if validation of the request failed
     */
    private AbstractServiceRequest<?> parseGetFeatureOfInterest(final GetFeatureOfInterestDocument getFoiDoc)
            throws OwsExceptionReport {

        final GetFeatureOfInterestRequest getFoiRequest = new GetFeatureOfInterestRequest();
        final GetFeatureOfInterestType getFoiType = getFoiDoc.getGetFeatureOfInterest();
        getFoiRequest.setService(getFoiType.getService());
        getFoiRequest.setVersion(getFoiType.getVersion());
        getFoiRequest.setFeatureIdentifiers(Arrays.asList(getFoiType.getFeatureOfInterestArray()));
        getFoiRequest.setObservedProperties(Arrays.asList(getFoiType.getObservedPropertyArray()));
        getFoiRequest.setProcedures(Arrays.asList(getFoiType.getProcedureArray()));
        getFoiRequest.setSpatialFilters(parseSpatialFilters4GetFeatureOfInterest(getFoiType.getSpatialFilterArray()));
        getFoiRequest.setExtensions(parseExtensibleRequest(getFoiType));
        return getFoiRequest;
    }

    private AbstractServiceRequest<?> parseGetObservationById(final GetObservationByIdDocument getObsByIdDoc)
            throws OwsExceptionReport {
        final GetObservationByIdRequest getObsByIdRequest = new GetObservationByIdRequest();
        final GetObservationByIdType getObsByIdType = getObsByIdDoc.getGetObservationById();
        getObsByIdRequest.setService(getObsByIdType.getService());
        getObsByIdRequest.setVersion(getObsByIdType.getVersion());
        getObsByIdRequest.setObservationIdentifier(Arrays.asList(getObsByIdType.getObservationArray()));
        getObsByIdRequest.setExtensions(parseExtensibleRequest(getObsByIdType));
        return getObsByIdRequest;
    }

    private AbstractServiceRequest<?> parseGetResult(final GetResultDocument getResultDoc) throws OwsExceptionReport {
        final GetResultType getResult = getResultDoc.getGetResult();
        final GetResultRequest sosGetResultRequest = new GetResultRequest();
        sosGetResultRequest.setService(getResult.getService());
        sosGetResultRequest.setVersion(getResult.getVersion());
        sosGetResultRequest.setOffering(getResult.getOffering());
        sosGetResultRequest.setObservedProperty(getResult.getObservedProperty());
        sosGetResultRequest.setFeatureIdentifiers(Arrays.asList(getResult.getFeatureOfInterestArray()));
        getResult.getFeatureOfInterestArray();
        if (getResult.isSetSpatialFilter()) {
            sosGetResultRequest.setSpatialFilter(parseSpatialFilter4GetResult(getResult.getSpatialFilter()));
        }
        sosGetResultRequest.setExtensions(parseExtensibleRequest(getResult));
        sosGetResultRequest.setTemporalFilter(parseTemporalFilters4GetResult(getResult.getTemporalFilterArray()));
        return sosGetResultRequest;
    }

    private AbstractServiceRequest<?> parseGetResultTemplate(final GetResultTemplateDocument getResultTemplateDoc)
            throws OwsExceptionReport {
        final GetResultTemplateType getResultTemplate = getResultTemplateDoc.getGetResultTemplate();
        final GetResultTemplateRequest sosGetResultTemplateRequest = new GetResultTemplateRequest();
        sosGetResultTemplateRequest.setService(getResultTemplate.getService());
        sosGetResultTemplateRequest.setVersion(getResultTemplate.getVersion());
        sosGetResultTemplateRequest.setOffering(getResultTemplate.getOffering());
        sosGetResultTemplateRequest.setObservedProperty(getResultTemplate.getObservedProperty());
        sosGetResultTemplateRequest.setExtensions(parseExtensibleRequest(getResultTemplate));
        return sosGetResultTemplateRequest;
    }

    private AbstractServiceResponse parseGetResultTemplateResponse(
            final GetResultTemplateResponseDocument getResultTemplateResponseDoc) throws OwsExceptionReport {
        final GetResultTemplateResponse sosGetResultTemplateResponse = new GetResultTemplateResponse();
        final GetResultTemplateResponseType getResultTemplateResponse =
                getResultTemplateResponseDoc.getGetResultTemplateResponse();
        final SosResultEncoding resultEncoding =
                parseResultEncoding(getResultTemplateResponse.getResultEncoding().getAbstractEncoding());
        final SosResultStructure resultStructure =
                parseResultStructure(getResultTemplateResponse.getResultStructure().getAbstractDataComponent());
        sosGetResultTemplateResponse.setResultEncoding(resultEncoding);
        sosGetResultTemplateResponse.setResultStructure(resultStructure);
        return sosGetResultTemplateResponse;
    }

    private AbstractServiceResponse parseGetResultResponse(final GetResultResponseDocument getResultResponseDoc)
            throws OwsExceptionReport {
        final GetResultResponse sosGetResultResponse = new GetResultResponse();
        final GetResultResponseType getResultResponse = getResultResponseDoc.getGetResultResponse();
        parseResultValues(getResultResponse.getResultValues());
        // sosGetResultResponse.setResultValues(resultValues);
        return sosGetResultResponse;
    }

    /**
     * Parses the spatial filter of a GetObservation request.
     * 
     * @param spatialFilter
     *            XmlBean representing the spatial filter parameter of the
     *            request
     * @return Returns SpatialFilter created from the passed foi request
     *         parameter
     * 
     * 
     * @throws OwsExceptionReport
     *             * if creation of the SpatialFilter failed
     */
    private SpatialFilter parseSpatialFilter4GetObservation(
            final net.opengis.sos.x20.GetObservationType.SpatialFilter spatialFilter) throws OwsExceptionReport {
        if (spatialFilter != null && spatialFilter.getSpatialOps() != null) {
            final Object filter = CodingHelper.decodeXmlElement(spatialFilter.getSpatialOps());
            if (filter instanceof SpatialFilter) {
                return (SpatialFilter) filter;
            }
        }
        return null;
    }

    /**
     * Parses the spatial filters of a GetFeatureOfInterest request.
     * 
     * @param spatialFilters
     *            XmlBean representing the spatial filter parameter of the
     *            request
     * @return Returns SpatialFilter created from the passed foi request
     *         parameter
     * 
     * 
     * @throws OwsExceptionReport
     *             * if creation of the SpatialFilter failed
     */
    private List<SpatialFilter> parseSpatialFilters4GetFeatureOfInterest(
            final net.opengis.sos.x20.GetFeatureOfInterestType.SpatialFilter[] spatialFilters)
            throws OwsExceptionReport {
        final List<SpatialFilter> sosSpatialFilters = new ArrayList<SpatialFilter>(spatialFilters.length);
        for (final net.opengis.sos.x20.GetFeatureOfInterestType.SpatialFilter spatialFilter : spatialFilters) {
            final Object filter = CodingHelper.decodeXmlElement(spatialFilter.getSpatialOps());
            if (filter instanceof SpatialFilter) {
                sosSpatialFilters.add((SpatialFilter) filter);
            }
        }
        return sosSpatialFilters;
    }

    private SpatialFilter parseSpatialFilter4GetResult(
            final net.opengis.sos.x20.GetResultType.SpatialFilter spatialFilter) throws OwsExceptionReport {
        if (spatialFilter != null && spatialFilter.getSpatialOps() != null) {
            final Object filter = CodingHelper.decodeXmlElement(spatialFilter.getSpatialOps());
            if (filter instanceof SpatialFilter) {
                return (SpatialFilter) filter;
            }
        }
        return null;
    }

    /**
     * parses the Time of the requests and returns an array representing the
     * temporal filters
     * 
     * @param temporalFilters
     *            array of XmlObjects representing the Time element in the
     *            request
     * @return Returns array representing the temporal filters
     * 
     * 
     * @throws OwsExceptionReport
     *             * if parsing of the element failed
     */
    private List<TemporalFilter> parseTemporalFilters4GetObservation(
            final net.opengis.sos.x20.GetObservationType.TemporalFilter[] temporalFilters) throws OwsExceptionReport {
        final List<TemporalFilter> sosTemporalFilters = new ArrayList<TemporalFilter>(temporalFilters.length);
        for (final net.opengis.sos.x20.GetObservationType.TemporalFilter temporalFilter : temporalFilters) {
            final Object filter = CodingHelper.decodeXmlElement(temporalFilter.getTemporalOps());
            if (filter instanceof TemporalFilter) {
                sosTemporalFilters.add((TemporalFilter) filter);
            }
        }
        return sosTemporalFilters;
    }

    private List<TemporalFilter> parseTemporalFilters4GetResult(
            final net.opengis.sos.x20.GetResultType.TemporalFilter[] temporalFilters) throws OwsExceptionReport {
        final List<TemporalFilter> sosTemporalFilters = new ArrayList<TemporalFilter>(temporalFilters.length);
        for (final net.opengis.sos.x20.GetResultType.TemporalFilter temporalFilter : temporalFilters) {
            final Object filter = CodingHelper.decodeXmlElement(temporalFilter.getTemporalOps());
            if (filter instanceof TemporalFilter) {
                sosTemporalFilters.add((TemporalFilter) filter);
            }
        }
        return sosTemporalFilters;
    }

    private SosResultStructure parseResultStructure(final XmlObject resultStructure) throws OwsExceptionReport {
        final Object decodedObject = CodingHelper.decodeXmlElement(resultStructure);
        if (decodedObject instanceof SweAbstractDataComponent) {
            final SweAbstractDataComponent sosSweData = (SweAbstractDataComponent) decodedObject;
            final SosResultStructure sosResultStructure = new SosResultStructure();
            sosResultStructure.setResultStructure(sosSweData);
            return sosResultStructure;
        } else {
            throw new InvalidParameterValueException().at(Sos2Constants.GetObservationParams.observation)
                    .withMessage("The requested result structure (%s) is not supported by this server!",
                            resultStructure.getDomNode().getNodeName());
        }
    }

    private SosResultEncoding parseResultEncoding(final XmlObject resultEncoding) throws OwsExceptionReport {
        final Object decodedObject = CodingHelper.decodeXmlElement(resultEncoding);
        if (decodedObject instanceof SweAbstractEncoding) {
            final SweAbstractEncoding sosSweEncoding = (SweAbstractEncoding) decodedObject;
            final SosResultEncoding encoding = new SosResultEncoding();
            encoding.setEncoding(sosSweEncoding);
            return encoding;
        } else {
            throw new InvalidParameterValueException().at(Sos2Constants.GetObservationParams.observation)
                    .withMessage("The requested result encoding (%s) is not supported by this server!",
                            resultEncoding.getDomNode().getNodeName());
        }
    }

    private String parseResultValues(final XmlObject resultValues) throws OwsExceptionReport {
        if (resultValues.schemaType() == XmlString.type) {
            return ((XmlString) resultValues).getStringValue().trim();
        } else if (resultValues.schemaType() == XmlObject.type) {
            final Node resultValuesNode = resultValues.getDomNode();
            if (resultValuesNode.hasChildNodes()) {
                final NodeList childNodes = resultValuesNode.getChildNodes();
                for (int i = 0; i < childNodes.getLength(); i++) {
                    final Node childNode = childNodes.item(i);
                    if (childNode.getNodeType() == Node.TEXT_NODE) {
                        return childNode.getNodeValue().trim();
                    }
                }
            }
            throw new MissingResultValuesException();
        } else {
            throw new NoApplicableCodeException().withMessage("The requested resultValue type is not supported");
        }
    }
}
