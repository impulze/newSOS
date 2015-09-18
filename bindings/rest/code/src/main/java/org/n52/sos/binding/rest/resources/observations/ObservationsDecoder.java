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
package org.n52.sos.binding.rest.resources.observations;

import java.util.ArrayList;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.n52.sos.binding.rest.decode.ResourceDecoder;
import org.n52.sos.binding.rest.requests.BadRequestException;
import org.n52.sos.binding.rest.requests.RestRequest;
import org.n52.sos.binding.rest.resources.OptionsRestRequest;
import org.n52.sos.exception.ows.InvalidParameterValueException;
import org.n52.sos.exception.ows.NoApplicableCodeException;
import org.n52.sos.exception.ows.OperationNotSupportedException;
import org.n52.sos.exception.ows.concrete.DateTimeException;
import org.n52.sos.ogc.ows.OwsExceptionReport;
import org.n52.sos.ogc.sos.Sos2Constants;
import org.n52.sos.ogc.swes.SwesExtension;
import org.n52.sos.ogc.swes.SwesExtensionImpl;
import org.n52.sos.ogc.swes.SwesExtensions;
import org.n52.sos.request.GetCapabilitiesRequest;
import org.n52.sos.request.GetObservationByIdRequest;
import org.n52.sos.request.GetObservationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="mailto:e.h.juerrens@52north.org">Eike Hinderk J&uuml;rrens</a>
 *
 */
public class ObservationsDecoder extends ResourceDecoder {
    
	private static final Logger LOGGER = LoggerFactory.getLogger(ObservationsDecoder.class);
	
    @Override
    protected RestRequest decodeGetRequest(HttpServletRequest httpRequest,
            String pathPayload) throws OwsExceptionReport, DateTimeException
    {
        // 0 variables
        RestRequest result = null;
        
        // 1 identify type of request: by id OR search (OR atom feed)
        // 2.1 by id
        if (pathPayload != null && !pathPayload.isEmpty() && httpRequest.getQueryString() == null)
        {
            result = decodeObservationByIdRequest(pathPayload);
        }
        // 2.2 search
        else if (httpRequest.getQueryString() != null && pathPayload == null)
        {
            result = decodeObservationsSearchRequest(httpRequest);
        }
        /*
        // 2.3 feed 
        else if (pathPayload == null && httpRequest.getQueryString() == null)
        {
            // pathpayload and querystring == null. if paging is implemented the querystring will not be empty
            result = decoderObservationsFeedRequest(httpRequest);
        }*/
        else
        {
            String errorMsg = createBadGetRequestMessage(bindingConstants.getResourceObservations(),false,true,true);
            BadRequestException bR = new BadRequestException(errorMsg);
            throw new NoApplicableCodeException().causedBy(bR).withMessage(errorMsg);
        }
        
        return result;
    }

    @Override
    protected RestRequest decodeDeleteRequest(HttpServletRequest httpRequest,
            String pathPayload) throws OwsExceptionReport
    {
        String errorMsg = String.format(bindingConstants.getErrorMessageHttpMethodNotAllowedForResource(),
        		"POST",
        		bindingConstants.getResourceObservations());
        LOGGER.error(errorMsg);
        throw new OperationNotSupportedException("HTTP DELETE").withMessage(errorMsg);
    }

	@Override
    protected RestRequest decodePostRequest(HttpServletRequest httpRequest,
            String pathPayload) throws OwsExceptionReport
    {
        String errorMsg = String.format(bindingConstants.getErrorMessageHttpMethodNotAllowedForResource(),
        		"POST",
        		bindingConstants.getResourceObservations());
        LOGGER.error(errorMsg);
        throw new OperationNotSupportedException("HTTP POST").withMessage(errorMsg);
    }

    @Override
    protected RestRequest decodePutRequest(HttpServletRequest httpRequest,
            String pathPayload) throws OwsExceptionReport
    {
    	throw new OperationNotSupportedException(String.format("HTTP-PUT + '%s'",
                bindingConstants.getResourceObservations()));
    }

    private ObservationsSearchRequest decodeObservationsSearchRequest(HttpServletRequest httpRequest) throws OwsExceptionReport, DateTimeException
    {
        // 2.2.1 get kvp encoded parameters from querystring
        Map<String,String> parameterMap = getKvPEncodedParameters(httpRequest);
        
        // 2.2.2 build requests
        GetObservationRequest getObservationRequest = buildGetObservationSearchRequest(parameterMap);
                
        String queryString = httpRequest.getQueryString();
        
        return new ObservationsSearchRequest(getObservationRequest,queryString);   
    }

    private ObservationsGetRequest decodeObservationByIdRequest(String pathPayload)
    {
        // build get observation by id request
        GetObservationByIdRequest getObservationRequest = buildGetObservationByIdRequest(pathPayload);
        // FIXME remove unused GetCapabilitiesRequest
        // build get capabilities request reduced to contents section
        GetCapabilitiesRequest getCapabilitesRequestOnlyContents = createGetCapabilitiesRequestWithContentSectionOnly();
        
        return new ObservationsGetRequest(getObservationRequest,getCapabilitesRequestOnlyContents);
    }

    private GetObservationRequest buildGetObservationSearchRequest(Map<String, String> parameterMap) throws OwsExceptionReport, DateTimeException
    {
        GetObservationRequest request = new GetObservationRequest();
        request.setVersion(bindingConstants.getSosVersion());
        request.setService(bindingConstants.getSosService());
        request.setExtensions(createSubsettingExtension(true));
        
        boolean parameterMapValid = false; // if at least one parameter is valid
        
        // TODO add checking of parameters
        
        for (String parameter : parameterMap.keySet()) {
            
            String value = parameterMap.get(parameter);
            if (parameter.equalsIgnoreCase(bindingConstants.getHttpGetParameterNameFoi()) &&
                    value != null &&  value.length() > 0)
            {
                request.setFeatureIdentifiers(splitKvpParameterValueToList(value));
                parameterMapValid = true;
            }
            else if (parameter.equalsIgnoreCase(bindingConstants.getHttpGetParameterNameObservedProperty()) &&
                    value != null &&  value.length() > 0)
            {
                request.setObservedProperties(splitKvpParameterValueToList(value));
                parameterMapValid = true;
            }
            else if (parameter.equalsIgnoreCase(bindingConstants.getHttpGetParameterNameOffering()) &&
                    value != null &&  value.length() > 0)
            {
                request.setOfferings(splitKvpParameterValueToList(value));
                parameterMapValid = true;
            }
            else if (parameter.equalsIgnoreCase(bindingConstants.getHttpGetParameterNameProcedure()) &&
                    value != null &&  value.length() > 0)
            {
                request.setProcedures(splitKvpParameterValueToList(value));
                parameterMapValid = true;
            }
            else if (parameter.equalsIgnoreCase(bindingConstants.getHttpGetParameterNameSpatialFilter()) &&
                    value != null &&  value.length() > 0)
            {
                request.setSpatialFilter(parseSpatialFilter(splitKvpParameterValueToList(value),parameter));
                parameterMapValid = true;
            }
            else if (parameter.equalsIgnoreCase(bindingConstants.getHttpGetParameterNameTemporalFilter()) &&
                    value != null &&  value.length() > 0)
            {
                request.setTemporalFilters(parseTemporalFilter(splitKvpParameterValueToList(value)));
                parameterMapValid = true;
            }
            else if (parameter.equalsIgnoreCase(bindingConstants.getHttpGetParameterNameNamespaces()) &&
                    value != null &&  value.length() > 0)
            {
                request.setNamespaces(parseNamespaces(value));
                parameterMapValid = true;
            }
            else 
            {
                throw new InvalidParameterValueException(parameter, value);
            }
        }
        if (!parameterMapValid)
        {
        	throw new InvalidParameterValueException().withMessage(bindingConstants.getErrorMessageBadGetRequestNoValidKvpParameter());
        }
        return request;
    }
    
    private GetObservationByIdRequest buildGetObservationByIdRequest(String observationId)
    {
        GetObservationByIdRequest request = new GetObservationByIdRequest();
        ArrayList<String> observationIds = new ArrayList<String>(1);
        observationIds.add(observationId);
        request.setObservationIdentifier(observationIds);
        request.setService(bindingConstants.getSosService());
        request.setVersion(bindingConstants.getSosVersion());
		SwesExtensions extensions = createSubsettingExtension(true);
		request.setExtensions(extensions);
        return request;
    }

	private SwesExtensions createSubsettingExtension(boolean enabled)
	{
		Boolean value = enabled?Boolean.TRUE:Boolean.FALSE;
		
		SwesExtensions extensions = new SwesExtensions();
        SwesExtension<Boolean> antiSubsettingExtension = new SwesExtensionImpl<Boolean>();
        antiSubsettingExtension.setDefinition(Sos2Constants.Extensions.MergeObservationsIntoDataArray.name());
        antiSubsettingExtension.setValue(value);
        extensions.addSwesExtension(antiSubsettingExtension);
		
		return extensions;
	}
    
    @Override
    protected RestRequest decodeOptionsRequest(HttpServletRequest httpRequest,
            String pathPayload)
    {
        boolean isGlobal = false, isCollection = false;
        if (httpRequest != null && httpRequest.getQueryString() != null && pathPayload == null)
        {
            isGlobal = true;
            isCollection = true;
        }
        else if (httpRequest != null && httpRequest.getQueryString() == null && pathPayload == null)
        {
            isGlobal = true;
            isCollection = false;
        }
        else if (httpRequest != null && httpRequest.getQueryString() == null && pathPayload != null) {
            isGlobal = false;
            isCollection = false;
        }
        return new OptionsRestRequest(bindingConstants.getResourceObservations(),isGlobal,isCollection);
    }
    
}
