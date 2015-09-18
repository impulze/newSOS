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
package org.n52.sos.binding.rest.resources.sensors;

import javax.servlet.http.HttpServletRequest;

import org.n52.sos.binding.rest.Constants;
import org.n52.sos.binding.rest.decode.ResourceDecoder;
import org.n52.sos.binding.rest.requests.BadRequestException;
import org.n52.sos.binding.rest.requests.RestRequest;
import org.n52.sos.binding.rest.resources.OptionsRestRequest;
import org.n52.sos.exception.ows.NoApplicableCodeException;
import org.n52.sos.exception.ows.OperationNotSupportedException;
import org.n52.sos.ogc.ows.OwsExceptionReport;
import org.n52.sos.request.DescribeSensorRequest;
import org.n52.sos.request.GetCapabilitiesRequest;

/**
 * @author <a href="mailto:e.h.juerrens@52north.org">Eike Hinderk
 *         J&uuml;rrens</a>
 * 
 */
public class SensorsDecoder extends ResourceDecoder {

    public SensorsDecoder() {
        bindingConstants = Constants.getInstance();
    }

    protected RestRequest decodeGetRequest(HttpServletRequest httpRequest,
            String pathPayload) throws OwsExceptionReport
    {
        // 1 identify type of request: global resource OR with id
        if (pathPayload != null && !pathPayload.isEmpty() && httpRequest.getQueryString() == null) {
            // 2.1 with id
            
            // 2.1.1 build describe sensor request with id pathPayload
            // create DescribeSensorRequest with service=SOS and version=2.0.0
            DescribeSensorRequest describeSensorRequest = createDescribeSensorRequest(pathPayload);

            return new GetSensorByIdRequest(describeSensorRequest);

        } else if (pathPayload == null && httpRequest.getQueryString() == null) {
            
            GetCapabilitiesRequest capabilitiesRequest = createGetCapabilitiesRequestWithContentSectionOnly();
            
            return new GetSensorsRequest(capabilitiesRequest);
            
        } else {
            String errorMsg = createBadGetRequestMessage(bindingConstants.getResourceSensors(),true,true,false);
            BadRequestException bR = new BadRequestException(errorMsg);
            throw new NoApplicableCodeException().causedBy(bR); 
        }
    }

    @Override
    protected RestRequest decodeDeleteRequest(HttpServletRequest httpRequest,
            String pathPayload) throws OwsExceptionReport
    {
    	throw new OperationNotSupportedException(String.format("HTTP-DELETE + \"%s\"",
                bindingConstants.getResourceSensors()));
    }

    @Override
    protected RestRequest decodePostRequest(HttpServletRequest httpRequest,
            String pathPayload) throws OwsExceptionReport
    {
        return null;
    }

    @Override
    protected RestRequest decodePutRequest(HttpServletRequest httpRequest,
            String pathPayload) throws OwsExceptionReport
    {
    	throw new OperationNotSupportedException(String.format("HTTP-PUT + \"%s\"",
                bindingConstants.getResourceSensors()));
    }
    
    private DescribeSensorRequest createDescribeSensorRequest(String procedureId)
    {
        DescribeSensorRequest describeSensorRequest = new DescribeSensorRequest();
        describeSensorRequest.setVersion(bindingConstants.getSosVersion());
        describeSensorRequest.setService(bindingConstants.getSosService());
        describeSensorRequest.setProcedureDescriptionFormat(bindingConstants.getDefaultDescribeSensorOutputFormat());
        describeSensorRequest.setProcedure(procedureId);
        return describeSensorRequest;
    }
    
    @Override
    protected RestRequest decodeOptionsRequest(HttpServletRequest httpRequest,
            String pathPayload)
    {
        boolean isGlobal = false, isCollection = false;
        if (httpRequest != null && httpRequest.getQueryString() == null && pathPayload == null)
        {
            isGlobal = true;
            isCollection = true;
        }
        return new OptionsRestRequest(bindingConstants.getResourceSensors(),isGlobal,isCollection);
    }
    

}
