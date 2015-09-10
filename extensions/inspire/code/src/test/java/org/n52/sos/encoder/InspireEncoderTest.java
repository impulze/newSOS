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
package org.n52.sos.encoder;

public class InspireEncoderTest {
    /*
     * xmlns:xsd="http://www.w3.org/2001/XMLSchema"
     * xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation=
     * "http://inspire.ec.europa.eu/schemas/inspire_dls/1.0 http://inspire.ec.europa.eu/schemas/inspire_dls/1.0/inspire_dls.xsd"
     */
//
//    @BeforeClass
//    public static void init() {
//        Map<String, String> prefixes = new HashMap<String, String>();
//        prefixes.put(InspireConstants.NS_INSPIRE_COMMON, InspireConstants.NS_INSPIRE_COMMON_PREFIX);
//        prefixes.put(InspireConstants.NS_INSPIRE_DLS, InspireConstants.NS_INSPIRE_DLS_PREFIX);
//        xmlOptions.setSaveSuggestedPrefixes(prefixes);
//        xmlOptions.setSaveImplicitNamespaces(prefixes);
//        xmlOptions.setSaveAggressiveNamespaces();
//        xmlOptions.setSavePrettyPrint();
//        xmlOptions.setSaveNamespacesFirst();
//        xmlOptions.setCharacterEncoding("UTF-8");
//    }
//
//    @Test
//    public void enocodeMinimalInspireExtendedCapabilities() throws UnsupportedEncoderInputException,
//            OwsExceptionReport, SAXException, IOException {
//        InspireXmlEncoder inspireEncoder = new InspireXmlEncoder();
//        validate(inspireEncoder.encode(getMinimalInspireExtendedCapabilities()));
//    }
//
//    @Test
//    public void enocodeFullIsnpireExtendedCapabilities() throws UnsupportedEncoderInputException, OwsExceptionReport,
//            SAXException, IOException {
//        InspireXmlEncoder inspireEncoder = new InspireXmlEncoder();
//        validate(inspireEncoder.encode(getFullInspireExtendedCapabilities()));
//    }
//
//    @Test
//    public void valid_iso8601() {
//        // date
//        String datePattern = "\\d{4}-(1[0-2]|0[1-9])-(3[0-1]|0[1-9]|[1-2][0-9])";
//        String date = "2013-09-26";
//        Assert.assertThat(Pattern.matches(datePattern, date), Matchers.is(true));
//        // time
//        String timePattern = "(T(2[0-3]|[0-1][0-9]):([0-5][0-9]):([0-5][0-9])(\\.[0-9]+)?)?";
//        String time_HH_MM_SS_S = "T12:49:41.740";
//        Assert.assertThat(Pattern.matches(timePattern, time_HH_MM_SS_S), Matchers.is(true));
//        String time_HH_MM_SS = "T12:49:41";
//        Assert.assertThat(Pattern.matches(timePattern, time_HH_MM_SS), Matchers.is(true));
//        // offset
//        String offsetPattern = "(Z|[+|-](2[0-3]|[0-1][0-9]):([0-5][0-9]))?";
//        String offset_PLUS_HH_MM = "+02:00";
//        Assert.assertThat(Pattern.matches(offsetPattern, offset_PLUS_HH_MM), Matchers.is(true));
//        String offset_MINUS_HH_MM = "-02:00";
//        Assert.assertThat(Pattern.matches(offsetPattern, offset_MINUS_HH_MM), Matchers.is(true));
//        String offset_Z = "Z";
//        Assert.assertThat(Pattern.matches(offsetPattern, offset_Z), Matchers.is(true));
//        // date time
//        String dtPattern = datePattern + timePattern;
//        Assert.assertThat(Pattern.matches(dtPattern, date + time_HH_MM_SS_S), Matchers.is(true));
//        Assert.assertThat(Pattern.matches(dtPattern, date + time_HH_MM_SS), Matchers.is(true));
//        // date time offset
//        String dtoPattern = dtPattern + offsetPattern; 
//        Assert.assertThat(Pattern.matches(dtoPattern, date + time_HH_MM_SS_S + offset_PLUS_HH_MM), Matchers.is(true));
//        Assert.assertThat(Pattern.matches(dtoPattern, date + time_HH_MM_SS_S + offset_MINUS_HH_MM), Matchers.is(true));
//        Assert.assertThat(Pattern.matches(dtoPattern, date + time_HH_MM_SS_S + offset_Z), Matchers.is(true));
//        Assert.assertThat(Pattern.matches(dtoPattern, date + time_HH_MM_SS + offset_PLUS_HH_MM), Matchers.is(true));
//        Assert.assertThat(Pattern.matches(dtoPattern, date + time_HH_MM_SS + offset_MINUS_HH_MM), Matchers.is(true));
//        Assert.assertThat(Pattern.matches(dtoPattern, date + time_HH_MM_SS + offset_Z), Matchers.is(true));
//        // valid patter for schema: \d{4}-(1[0-2]|0[1-9])-(3[0-1]|0[1-9]|[1-2][0-9])(T(2[0-3]|[0-1][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?)?(Z|[+|-](2[0-3]|[0-1][0-9]):([0-5][0-9]))?
//        
////        String pattern =
////                "\\d{4}-(1[0-2]|0[1-9])-(3[0-1]|0[1-9]|[1-2][0-9])(T(2[0-3]|[0-1][0-9]):([0-5][0-9]):([0-5][0-9])(\\.[0-9]+)?)?(Z|([+|-](2[0-3]|[0-1][0-9]):([0-5][0-9]):([0-5][0-9])(\\.[0-9])?)?)?";
////        Assert.assertThat(Pattern.matches(pattern, "2013-09-26T12:49:41.740+02:00"), Matchers.is(true));
//    }

}
