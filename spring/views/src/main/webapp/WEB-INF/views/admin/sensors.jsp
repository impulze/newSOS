<%--

    Copyright (C) 2012-2015 52°North Initiative for Geospatial Open Source
    Software GmbH

    This program is free software; you can redistribute it and/or modify it
    under the terms of the GNU General Public License version 2 as published
    by the Free Software Foundation.

    If the program is linked with libraries which are licensed under one of
    the following licenses, the combination of the program with the linked
    library is not considered a "derivative work" of the program:

        - Apache License, version 2.0
        - Apache Software License, version 1.0
        - GNU Lesser General Public License, version 3
        - Mozilla Public License, versions 1.0, 1.1 and 2.0
        - Common Development and Distribution License (CDDL), version 1.0

    Therefore the distribution of the program linked with libraries licensed
    under the aforementioned licenses, is permitted by the copyright holders
    if the distribution is compliant with both the GNU General Public
    License version 2 and the aforementioned licenses.

    This program is distributed in the hope that it will be useful, but
    WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
    Public License for more details.

--%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions"%>
<%@ taglib prefix="sos" uri="http://52north.org/communities/sensorweb/sos/tags" %>

<jsp:include page="../common/header.jsp">
	<jsp:param name="activeMenu" value="admin" />
</jsp:include>

<link rel="stylesheet" href="<c:url value="/static/lib/prettify.css" />" type="text/css" />
<link rel="stylesheet" href="<c:url value="/static/lib/codemirror-2.34.css" />" type="text/css" />
<link rel="stylesheet" href="<c:url value="/static/css/codemirror.custom.css" />" type="text/css" />
<script type="text/javascript" src="<c:url value="/static/lib/codemirror-2.34.js" />"></script>
<script type="text/javascript" src="<c:url value="/static/lib/codemirror-2.34-xml.js" />"></script>
<script type="text/javascript" src="<c:url value="/static/lib/prettify.min.js" />"></script>
<script type="text/javascript" src="<c:url value="/static/lib/vkbeautify-0.99.00.beta.js" />"></script>
<script type="text/javascript" src="<c:url value="/static/js/jquery.additions.js" />"></script>
<script type="text/javascript" src="<c:url value="/static/js/EventMixin.js" />"></script>
<script type="text/javascript" src="<c:url value="/static/lib/jsxml-0.2.2.js" />"></script>

<jsp:include page="../common/logotitle.jsp">
	<jsp:param name="title" value="Procedure Descriptions" />
	<jsp:param name="leadParagraph" value="" />
</jsp:include>

<style type="text/css">
	.btn-icon { height: 30px; }
	.btn-icon i { margin-right: 0px !important; }
	.btn-single { margin-bottom: 1px;}
	#stcaps-publish { margin-bottom: -11px; }
	#back-top {
		position: fixed;
		bottom: 30px;
		margin-left: -150px;
	}
	#back-top a {
		width: 108px;
		display: block;
		text-align: center;
		font: 11px Arial, Helvetica, sans-serif;
		text-transform: uppercase;
		text-decoration: none;
		color: #bbb;
		/* background color transition */
		-webkit-transition: 1s;
		-moz-transition: 1s;
		transition: 1s;
	}
	#back-top a:hover {
		color: #000;
	}
	#back-top i {
		display: block;
		margin-bottom: 7px;
		margin-left: 48px;
		-webkit-border-radius: 15px;
		-moz-border-radius: 15px;
		border-radius: 15px;
		-webkit-transition: 1s;
		-moz-transition: 1s;
		transition: 1s;
	}
</style>

<script type="text/javascript">
	jQuery(document).ready(function($) {
		$(window).scroll(function () {
			var pos = $(this).scrollTop();
			if (pos > "fast" && pos < $(document).height()
									- ($(this).height() + 400)) {
				$('#back-top').fadeIn();
			} else {
				$('#back-top').fadeOut();
			}
		});
		$('#back-top a').click(function () {
			$('body, html').animate({
				scrollTop: 0
			}, 400);
			return false;
		});
	});

	function xml2string(xml) {
		return typeof(xml) === "string" ? xml : xml.xml ? xml.xml
				: new XMLSerializer().serializeToString(xml);
	}
</script>
<div id="procedure-container">
	<div class="row">
		<div class="span12 form-inline" style="margin-bottom: 5px;">
			<select id="id" class="span6">
				<option disabled="disabled" selected="selected" style="display: none;" value="">Procedures</option>
			</select>
			<div class="btn-group">
			</div>
		</div>
	</div>
	<textarea id="editor" class="span12"></textarea>
</div>

<p id="back-top" style="display: none;"><a href="#top"><i class="icon-chevron-up"></i>Back to Top</a></p>


<script type="text/javascript">

function Descriptons(options) {
	$.extend(this, options);
}

$.extend(Descriptons.prototype, {
	NS: {
		soap: "http://www.w3.org/2003/05/soap-envelope",
		swes: "http://www.opengis.net/swes/2.0",
		sos: "http://www.opengis.net/sos/2.0"
	},
	_sendSoapRequest: function(req, success, error) {
		$.ajax({
			type: "POST", url: baseUrl + "sos/soap", context: this,
			contentType: "application/soap+xml", dataType: "xml", data: xml2string(req)
		}).done(success).fail(error);
	},
	_sendPoxRequest: function(req, success, error) {
		$.ajax({
			type: "POST", url: baseUrl + "sos/pox", context: this,
			contentType: "application/xml", dataType: "xml", data: xml2string(req)
		}).done(success).fail(error);
	},
	_sendKvpRequest: function(req, success, error) {
		$.ajax({
			type: "GET", url: baseUrl + "sos/kvp?",
			context: this, data: req, dataType: "xml"
		}).done(success).fail(error);
	},
	_testId: function(id) {
		if (this.sensors.indexOf(id) < 0) {
			throw new Error("invalid procedure id");
		}
	},
	_getProcedureFormat: function(id) {
        if (!this.procedureFormats.hasOwnProperty(id)) {
            throw new Error("invalid procedure id");
        }
		return this.procedureFormats[id];
	},
	_isException: function(e) {
		var faults = e.getElementsByTagNameNS(this.NS.soap, "Fault"),
			exceptions = e.getElementsByTagNameNS(this.NS.ows, "ExceptionReport");
		if (faults.length > 0 || exceptions > 0) { return true; }

		if (e.documentElement.namespaceURI === this.NS.sos
		 || e.documentElement.namespaceURI === this.NS.soap
		 || e.documentElement.namespaceURI === this.NS.swes) {
			return false;
		}
		return true;
	},
	_createGetSensorRequest: function(id) {
		return this._soap(this._createDescribeSensorRequest(id));
	},
	_setServiceAndVersion: function(x) {
		x.setAttribute("service", "SOS");
		x.setAttribute("version", "2.0.0");
	},
	_createDescribeSensorRequest: function(id) {
		var doc = jsxml.fromString('<?xml version="1.0" encoding="UTF-8"?>' +
				'<swes:DescribeSensor xmlns:swes="' + this.NS.swes + '"/>'),
			describeSensor = doc.documentElement,
			procedure = doc.createElement("swes:procedure"),
			procedureDescriptionFormat = doc.createElement("swes:procedureDescriptionFormat");
		describeSensor.appendChild(procedure);
		procedure.appendChild(doc.createTextNode(id));
		describeSensor.appendChild(procedureDescriptionFormat);
		procedureDescriptionFormat.appendChild(doc.createTextNode(this._getProcedureFormat(id)));
		this._setServiceAndVersion(describeSensor);
		return doc;
	},
	_soap: function(content) {
		var doc = jsxml.fromString('<?xml version="1.0" encoding="UTF-8"?>' +
				'<soap:Envelope xmlns:soap="' + this.NS.soap + '"/>'),
			envelope = doc.documentElement,
			body = doc.createElement("soap:Body");
		envelope.appendChild(body);
		body.appendChild(content.documentElement);
		return doc;
	},
	_getSensorDescription: function(response) {
		var i, j, data, datas = response.getElementsByTagNameNS(this.NS.swes,"data");
		for (i = 0; i < datas.length; ++i) {
			return datas[i].firstElementChild;
		}
		return;
	},
	_createGetSensorKvpRequest: function(id) {
		return {
			service: "SOS",
			version: "2.0.0",
			request: "DescribeSensor",
			procedureDescriptionFormat: this._getProcedureFormat(id),
			procedure: id
		};
	},
	_getSensor: function(id, success, error) {
		if (this.describeSensorRequestMethod === "GET") {
			this._sendKvpRequest(this._createGetSensorKvpRequest(id), success, error);
		} else if (this.describeSensorRequestMethod === "POST") {
			this._sendSoapRequest(this._createGetSensorSoapRequest(id), success, error);
		}
	},
	get: function(id, success, error, context) {
		if (arguments.length === 0) {
			return this.sensors;
		}
		this._testId(id);
		this._getSensor(id, function(response) {
			//TODO get error message
			var fail = this._isException(response),
				callback = fail ? error : success;
			if (fail) {
				if ($.isFunction(error)) {
					error.apply(context || this, arguments);
				}
			} else {
				if ($.isFunction(success)) {
					success.call(context || this, this._getSensorDescription(response));
				}
			}
		}, function() {
			if ($.isFunction(error)) {
				error.apply(context || this, arguments);
			}
		});
	},
});

function Controller(options) {
	$.extend(this, options);
	$.extend(this, {
		$validate: $("#validate"),
		$editor: $("#editor"),
		$procedure: $("#id"),
		$container: $("#procedure-container")
	});
	this.init();
}

$.extend(Controller.prototype, {
	init: function() {
		var i, self = this,
			procedures = this.descriptions.get();

		this.$editor.codeMirror({
			mode: "xml",
			lineNumbers: true,
			lineWrapping: true,
			onChange: function() {
				self.onEditorChange.apply(self, arguments);
			}
		});
		this.$validate.on("click", function() {
			self.onValidate.apply(self, arguments);
		});
		this.$procedure.on("change", function() {
			self.onIdChange.apply(self, arguments);
		});

		for (i = 0; i < procedures.length; ++i) {
			$("<option>").text(procedures[i]).appendTo(this.$procedure);
		}
		this.$procedure.trigger("change");

	},
	getSelectedProcedure: function() {
		return this.$procedure.val();
	},
	getEditorContent: function() {
		return this.$editor.codeMirror("get");
	},
	setEditorContent: function(x) {
		this.$editor.codeMirror("set", x);
	},
	onIdChange: function() {
		var id = this.getSelectedProcedure(),
			onSuccess = function(response) {
				this.setEditorContent(vkbeautify.xml(xml2string(response)));
				this.$validate.disabled(false);
			},
			onError = function(e) {
				showError("Error requesting description for procedure <code>" + id
					+ "</code>: <code>" + e.status + " " + e.statusText
					+ "</code> " + e.responseText);
			};
		if (!id) {
			this.$validate.disabled(true);
			this.setEditorContent("");
		} else {
			this.descriptions.get(id, onSuccess, onError, this);
		}
	},
	onValidate: function() {
		var xml = this.getEditorContent();
		this.$validate.disabled();
		$.ajax({
			type: "POST",
			url: this.baseUrl + "admin/caps/ajax/validation",
			contentType: "application/xml",
			dataType: "json",
			data: xml,
			context: this
		}).done(function(json) {
			this.$validate.disabled(false);
			this.displayValidationResults(json);
		}).fail(function() {
			showError("Error validating document: <code>" + e.status + " "
				+ e.statusText + "</code> " + e.responseText);
			this.$validate.disabled(false);
		});
	},
	displayValidationResults: function(results) {
		var $results = this.$container.find(".validation-result");
		if ($results.length === 0) {
			$results = $("<div>").addClass("validation-result").prependTo(this.$container);
		}
		$results.slideUp(function() {
			$results.children().remove();
			$("<button>").attr({
				"type": "button",
				"class": "close",
				"data-dismiss": "alert"
			}).html("&times;").appendTo($results);

			$results.addClass("alert fade in")
					.removeClass("alert-block alert-success alert-error");

			if (results.valid) {
				$results.addClass("alert-success").append($("<h5>Validation succeeded</h5>"));
			} else {
				$results.addClass("alert-error").append($("<h5>Validation failed</h5>"));

				var $ul = $("<ul>");
				for (var i = 0; i < results.errors.length; ++i) {
					$("<li>").append($("<small>").text(results.errors[i])).appendTo($ul);
				}

				$results.append($ul);
			}
			$results.slideDown();
		});
	}
});


</script>


<script type="text/javascript">
	var sensors = [];
	<c:forEach items="${sensors}" var="s">
	sensors.push("${s}");</c:forEach>

    var procedureFormats = ${sos:mapToJson(procedureFormatMap)};

	var isDescribeSensorSupported = ${isDescribeSensorSupported};
	var baseUrl = "<c:url value="/"/>";
	var descriptions = new Descriptons({
		describeSensorRequestMethod: "${describeSensorRequestMethod}",
		baseUrl: baseUrl,
		sensors: sensors,
		procedureFormats: procedureFormats
	});
	new Controller({
		baseUrl: baseUrl,
		descriptions: descriptions
	});
</script>

<jsp:include page="../common/footer.jsp" />