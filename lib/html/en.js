// @ts-check
var longMonths = [
	'January',
	'February',
	'March',
	'April',
	'May',
	'June',
	'July',
	'August',
	'September',
	'October',
	'November',
	'December'
];
var shortMonths = longMonths.map((s) => s.slice(0, 3));
var monthPattern = '(' + shortMonths.join('|') + '|' + longMonths.join('|') + ')';

/**
 *
 * @param {String} inputname
 */
function getCheckedRadio(inputname) {
	'use strict';
	var /**@type {any} */radio_group, i, /** @type {HTMLInputElement} */ button;
	radio_group = document.getElementsByName(inputname);
	for (i = 0; i < radio_group.length; i = i + 1) {
		button = radio_group[i];
		if (button.checked) {
			return button;
		}
	}
}

/**
 * 
 * @param {number} y 
 * @param {number} m 
 * @param {number} d 
 * @returns {String}
 */
function ymd(y, m, d) {
	'use strict';
	return `${y}-${(m + 1).toString().padStart(2, '0')}-${d.toString().padStart(2, '0')}`;
}

/**
 * 
 * @param {number} y 
 * @param {number} m 
 * @param {number} d 
 * @returns {String}
 */
function bbdy(y, m, d) {
	'use strict';
	return longMonths[m] + ' ' + d + ', ' + y;
}

/**
 * 
 * @param {number} y 
 * @param {number} m 
 * @param {number} d 
 * @returns {String}
 */
function bdy(y, m, d) {
	'use strict';
	return shortMonths[m] + ' ' + d + ', ' + y;
}

/**
 * 
 * @param {number} y 
 * @param {number} m 
 * @param {number} d 
 * @returns {String}
 */
function dbby(y, m, d) {
	'use strict';
	return d + ' ' + longMonths[m] + ' ' + y;
}

/**
 * 
 * @param {number} y 
 * @param {number} m 
 * @param {number} d 
 * @returns {String}
 */
function dby(y, m, d) {
	'use strict';
	return d + ' ' + shortMonths[m] + ' ' + y;
}

/**
 * 
 * @param {String} s 
 * @returns {Array<number> | undefined}
 */
function parseDate(s) {
	'use strict';
	var m = /(\d{4})-(\d{1,2})-(\d{1,2})/.exec(s);
	if (m) {
		return [parseInt(m[1]), parseInt(m[2]) - 1, parseInt(m[3])];
	}
	m = RegExp(monthPattern + ' ' + /(\d{1,2})/.source + ', ' + /(\d{4})/.source, 'i').exec(s);
	if (m) {
		return [parseInt(m[3]), shortMonths.indexOf(m[1].slice(0, 3)), parseInt(m[2])];
	}
	m = RegExp(/(\d{1,2})/.source + ' ' + monthPattern + ' ' + /(\d{4})/.source, 'i').exec(s);
	if (m) {
		return [parseInt(m[3]), shortMonths.indexOf(m[2].slice(0, 3)), parseInt(m[1])];
	}
}

/**
 * 
 * @param {String} id 
 */
function changeDates(id) {
	'use strict';
	var i, text1, text2, dates, date, newdate, formatter, pipe;
	pipe = getCheckedRadio('pipeformat').value;

	text1 = /**@type {HTMLElement} */(document.getElementById('shortened')).innerHTML;
	text2 = /**@type {HTMLElement} */(document.getElementById('named_ref')).innerHTML;
	dates = text1.match(/date=.*?(?=\}\}|\s?\|\s?)/g);
	if (!dates) return;
	for (i = 0; i < dates.length; i = i + 1) {
		date = dates[i].slice(5);  // omit the `date=` part
		formatter = window[id];
		var ymd = parseDate(date);
		if (!ymd) {
			console.warn('parseDate(date) returned null');
			continue;
		}
		newdate = formatter(...ymd);
		text1 = text1.replace(new RegExp('((?:access)?date=)' + date + '(?=}}|' + pipe + ')'), '$1' + newdate);
		text2 = text2.replace(new RegExp('((?:access)?date=)' + date + '(?=}}|' + pipe + ')'), '$1' + newdate);
		/**@type {HTMLElement} */(document.getElementById('shortened')).innerHTML = text1;
		/**@type {HTMLElement} */(document.getElementById('named_ref')).innerHTML = text2;
	}
}

function onDateChange() {
	'use strict';
	var checkedDate = getCheckedRadio('dateformat');
	var dateId = checkedDate.id;
	changeDates(dateId);
	localStorage.setItem("datefmt", dateId);
	updateURL('dateformat', checkedDate.value);
}

/**
 *
 * @param {String} id
 */
function changePipes(id) {
	'use strict';
	var text1, text2, sfns, newpipe;

	newpipe = '|';
	if (id == 'before') {
		newpipe = ' |';
	} else if (id == 'both') {
		newpipe = ' | ';
	}

	text1 = /**@type {HTMLElement} */(document.getElementById('shortened')).innerHTML;
	text2 = /**@type {HTMLElement} */(document.getElementById('named_ref')).innerHTML;

	sfns = text1.match(/\{\{sfn[ref]?.*?(?=\}\})/g);

	if (!sfns) return;

	// temporarily remove sfns which should always be compact
	text1 = text1.replace(sfns[0], '');
	text1 = text1.replace(sfns[1], '');

	// replace pipes
	text1 = text1.replaceAll(new RegExp('\\s?\\|\\s?', 'g'), newpipe);
	text2 = text2.replaceAll(new RegExp('\\s?\\|\\s?', 'g'), newpipe);

	// replace unmodified sfns removed earlier
	text1 = sfns[0] + text1;
	text1 = text1.replace('ref=}}', 'ref=' + sfns[1] + '}}');

	/**@type {HTMLElement} */(document.getElementById('shortened')).innerHTML = text1;
	/**@type {HTMLElement} */(document.getElementById('named_ref')).innerHTML = text2;
}

function onInputTypeSelect() {
    'use strict';
    var inputType = document.getElementById('input_type').value;
    var userInput = document.getElementById('user_input');

    if (inputType === 'sru' || inputType === 'ixtheo') {
        userInput.placeholder = 'Enter search query';
    } else {
        userInput.placeholder = '';
    }
}

function onPipeChange() {
    'use strict';
	var checkedPipe = getCheckedRadio('pipeformat');
	var pipeId = checkedPipe.id;
	changePipes(pipeId);
	localStorage.setItem("pipefmt", pipeId);
	updateURL('pipeformat', checkedPipe.value);
}

function onTemplateFormatChange() {
    'use strict';
    var templateFormat = document.getElementById('template_format').value;
    localStorage.setItem("template_format", templateFormat);
    updateURL('template_format', templateFormat);

    if (templateFormat === 'custom') {
        document.getElementById('custom_format_output').style.display = 'block';
        document.getElementById('wikipedia_format_output').style.display = 'none';
    } else {
        document.getElementById('custom_format_output').style.display = 'none';
        document.getElementById('wikipedia_format_output').style.display = 'block';
    }
}

function applyLSFormats() {
	'use strict';
	var queryParams = new URL(window.location).searchParams;

	var datefmt = localStorage.getItem("datefmt");
	if (datefmt && !queryParams.has('dateformat')) {
		/**@type {HTMLInputElement} */(document.getElementById(datefmt)).checked = true;
		changeDates(datefmt);
	}

	var pipefmt = localStorage.getItem("pipefmt");
	if (pipefmt && !queryParams.has('pipeformat')) {
		/**@type {HTMLInputElement} */(document.getElementById(pipefmt)).checked = true;
		changePipes(pipefmt);
	}

    var templateFormat = localStorage.getItem("template_format");
    if (templateFormat && !queryParams.has('template_format')) {
        /**@type {HTMLSelectElement} */(document.getElementById('template_format')).value = templateFormat;
    }
}

function updateURL(key, value) {
    'use strict';
    var url = new URL(window.location);
    url.searchParams.set(key, value);
    window.history.pushState({}, '', url);
}

window.onload = function () {
    document.getElementById('form').addEventListener('submit', function (e) {
        e.preventDefault();
        var url = new URL(window.location);
        var userInput = document.getElementById('user_input').value;
        var inputType = document.getElementById('input_type').value;
        var dateformat = getCheckedRadio('dateformat').value;
        var pipeformat = getCheckedRadio('pipeformat').value;
        var templateFormat = document.getElementById('template_format').value;

        url.searchParams.set('user_input', userInput);
        url.searchParams.set('input_type', inputType);
        url.searchParams.set('dateformat', dateformat);
        url.searchParams.set('pipeformat', pipeformat);
        url.searchParams.set('template_format', templateFormat);

        window.location.href = url.href;
    });
    applyLSFormats();
    onTemplateFormatChange(); // Call on page load to set initial visibility

    var customElem = document.getElementById('custom_ref');
    var shortElem = document.getElementById('shortened');
    var namedElem = document.getElementById('named_ref');

    onInputTypeSelect(); // Call on page load to set initial placeholder
};
