// Copyright 2022 Stock Parfait

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

var colors = [
    'Black',
    'DarkBlue',
    'Blue',
    'DarkGreen',
    'Green',
    'Teal',
    'MidnightBlue',
    'Indigo',
    'DarkOliveGreen',
    'DimGray',
    'OliveDrab',
    'LawnGreen',
    'Aqua',
    'Aquamarine',
    'Maroon',
    'Purple',
    'Olive',
    'DarkRed',
    'DarkMagenta',
    'SaddleBrown',
    'Brown',
    'MediumVioletRed',
    'GoldenRod',
    'Crimson',
    'Red',
];

function nextColor() {
    return colors[Math.floor(Math.random() * colors.length)];
}

// errorMsg adds an error message block to elem with the content of msg.
function errorMsg(elem, msg) {
    var err = document.createElement('p');
    err.className = 'error_msg';
    err.innerHTML = msg;
    elem.appendChild(err)
}

// initPlots adds all the plots configured in DATA to the DOM element 'elem'.
function initPlots(elem, data) {
    if(data.Groups == null) {
	errorMsg(elem, 'File <code>data.js</code> contains no plots.');
	return;
    }
    for(var i = 0; i < data.Groups.length; i++) {
	var group = data.Groups[i];
	if (group.Kind == 'KindSeries') {
	    addGroupSeries(elem, group);
	} else {
	    addGroupXY(elem, group);
	}
    }
}

function addGroupSeries(elem, group) {
    if(group.Graphs == null) {
	return
    }
    for(var i = 0; i < group.Graphs.length; i++) {
	var graph = group.Graphs[i];
	addGraphSeries(elem, graph, group.MinDate, group.MaxDate, group.XLogScale);
    }
}

function addGroupXY(elem, group) {
    if(group.Graphs == null) {
	return
    }
    for(var i = 0; i < group.Graphs.length; i++) {
	var graph = group.Graphs[i];
	addGraphXY(elem, graph, group.MinX, group.MaxX, group.XLogScale);
    }
}

function addGraphSeries(elem, graph, minDate, maxDate, xLogScale) {
    canvas = addCanvas(elem, graph.Title);
}

function addGraphXY(elem, graph, minX, maxX, xLogScale) {
    canvas = addCanvas(elem, graph.Title);
}

function addCanvas(elem, title) {
    var chartDiv = document.createElement('div');
    chartDiv.className = 'chart_block';
    elem.appendChild(chartDiv);

    var chartTitle = document.createElement('div');
    chartTitle.className = 'chart_title';
    chartTitle.innerHTML = title;
    chartDiv.appendChild(chartTitle);

    var chartCanvas = document.createElement('canvas');
    chartDiv.appendChild(chartCanvas);
    return chartCanvas;
}

function plotDataSeries(plot) {
    return [];
}

function plotDataXY(plot) {
    return [];
}

// plotDataset generates a Chart compatible dataset object from plot.
function plotDataset(plot) {
    var data = [];
    if(plot.Kind == 'KindSeries') {
	data = plotDataSeries(plot);
    } else {
	data = plotDataXY(plot);
    }
    ds = {
	data: data,
	label: plot.YLabel,
	backgroundColor: 'white', // inside points
	borderColor: nextColor(),
	borderWidth: 2,
    };
    if(plot.ChartType == 'ChartDashed') {
	ds.borderDash = [10, 3];
    } else if(plot.ChartType == 'ChartScatter') {
	ds.showLine = false;
    }
    return ds;
}
