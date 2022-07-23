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

// Each color provides a matching foreground / background pair of colors.
var colors = [
    {'fg': 'Black', 'bg': 'LightGray'},
    {'fg': 'DarkBlue', 'bg': 'CornflowerBlue'},
    {'fg': 'Blue', 'bg': 'CornflowerBlue'},
    {'fg': 'DarkGreen', 'bg': 'GreenYellow'},
    {'fg': 'Green', 'bg': 'LightGreen'},
    {'fg': 'Teal', 'bg': 'Turquoise'},
    {'fg': 'MidnightBlue', 'bg': 'SkyBlue'},
    {'fg': 'Indigo', 'bg': 'Fuchsia'},
    {'fg': 'DarkOliveGreen', 'bg': 'DarkSeaGreen'},
    {'fg': 'DimGray', 'bg': 'Gainsboro'},
    {'fg': 'OliveDrab', 'bg': 'YellowGreen'},
    {'fg': 'ForestGreen', 'bg': 'PaleGreen'},
    {'fg': 'DarkCyan', 'bg': 'Cyan'},
    {'fg': 'MediumAquaMarine', 'bg': 'AquaMarine'},
    {'fg': 'Maroon', 'bg': 'Red'},
    {'fg': 'Purple', 'bg': 'MediumPurple'},
    {'fg': 'Olive', 'bg': 'Goldenrod'},
    {'fg': 'DarkRed', 'bg': 'LightCoral'},
    {'fg': 'DarkMagenta', 'bg': 'Magenta'},
    {'fg': 'SaddleBrown', 'bg': 'Peru'},
    {'fg': 'Brown', 'bg': 'SandyBrown'},
    {'fg': 'MediumVioletRed', 'bg': 'PaleVioletRed'},
    {'fg': 'GoldenRod', 'bg': 'PaleGoldenRod'},
    {'fg': 'Crimson', 'bg': 'DeepPink'},
];

// nextColor returns an object with 'fg' and 'bg' attributes for the foreground
// and background colors.
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

function addGroupElem(elem, group) {
    var groupDiv = document.createElement('div');
    groupDiv.className = 'group_block';
    elem.appendChild(groupDiv);

    if(group.Title != null) {
	var groupTitle = document.createElement('div');
	groupTitle.className = 'group_title';
	groupTitle.innerHTML = group.Title;
	groupDiv.appendChild(groupTitle);
    }
    return groupDiv;
}

function addGroupSeries(elem, group) {
    if(group.Graphs == null) {
	return
    }
    var groupElem = addGroupElem(elem, group);
    for(var i = 0; i < group.Graphs.length; i++) {
	var graph = group.Graphs[i];
	addGraphSeries(groupElem, graph, group.MinDate, group.MaxDate, group.XLogScale);
    }
}

function addGroupXY(elem, group) {
    if(group.Graphs == null) {
	return
    }
    var groupElem = addGroupElem(elem, group);
    for(var i = 0; i < group.Graphs.length; i++) {
	var graph = group.Graphs[i];
	addGraphXY(groupElem, graph, group.MinX, group.MaxX, group.XLogScale);
    }
}

function addGraphSeries(elem, graph, minDate, maxDate, xLogScale) {
    canvas = addCanvas(elem, graph.Title);
    var conf = {
	type: 'line',
	data: { datasets: [] },
	options: {
	    maintainAspectRatio: false,
            scales: {
		x: {
		    type: 'time',
		    ticks: {source: 'auto'},
		    time: {
			displayFormats: {day: 'yyyy-MM-dd'},
			minUnit: 'day',
		    },
		    min: minDate,
		    max: maxDate,
		},
	    },
	},
    }
    addPlots(graph, conf);
    var chart = new Chart(canvas.getContext('2d'), conf);
}

function addGraphXY(elem, graph, minX, maxX, xLogScale) {
    canvas = addCanvas(elem, graph.Title);
    var conf = {
	type: 'line',
	data: { datasets: [] },
	options: {
	    maintainAspectRatio: false,
            scales: {
		x: {
		    type: xLogScale? 'logarithmic' : 'linear',
		    ticks: {source: 'auto'},
		    min: minX,
		    max: maxX,
		},
	    },
	},
    }
    addPlots(graph, conf);
    var chart = new Chart(canvas.getContext('2d'), conf);
}

function addPlots(graph, conf) {
    if(graph.Plots == null) {
	return;
    }
    var labelsLeft = {};
    var labelsRight = {};
    for(var i=0; i<graph.Plots.length; i++) {
	var plot = graph.Plots[i];
	conf.data.datasets.push(plotDataset(plot));
	if(plot.LeftAxis) {
	    labelsLeft[plot.YLabel] = true;
	} else {
	    labelsRight[plot.YLabel] = true;
	}
    }

    if(Object.keys(labelsLeft).length > 0) {
	var labelStr = '';
	for(l in labelsLeft) {
	    labelStr += (labelStr == '' ? '' : ', ') + l;
	}
	conf.options.scales.yLeft = {
	    type: graph.YLogScale ? 'logarithmic' : 'linear',
	    position: 'left',
	    title: {display: true, text: labelStr},
	};
    }
    if(Object.keys(labelsRight).length > 0) {
	var labelStr = '';
	for(l in labelsRight) {
	    labelStr += (labelStr == '' ? '' : ', ') + l;
	}
	conf.options.scales.yRight = {
	    type: graph.YLogScale ? 'logarithmic' : 'linear',
	    position: 'right',
	    title: {display: true, text: labelStr},
	};
    }
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
    var data = [];
    for(var i=0; i<plot.Dates.length; i++) {
	data.push({x: plot.Dates[i], y: plot.Y[i]});
    }
    return data;
}

function plotDataXY(plot) {
    var data = [];
    for(var i=0; i<plot.Y.length; i++) {
	data.push({x: plot.X[i], y: plot.Y[i]});
    }
    return data;
}

function chartType(tp) {
    if(tp == 'ChartBars') {
	return 'bar';
    }
    return 'line';
}

// plotDataset generates a Chart compatible dataset object from plot.
function plotDataset(plot) {
    var data = [];
    if(plot.Kind == 'KindSeries') {
	data = plotDataSeries(plot);
    } else {
	data = plotDataXY(plot);
    }
    var color = nextColor();
    var ds = {
	data: data,
	type: chartType(plot.ChartType),
	yAxisID: plot.LeftAxis ? "yLeft" : "yRight",
	label: plot.Legend,
	backgroundColor: color.bg, // inside points or bars
	borderColor: color.fg,
	borderWidth: 2,
    };
    if(plot.ChartType == 'ChartLine' || plot.ChartType == 'ChartDashed') {
	ds.elements = {
	    point: {
		radius: 0,
		hitRadius: 10,
		hoverRadius: 5,
	    },
	};
    }
    if(plot.ChartType == 'ChartDashed') {
	ds.borderDash = [10, 3];
    }
    if(plot.ChartType == 'ChartScatter') {
	ds.showLine = false;
	ds.elements = {
	    point: {
		radius: 1,
		hoverRadius: 5,
	    },
	};
    }
    return ds;
}
