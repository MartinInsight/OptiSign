let KCCIChart;
let SCFIChart;
let WCIChart;
let IACIChart;
let blankSailingChart;
let FBXChart;
let XSIChart;
let MBCIChart;
let exchangeRateChart;

const DATA_JSON_URL = 'data/crawling_data.json';

document.addEventListener('DOMContentLoaded', () => {
    const setupChart = (chartId, type, datasets, additionalOptions = {}, isAggregated = false) => {
        const ctx = document.getElementById(chartId);
        if (ctx) {
            if (Chart.getChart(chartId)) {
                Chart.getChart(chartId).destroy();
            }

            const defaultOptions = {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: {
                        display: true,
                        title: {
                            display: true,
                            text: 'Date'
                        },
                        type: 'time',
                        time: {
                            unit: isAggregated ? 'month' : 'day',
                            displayFormats: {
                                month: 'MMM \'yy',
                                day: 'M/dd'
                            },
                            tooltipFormat: 'M/d/yyyy'
                        },
                        ticks: {
                            source: 'auto',
                            autoSkipPadding: 10
                        },
                        grid: {
                            display: false
                        }
                    },
                    y: {
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: 'Value'
                        },
                        ticks: {
                            count: 5
                        },
                        grid: {
                            display: false
                        }
                    }
                },
                plugins: {
                    legend: {
                        display: true,
                        position: 'right'
                    },
                    tooltip: {
                        mode: 'index',
                        intersect: false
                    }
                },
                elements: {
                    point: {
                        radius: 0
                    }
                }
            };

            if (isAggregated) {
                defaultOptions.scales.x.ticks.maxTicksLimit = 12;
            } else {
                delete defaultOptions.scales.x.ticks.maxTicksLimit;
            }

            const options = { ...defaultOptions, ...additionalOptions };
            if (options.scales && additionalOptions.scales) {
                options.scales = { ...defaultOptions.scales, ...additionalOptions.scales };
                if (options.scales.x && additionalOptions.scales.x) {
                    options.scales.x = { ...defaultOptions.scales.x, ...additionalOptions.scales.x };
                    if (isAggregated) {
                        options.scales.x.ticks.maxTicksLimit = 12;
                    } else {
                        delete options.scales.x.ticks.maxTicksLimit;
                    }
                }
                if (options.scales.y && additionalOptions.scales.y) {
                    options.scales.y = { ...defaultOptions.scales.y, ...additionalOptions.scales.y };
                    if (!additionalOptions.scales.y.ticks || !additionalOptions.scales.y.ticks.hasOwnProperty('count')) {
                        options.scales.y.ticks.count = defaultOptions.scales.y.ticks.count;
                    }
                }
            }

            let chartData = { datasets: datasets };
            if (type === 'bar' && additionalOptions.labels) {
                chartData = { labels: additionalOptions.labels, datasets: datasets };
                delete additionalOptions.labels;
            }

            return new Chart(ctx, {
                type: type,
                data: chartData,
                options: options
            });
        }
        return null;
    };

    const colors = [
        'rgba(0, 101, 126, 0.8)',
        'rgba(0, 58, 82, 0.8)',
        'rgba(40, 167, 69, 0.8)',
        'rgba(253, 126, 20, 0.8)',
        'rgba(111, 66, 193, 0.8)',
        'rgba(220, 53, 69, 0.8)',
        'rgba(23, 162, 184, 0.8)',
        'rgba(108, 117, 125, 0.8)'
    ];

    const borderColors = [
        '#00657e',
        '#003A52',
        '#218838',
        '#e68a00',
        '#5a32b2',
        '#c82333',
        '#138496',
        '#6c757d'
    ];

    let colorIndex = 0;
    const getNextColor = () => {
        const color = colors[colorIndex % colors.length];
        colorIndex++;
        return color;
    };
    const getNextBorderColor = () => {
        const color = borderColors[colorIndex % borderColors.length];
        return color;
    };

    const aggregateDataByMonth = (data, numMonths = 12) => {
        if (data.length === 0) return { aggregatedData: [], monthlyLabels: [] };

        data.sort((a, b) => new Date(a.date) - new Date(b.date));

        const monthlyDataMap = new Map();

        const latestDate = new Date(data[data.length - 1].date);
        const startDate = new Date(latestDate);
        startDate.setMonth(latestDate.getMonth() - (numMonths - 1));

        const allMonthKeys = [];
        let currentMonth = new Date(startDate.getFullYear(), startDate.getMonth(), 1);
        while (currentMonth <= latestDate) {
            allMonthKeys.push(`${currentMonth.getFullYear()}-${(currentMonth.getMonth() + 1).toString().padStart(2, '0')}`);
            currentMonth.setMonth(currentMonth.getMonth() + 1);
        }

        data.forEach(item => {
            const date = new Date(item.date);
            const yearMonth = `${date.getFullYear()}-${(date.getMonth() + 1).toString().padStart(2, '0')}`;

            if (!monthlyDataMap.has(yearMonth)) {
                monthlyDataMap.set(yearMonth, {});
            }
            const monthEntry = monthlyDataMap.get(yearMonth);

            for (const key in item) {
                if (key !== 'date' && item[key] !== null && !isNaN(item[key])) {
                    if (!monthEntry[key]) {
                        monthEntry[key] = { sum: 0, count: 0 };
                    }
                    monthEntry[key].sum += item[key];
                    monthEntry[key].count++;
                }
            }
        });

        const aggregatedData = [];
        const monthlyLabels = [];

        const allDataKeys = new Set();
        if (data.length > 0) {
            Object.keys(data[0]).forEach(key => {
                if (key !== 'date') allDataKeys.add(key);
            });
        }

        allMonthKeys.forEach(yearMonth => {
            const monthEntry = monthlyDataMap.get(yearMonth);
            const newEntry = { date: yearMonth + '-01' };

            allDataKeys.forEach(key => {
                newEntry[key] = monthEntry && monthEntry[key] && monthEntry[key].count > 0
                                ? monthEntry[key].sum / monthEntry[key].count
                                : null;
            });
            
            aggregatedData.push(newEntry);
            monthlyLabels.push(yearMonth + '-01');
        });

        return { aggregatedData: aggregatedData, monthlyLabels: monthlyLabels };
    };

    const setupSlider = (slidesSelector, intervalTime) => {
        const slides = document.querySelectorAll(slidesSelector);
        let currentSlide = 0;

        const showSlide = (index) => {
            slides.forEach((slide, i) => {
                if (i === index) {
                    slide.classList.add('active');
                } else {
                    slide.classList.remove('active');
                }
            });
        };

        const nextSlide = () => {
            currentSlide = (currentSlide + 1) % slides.length;
            showSlide(currentSlide);
        };

        if (slides.length > 0) {
            showSlide(currentSlide);
            if (slides.length > 1) {
                setInterval(nextSlide, intervalTime);
            }
        }
    };

    const cityTimezones = {
        'la': 'America/Los_Angeles',
        'ny': 'America/New_York',
        'paris': 'Europe/Paris',
        'shanghai': 'Asia/Shanghai',
        'seoul': 'Asia/Seoul',
        'sydney': 'Australia/Sydney'
    };

    function updateWorldClocks() {
        const now = new Date();
        for (const cityKey in cityTimezones) {
            const timezone = cityTimezones[cityKey];
            const options = {
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit',
                hour12: false,
                timeZone: timezone
            };
            const timeString = new Intl.DateTimeFormat('en-US', options).format(now);
            const elementId = `time-${cityKey}`;
            const timeElement = document.getElementById(elementId);
            if (timeElement) {
                timeElement.textContent = timeString;
            }
        }
    }

    const renderTable = (containerId, headers, rows) => {
        const container = document.getElementById(containerId);
        if (!container) {
            console.warn(`Table container with ID ${containerId} not found.`);
            return;
        }

        container.innerHTML = '';

        if (!headers || headers.length === 0 || !rows || rows.length === 0) {
            container.innerHTML = '<p class="text-gray-600 text-center">No data available for this table.</p>';
            return;
        }

        const table = document.createElement('table');
        table.classList.add('data-table');

        const thead = document.createElement('thead');
        const headerRow = document.createElement('tr');
        headers.forEach(headerText => {
            const th = document.createElement('th');
            th.textContent = headerText;
            headerRow.appendChild(th);
        });
        thead.appendChild(headerRow);
        table.appendChild(thead);

        const tbody = document.createElement('tbody');
        rows.forEach(rowData => {
            const tr = document.createElement('tr');
            headers.forEach(header => {
                const td = document.createElement('td');
                let content = '';
                let colorClass = '';

                if (header.includes('Weekly Change')) {
                    const weeklyChange = rowData.weekly_change;
                    content = weeklyChange?.value !== undefined && weeklyChange?.percentage !== undefined
                              ? `${weeklyChange.value} (${weeklyChange.percentage})`
                              : '-';
                    colorClass = weeklyChange?.color_class || '';
                } else if (header.includes('Current Index')) {
                    content = rowData.current_index ?? '-';
                } else if (header.includes('Previous Index')) {
                    content = rowData.previous_index ?? '-';
                } else if (header.includes('항로') || header.includes('route')) {
                    const displayRouteName = rowData.route ? rowData.route.split('_').slice(1).join('_') : '-';
                    content = displayRouteName;
                } else {
                    content = rowData[header.toLowerCase().replace(/\s/g, '_')] ?? '-';
                }
                
                td.textContent = content;
                if (colorClass) {
                    td.classList.add(colorClass);
                }
                tr.appendChild(td);
            });
            tbody.appendChild(tr);
        });
        table.appendChild(tbody);
        container.appendChild(table);
    };

    const routeToDataKeyMap = {
        KCCI: {
            "종합지수": "KCCI_Composite_Index",
            "미주서안": "KCCI_US_West_Coast",
            "미주동안": "KCCI_US_East_Coast",
            "유럽": "KCCI_Europe",
            "지중해": "KCCI_Mediterranean",
            "중동": "KCCI_Middle_East",
            "호주": "KCCI_Australia",
            "남미동안": "KCCI_South_America_East_Coast",
            "남미서안": "KCCI_South_America_West_Coast",
            "남아프리카": "KCCI_South_Africa",
            "서아프리카": "KCCI_West_Africa",
            "중국": "KCCI_China",
            "일본": "KCCI_Japan",
            "동남아시아": "KCCI_Southeast_Asia"
        },
        SCFI: {
            "종합지수": "SCFI_Composite_Index",
            "미주서안": "SCFI_US_West_Coast",
            "미주동안": "SCFI_US_East_Coast",
            "북유럽": "SCFI_North_Europe",
            "지중해": "SCFI_Mediterranean",
            "동남아시아": "SCFI_Southeast_Asia",
            "중동": "SCFI_Middle_East",
            "호주/뉴질랜드": "SCFI_Australia_New_Zealand",
            "남아메리카": "SCFI_South_America",
            "일본서안": "SCFI_Japan_West_Coast",
            "일본동안": "SCFI_Japan_East_Coast",
            "한국": "SCFI_Korea",
            "동부/서부 아프리카": "SCFI_East_West_Africa",
            "남아공": "SCFI_South_Africa"
        },
        WCI: {
            "종합지수": "WCI_Composite_Index",
            "상하이 → 로테르담": "WCI_Shanghai_Rotterdam",
            "로테르담 → 상하이": "WCI_Rotterdam_Shanghai",
            "상하이 → 제노바": "WCI_Shanghai_Genoa",
            "상하이 → 로스엔젤레스": "WCI_Shanghai_Los_Angeles",
            "로스엔젤레스 → 상하이": "WCI_Los_Angeles_Shanghai",
            "상하이 → 뉴욕": "WCI_Shanghai_New_York",
            "뉴욕 → 로테르담": "WCI_New_York_Rotterdam",
            "로테르담 → 뉴욕": "WCI_Rotterdam_New_York",
        },
        IACI: {
            "종합지수": "IACI_Composite_Index"
        },
        BLANK_SAILING: {
            "Gemini Cooperation": "BLANK_SAILING_Gemini_Cooperation",
            "MSC": "BLANK_SAILING_MSC",
            "OCEAN Alliance": "BLANK_SAILING_OCEAN_Alliance",
            "Premier Alliance": "BLANK_SAILING_Premier_Alliance",
            "Others/Independent": "BLANK_SAILING_Others_Independent",
            "Total": "BLANK_SAILING_Total"
        },
        FBX: {
            "글로벌 컨테이너 운임 지수": "FBX_Composite_Index",
            "중국/동아시아 → 미주서안": "FBX_China_EA_US_West_Coast",
            "미주서안 → 중국/동아시아": "FBX_US_West_Coast_China_EA",
            "중국/동아시아 → 미주동안": "FBX_China_EA_US_East_Coast",
            "미주동안 → 중국/동아시아": "FBX_US_East_Coast_China_EA",
            "중국/동아시아 → 북유럽": "FBX_China_EA_North_Europe",
            "북유럽 → 중국/동아시아": "FBX_North_Europe_China_EA",
            "중국/동아시아 → 지중해": "FBX_China_EA_Mediterranean",
            "지중해 → 중국/동아시아": "FBX_Mediterranean_China_EA",
            "미주동안 → 북유럽": "FBX_US_East_Coast_North_Europe",
            "북유럽 → 미주동안": "FBX_North_Europe_US_East_Coast",
            "유럽 → 남미동안": "FBX_Europe_South_America_East_Coast",
            "유럽 → 남미서안": "FBX_Europe_South_America_West_Coast"
        },
        XSI: {
            "동아시아 → 북유럽": "XSI_East_Asia_North_Europe",
            "북유럽 → 동아시아": "XSI_North_Europe_East_Asia",
            "동아시아 → 미주서안": "XSI_East_Asia_US_West_Coast",
            "미주서안 → 동아시아": "XSI_US_West_Coast_East_Asia",
            "동아시아 → 남미동안": "XSI_East_Asia_South_America_East_Coast",
            "북유럽 → 미주동안": "XSI_North_Europe_US_East_Coast",
            "미주동안 → 북유럽": "XSI_US_East_Coast_North_Europe",
            "북유럽 → 남미동안": "XSI_North_Europe_South_America_East_Coast"
        },
        MBCI: {
            "MBCI": "MBCI_Value"
        }
    };

    const createDatasetsFromTableRows = (indexType, chartData, tableRows) => {
        const datasets = [];
        const mapping = routeToDataKeyMap[indexType];
        if (!mapping) {
            console.warn(`No data key mapping found for index type: ${indexType}`);
            return datasets;
        }

        tableRows.forEach(row => {
            const originalRouteName = row.route.split('_').slice(1).join('_');
            const dataKey = mapping[originalRouteName];
            
            if (dataKey !== null && dataKey !== undefined && row.current_index !== "") { 
                const mappedData = chartData.map(item => {
                    const xVal = item.date;
                    const yVal = item[dataKey];
                    return { x: xVal, y: yVal };
                });

                const filteredMappedData = mappedData.filter(point => point.y !== null && point.y !== undefined);

                if (filteredMappedData.length > 0) {
                    datasets.push({
                        label: originalRouteName,
                        data: filteredMappedData,
                        backgroundColor: getNextColor(),
                        borderColor: getNextBorderColor(),
                        borderWidth: (originalRouteName.includes('종합지수') || originalRouteName.includes('글로벌 컨테이너 운임 지수') || originalRouteName.includes('US$/40ft') || originalRouteName.includes('Index(종합지수)')) ? 2 : 1,
                        fill: false
                    });
                } else {
                    console.warn(`WARNING: No valid data points found for ${indexType} - route: '${originalRouteName}' (dataKey: '${dataKey}'). Skipping dataset.`);
                }
            } else if (dataKey === null) {
                console.info(`INFO: Skipping chart dataset for route '${row.route}' in ${indexType} as it's explicitly mapped to null (no chart data expected).`);
            } else {
                console.warn(`WARNING: No dataKey found or current_index is empty for ${indexType} - route: '${row.route}'. Skipping dataset.`);
            }
        });
        return datasets;
    };


    async function loadAndDisplayData() {
        let allDashboardData = {};
        try {
            const response = await fetch(DATA_JSON_URL);
            if (!response.ok) {
                throw new Error(`HTTP error! Status: ${response.status}`);
            }
            allDashboardData = await response.json();
            console.log("Loaded all dashboard data:", allDashboardData);

            const chartDataBySection = allDashboardData.chart_data || {};
            const weatherData = allDashboardData.weather_data || {};
            const exchangeRatesData = allDashboardData.exchange_rate || [];
            const tableDataBySection = allDashboardData.table_data || {};

            if (Object.keys(chartDataBySection).length === 0) {
                console.warn("No chart data sections found in the JSON file.");
                document.querySelector('.chart-slider-container').innerHTML = '<p class="placeholder-text">No chart data available.</p>';
                return;
            }

            const currentWeatherData = weatherData.current || {};
            const forecastWeatherData = weatherData.forecast || [];

            document.getElementById('temperature-current').textContent = currentWeatherData.LA_Temperature ? `${currentWeatherData.LA_Temperature}°F` : '--°F';
            document.getElementById('status-current').textContent = currentWeatherData.LA_WeatherStatus || 'Loading...';
            const weatherIconUrl = (status) => {
                const base = 'https://placehold.co/80x80/';
                const defaultColor = 'cccccc';
                const textColor = 'ffffff';
                if (status) {
                    const lowerStatus = status.toLowerCase();
                    if (lowerStatus.includes('clear') || lowerStatus.includes('맑음')) return `${base}00657e/${textColor}?text=SUN`;
                    if (lowerStatus.includes('cloud') || lowerStatus.includes('구름')) return `${base}003A52/${textColor}?text=CLOUD`;
                    if (lowerStatus.includes('rain') || lowerStatus.includes('비')) return `${base}28A745/${textColor}?text=RAIN`;
                    if (lowerStatus.includes('snow') || lowerStatus.includes('눈')) return `${base}17A2B8/${textColor}?text=SNOW`;
                }
                return `${base}${defaultColor}/${textColor}?text=Icon`;
            };
            document.getElementById('weather-icon-current').src = weatherIconUrl(currentWeatherData.LA_WeatherStatus);

            document.getElementById('humidity-current').textContent = currentWeatherData.LA_Humidity ? `${currentWeatherData.LA_Humidity}%` : '--%';
            document.getElementById('wind-speed-current').textContent = currentWeatherData.LA_WindSpeed ? `${currentWeatherData.LA_WindSpeed} mph` : '-- mph';
            document.getElementById('pressure-current').textContent = currentWeatherData.LA_Pressure ? `${currentWeatherData.LA_Pressure} hPa` : '-- hPa';
            document.getElementById('visibility-current').textContent = currentWeatherData.LA_Visibility ? `${currentWeatherData.LA_Visibility} mile` : '-- mile';
            document.getElementById('sunrise-time').textContent = currentWeatherData.LA_Sunrise || '--';
            document.getElementById('sunset-time').textContent = currentWeatherData.LA_Sunset || '--';

            const forecastBody = document.getElementById('forecast-body');
            forecastBody.innerHTML = '';
            if (forecastWeatherData.length > 0) {
                forecastWeatherData.slice(0, 7).forEach(day => {
                    const row = forecastBody.insertRow();
                    row.insertCell().textContent = day.date || '--';
                    row.insertCell().textContent = day.min_temp ? `${day.min_temp}°F` : '--';
                    row.insertCell().textContent = day.max_temp ? `${day.max_temp}°F` : '--';
                    row.insertCell().textContent = day.status || '--';
                });
            } else {
                forecastBody.innerHTML = '<tr><td colspan="4">No forecast data available.</td></tr>';
            }


            const filteredExchangeRates = exchangeRatesData.slice(Math.max(exchangeRatesData.length - 30, 0));

            const currentExchangeRate = filteredExchangeRates.length > 0 ? filteredExchangeRates[filteredExchangeRates.length - 1].rate : null;
            document.getElementById('current-exchange-rate-value').textContent = currentExchangeRate ? `${currentExchangeRate.toFixed(2)} KRW` : 'Loading...';

            if (exchangeRateChart) exchangeRateChart.destroy();
            
            const exchangeRateDatasets = [{
                label: 'USD/KRW Exchange Rate',
                data: filteredExchangeRates.map(item => ({ x: item.date, y: item.rate })),
                backgroundColor: 'rgba(253, 126, 20, 0.5)',
                borderColor: '#e68a00',
                borderWidth: 2,
                fill: false,
                pointRadius: 0
            }];
            console.log("Exchange Rate Chart Datasets (before setup):", exchangeRateDatasets);
            console.log("Exchange Rate Chart Data Sample (first 5 points):", exchangeRateDatasets[0].data.slice(0, 5));


            exchangeRateChart = setupChart(
                'exchangeRateChartCanvas', 'line',
                exchangeRateDatasets,
                {
                    scales: {
                        x: {
                            type: 'time',
                            time: { 
                                unit: 'day', 
                                displayFormats: { day: 'MM/dd' },
                                tooltipFormat: 'M/d/yyyy'
                            },
                            ticks: { autoSkipPadding: 10 }
                        },
                        y: {
                            beginAtZero: false,
                            ticks: { count: 5 },
                            grid: { display: false }
                        }
                    },
                    plugins: {
                        legend: { display: false }
                    }
                },
                false
            );


            colorIndex = 0;

            const KCCIData = chartDataBySection.KCCI || [];
            KCCIData.sort((a, b) => new Date(a.date) - new Date(b.date));
            const KCCITableRows = tableDataBySection.KCCI ? tableDataBySection.KCCI.rows : [];
            const KCCIDatasets = createDatasetsFromTableRows('KCCI', KCCIData, KCCITableRows);
            KCCIChart = setupChart('KCCIChart', 'line', KCCIDatasets, {}, false);
            renderTable('KCCITableContainer', tableDataBySection.KCCI.headers, KCCITableRows);


            colorIndex = 0;
            const SCFIData = chartDataBySection.SCFI || [];
            SCFIData.sort((a, b) => new Date(a.date) - new Date(b.date));
            const SCFITableRows = tableDataBySection.SCFI ? tableDataBySection.SCFI.rows : [];
            const SCFIDatasets = createDatasetsFromTableRows('SCFI', SCFIData, SCFITableRows);
            SCFIChart = setupChart('SCFIChart', 'line', SCFIDatasets, {}, false);
            renderTable('SCFITableContainer', tableDataBySection.SCFI.headers, SCFITableRows);


            colorIndex = 0;
            const WCIData = chartDataBySection.WCI || [];
            WCIData.sort((a, b) => new Date(a.date) - new Date(b.date));
            const WCITableRows = tableDataBySection.WCI ? tableDataBySection.WCI.rows : [];
            const WCIDatasets = createDatasetsFromTableRows('WCI', WCIData, WCITableRows);
            WCIChart = setupChart('WCIChart', 'line', WCIDatasets, {}, false);
            renderTable('WCITableContainer', tableDataBySection.WCI.headers, WCITableRows);


            colorIndex = 0;
            const IACIData = chartDataBySection.IACI || [];
            IACIData.sort((a, b) => new Date(a.date) - new Date(b.date));
            const IACITableRows = tableDataBySection.IACI ? tableDataBySection.IACI.rows : [];
            const IACIDatasets = createDatasetsFromTableRows('IACI', IACIData, IACITableRows);
            IACIChart = setupChart('IACIChart', 'line', IACIDatasets, {}, false);
            renderTable('IACITableContainer', tableDataBySection.IACI.headers, IACITableRows);


            const blankSailingRawData = chartDataBySection.BLANK_SAILING || [];
            const { aggregatedData: aggregatedBlankSailingData, monthlyLabels: blankSailingChartDates } = aggregateDataByMonth(blankSailingRawData, 12);
            
            colorIndex = 0;
            const blankSailingDatasets = [
                {
                    label: "Gemini Cooperation",
                    data: aggregatedBlankSailingData.map(item => ({ x: item.date, y: item.BLANK_SAILING_Gemini_Cooperation })),
                    backgroundColor: getNextColor(),
                    borderColor: getNextBorderColor(),
                    borderWidth: 1
                },
                {
                    label: "MSC",
                    data: aggregatedBlankSailingData.map(item => ({ x: item.date, y: item.BLANK_SAILING_MSC })),
                    backgroundColor: getNextColor(),
                    borderColor: getNextBorderColor(),
                    borderWidth: 1
                },
                {
                    label: "OCEAN Alliance",
                    data: aggregatedBlankSailingData.map(item => ({ x: item.date, y: item.BLANK_SAILING_OCEAN_Alliance })),
                    backgroundColor: getNextColor(),
                    borderColor: getNextBorderColor(),
                    borderWidth: 1
                },
                {
                    label: "Premier Alliance",
                    data: aggregatedBlankSailingData.map(item => ({ x: item.date, y: item.BLANK_SAILING_Premier_Alliance })),
                    backgroundColor: getNextColor(),
                    borderColor: getNextBorderColor(),
                    borderWidth: 1
                },
                {
                    label: "Others/Independent",
                    data: aggregatedBlankSailingData.map(item => ({ x: item.date, y: item.BLANK_SAILING_Others_Independent })),
                    backgroundColor: getNextColor(),
                    borderColor: getNextBorderColor(),
                    borderWidth: 1
                },
                {
                    label: "Total",
                    data: aggregatedBlankSailingData.map(item => ({ x: item.date, y: item.BLANK_SAILING_Total })),
                    backgroundColor: getNextColor(),
                    borderColor: getNextBorderColor(),
                    borderWidth: 1
                }
            ].filter(dataset => dataset.data.some(point => point.y !== null && point.y !== undefined));

            blankSailingChart = setupChart(
                'blankSailingChart', 'bar',
                blankSailingDatasets,
                {
                    scales: {
                        x: {
                            stacked: true,
                            type: 'time',
                            time: {
                                unit: 'month',
                                displayFormats: { month: 'MMM \'yy' },
                                tooltipFormat: 'M/d/yyyy'
                            },
                            title: { display: true, text: 'Month' }
                        },
                        y: {
                            stacked: true,
                            beginAtZero: true,
                            title: { display: true, text: 'Blank Sailings' }
                        }
                    }
                },
                true
            );
            const blankSailingTableRows = tableDataBySection.BLANK_SAILING ? tableDataBySection.BLANK_SAILING.rows : [];
            renderTable('blankSailingTableContainer', tableDataBySection.BLANK_SAILING.headers, blankSailingTableRows);


            colorIndex = 0;
            const FBXData = chartDataBySection.FBX || [];
            FBXData.sort((a, b) => new Date(a.date) - new Date(b.date));
            const FBXTableRows = tableDataBySection.FBX ? tableDataBySection.FBX.rows : [];
            const FBXDatasets = createDatasetsFromTableRows('FBX', FBXData, FBXTableRows);
            FBXChart = setupChart('FBXChart', 'line', FBXDatasets, {}, false);
            renderTable('FBXTableContainer', tableDataBySection.FBX.headers, FBXTableRows);


            colorIndex = 0;
            const XSIData = chartDataBySection.XSI || [];
            XSIData.sort((a, b) => new Date(a.date) - new Date(b.date));
            const XSITableRows = tableDataBySection.XSI ? tableDataBySection.XSI.rows : [];
            const XSIDatasets = createDatasetsFromTableRows('XSI', XSIData, XSITableRows);
            XSIChart = setupChart('XSIChart', 'line', XSIDatasets, {}, false);
            renderTable('XSITableContainer', tableDataBySection.XSI.headers, XSITableRows);

            colorIndex = 0;
            const MBCIData = chartDataBySection.MBCI || [];
            MBCIData.sort((a, b) => new Date(a.date) - new Date(b.date));
            const MBCITableRows = tableDataBySection.MBCI ? tableDataBySection.MBCI.rows : [];
            const MBCIDatasets = createDatasetsFromTableRows('MBCI', MBCIData, MBCITableRows);
            MBCIChart = setupChart('MBCIChart', 'line', MBCIDatasets, {}, false);
            renderTable('MBCITableContainer', tableDataBySection.MBCI.headers, MBCITableRows);


            setupSlider('.chart-slide', 5000);
            setupSlider('.top-info-slide', 7000); // Changed to top-info-slide

        } catch (error) {
            console.error('Error loading or processing data:', error);
            document.querySelector('.chart-slider-container').innerHTML = '<p class="error-text">Failed to load chart data. Please check the data source and console for errors.</p>';
            document.querySelector('.table-slider-container').innerHTML = '<p class="error-text">Failed to load table data. Please check the data source and console for errors.</p>';
            document.getElementById('weather-info').innerHTML = '<p class="error-text">Failed to load weather data.</p>';
            document.getElementById('exchange-rate-info').innerHTML = '<p class="error-text">Failed to load exchange rate data.</p>';
        }
    }

    loadAndDisplayData();

    setInterval(updateWorldClocks, 1000);
});
