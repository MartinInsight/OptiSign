// Global variables
let dashboardData = {};
let topInfoCarouselIndex = 0;
let mainChartsCarouselIndex = 0;
let topInfoCarouselInterval;
let mainChartsCarouselInterval;

// Configuration for chart and table data mappings (similar to Python script for consistency)
const SECTION_MAPPINGS_FRONTEND = {
    "KCCI": {
        "chartId": "kcciChart",
        "tableId": "kcciTable",
        "tableCurrentHeaderId": "kcciCurrentHeader",
        "tablePreviousHeaderId": "kcciPreviousHeader",
        "data_cols_map": {
            "종합지수": "Composite_Index", "미주서안": "US_West_Coast", "미주동안": "US_East_Coast",
            "유럽": "Europe", "지중해": "Mediterranean", "중동": "Middle_East", "호주": "Australia",
            "남미동안": "South_America_East_Coast", "남미서안": "South_America_West_Coast",
            "남아프리카": "South_Africa", "서아프리카": "West_Africa", "중국": "China",
            "일본": "Japan", "동남아시아": "Southeast_Asia"
        }
    },
    "SCFI": {
        "chartId": "scfiChart",
        "tableId": "scfiTable",
        "tableCurrentHeaderId": "scfiCurrentHeader",
        "tablePreviousHeaderId": "scfiPreviousHeader",
        "data_cols_map": {
            "종합지수": "Composite_Index_1", "미주서안": "US_West_Coast_1", "미주동안": "US_East_Coast_1",
            "북유럽": "North_Europe", "지중해": "Mediterranean_1", "동남아시아": "Southeast_Asia_1",
            "중동": "Middle_East_1", "호주/뉴질랜드": "Australia_New_Zealand_SCFI",
            "남아메리카": "South_America_SCFI", "일본서안": "Japan_West_Coast_SCFI",
            "일본동안": "Japan_East_Coast_SCFI", "한국": "Korea_SCFI",
            "동부/서부 아프리카": "East_West_Africa_SCFI", "남아공": "South_Africa_SCFI"
        }
    },
    "WCI": {
        "chartId": "wciChart",
        "tableId": "wciTable",
        "tableCurrentHeaderId": "wciCurrentHeader",
        "tablePreviousHeaderId": "wciPreviousHeader",
        "data_cols_map": {
            "종합지수": "Composite_Index_2", "상하이 → 로테르담": "Shanghai_Rotterdam_WCI",
            "로테르담 → 상하이": "Rotterdam_Shanghai_WCI", "상하이 → 제노바": "Shanghai_Genoa_WCI",
            "상하이 → 로스엔젤레스": "Shanghai_Los_Angeles_WCI", "로스엔젤레스 → 상하이": "Los_Angeles_Shanghai_WCI",
            "상하이 → 뉴욕": "Shanghai_New_York_WCI", "뉴욕 → 로테르담": "New_York_Rotterdam_WCI",
            "로테르담 → 뉴욕": "Rotterdam_New_York_WCI",
        }
    },
    "IACI": {
        "chartId": "iaciChart",
        "tableId": "iaciTable",
        "tableCurrentHeaderId": "iaciCurrentHeader",
        "tablePreviousHeaderId": "iaciPreviousHeader",
        "data_cols_map": {
            "종합지수": "Composite_Index_3"
        }
    },
    "BLANK_SAILING": {
        "chartId": "blankSailingChart",
        "tableId": "blankSailingTable",
        "tableCurrentHeaderId": "blankSailingCurrentHeader",
        "tablePreviousHeaderId": "blankSailingPreviousHeader",
        "data_cols_map": {
            "Index": "Index_Blank_Sailing", "Gemini Cooperation": "Gemini_Cooperation_Blank_Sailing",
            "MSC": "MSC_Alliance_Blank_Sailing", "OCEAN Alliance": "OCEAN_Alliance_Blank_Sailing",
            "Premier Alliance": "Premier_Alliance_Blank_Sailing",
            "Others/Independent": "Others_Independent_Blank_Sailing", "Total": "Total_Blank_Sailings"
        }
    },
    "FBX": {
        "chartId": "fbxChart",
        "tableId": "fbxTable",
        "tableCurrentHeaderId": "fbxCurrentHeader",
        "tablePreviousHeaderId": "fbxPreviousHeader",
        "data_cols_map": {
            "종합지수": "Composite_Index_4", "중국/동아시아 → 미주서안": "China_EA_US_West_Coast_FBX",
            "미주서안 → 중국/동아시아": "US_West_Coast_China_EA_FBX", "중국/동아시아 → 미주동안": "China_EA_US_East_Coast_FBX",
            "미주동안 → 중국/동아시아": "US_East_Coast_China_EA_FBX", "중국/동아시아 → 북유럽": "China_EA_North_Europe_FBX",
            "북유럽 → 중국/동아시아": "North_Europe_China_EA_FBX", "중국/동아시아 → 지중해": "China_EA_Mediterranean_FBX",
            "지중해 → 중국/동아시아": "Mediterranean_China_EA_FBX", "미주동안 → 북유럽": "US_East_Coast_North_Europe_FBX",
            "북유럽 → 미주동안": "North_Europe_US_East_Coast_FBX", "유럽 → 남미동안": "Europe_South_America_East_Coast_FBX",
            "유럽 → 남미서안": "Europe_South_America_West_Coast_FBX",
        }
    },
    "XSI": {
        "chartId": "xsiChart",
        "tableId": "xsiTable",
        "tableCurrentHeaderId": "xsiCurrentHeader",
        "tablePreviousHeaderId": "xsiPreviousHeader",
        "data_cols_map": {
            "동아시아 → 북유럽": "XSI_East_Asia_North_Europe", "북유럽 → 동아시아": "XSI_North_Europe_East_Asia",
            "동아시아 → 미주서안": "XSI_East_Asia_US_West_Coast", "미주서안 → 동아시아": "XSI_US_West_Coast_East_Asia",
            "동아시아 → 남미동안": "XSI_East_Asia_South_America_East_Coast",
            "북유럽 → 미주동안": "XSI_North_Europe_US_East_Coast",
            "미주동안 → 북유럽": "XSI_US_East_Coast_North_Europe",
            "북유럽 → 남미동안": "XSI_North_Europe_South_America_East_Coast"
        }
    },
    "MBCI": {
        "chartId": "mbciChart",
        "tableId": "mbciTable",
        "tableCurrentHeaderId": "mbciCurrentHeader",
        "tablePreviousHeaderId": "mbciPreviousHeader",
        "data_cols_map": {
            "MBCI": "MBCI_MBCI_Value",
        }
    }
};

// Utility function to generate distinct colors for chart lines
function getChartColors(numColors) {
    const colors = [
        '#4299E1', '#F6AD55', '#48BB78', '#ED8936', '#9F7AEA', '#ECC94B', '#63B3ED', '#FC8181',
        '#81E6D9', '#D53F8C', '#F6E05E', '#B794F4', '#38B2AC', '#F56565', '#4FD1C5', '#A0AEC0'
    ];
    return Array.from({ length: numColors }, (_, i) => colors[i % colors.length]);
}

// Function to create and manage carousels
function setupCarousel(carouselInnerId, dotsContainerId, intervalTime = 10000) {
    const carouselInner = document.getElementById(carouselInnerId);
    const dotsContainer = document.getElementById(dotsContainerId);
    const items = carouselInner.children;
    const totalItems = items.length;
    let currentIndex = 0;
    let interval;

    // Create dots
    for (let i = 0; i < totalItems; i++) {
        const dot = document.createElement('span');
        dot.classList.add('dot');
        dot.addEventListener('click', () => {
            moveToSlide(i);
            resetInterval();
        });
        dotsContainer.appendChild(dot);
    }

    const dots = dotsContainer.children;

    function updateCarousel() {
        carouselInner.style.transform = `translateX(-${currentIndex * 100}%)`;
        Array.from(dots).forEach((dot, idx) => {
            dot.classList.toggle('active', idx === currentIndex);
        });
    }

    function moveToSlide(index) {
        currentIndex = index;
        updateCarousel();
    }

    function nextSlide() {
        currentIndex = (currentIndex + 1) % totalItems;
        updateCarousel();
    }

    function resetInterval() {
        clearInterval(interval);
        interval = setInterval(nextSlide, intervalTime);
    }

    // Initial setup
    updateCarousel();
    resetInterval();

    return interval; // Return interval ID to clear later if needed
}

// --- Chart Creation Functions ---

// General options for line charts
const commonLineChartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    scales: {
        x: {
            type: 'time',
            time: {
                unit: 'month',
                displayFormats: {
                    month: 'MM/01/yyyy' // User requested format
                },
                tooltipFormat: 'M/d/yyyy' // Tooltip format
            },
            ticks: {
                autoSkip: true,
                maxRotation: 0,
                minRotation: 0,
                source: 'auto',
                color: '#666'
            },
            grid: {
                display: false // Remove X-axis grid lines
            }
        },
        y: {
            ticks: {
                maxTicksLimit: 5, // Limit to 5 ticks
                color: '#666'
            },
            grid: {
                display: true, // Display Y-axis grid lines
                drawOnChartArea: true, // Draw on chart area
                drawTicks: false, // Do not draw ticks on grid lines
                color: 'rgba(200, 200, 200, 0.2)', // Light grey color
                z: -1 // Draw behind the graph
            }
        }
    },
    plugins: {
        legend: {
            position: 'right', // Legend on the right
            labels: {
                color: '#333'
            }
        },
        tooltip: {
            callbacks: {
                title: function(context) {
                    return dayjs(context[0].parsed.x).format('M/D/YYYY'); // Tooltip date format
                }
            }
        }
    },
    elements: {
        point: {
            radius: 0 // Remove data points
        }
    },
    animation: {
        duration: 0 // Disable animation for faster rendering
    }
};

// Function to create a line chart
function createLineChart(chartId, rawData, dataColsMap) {
    const ctx = document.getElementById(chartId);
    if (!ctx) {
        console.warn(`Canvas element with ID '${chartId}' not found.`);
        return;
    }

    // Filter data for the last 12 months
    const oneYearAgo = dayjs().subtract(1, 'year').startOf('day');
    const filteredData = rawData.filter(d => dayjs(d.date).isAfter(oneYearAgo) || dayjs(d.date).isSame(oneYearAgo, 'day'));

    console.log(`${chartId} Raw Data:`, rawData);
    console.log(`${chartId} Filtered Data (last 12 months):`, filteredData);

    const datasets = [];
    const labels = filteredData.map(d => d.date); // Use all dates from filtered data for labels
    const colors = getChartColors(Object.keys(dataColsMap).length);
    let colorIndex = 0;

    for (const displayLabel in dataColsMap) {
        if (displayLabel === "날짜") continue; // Skip date column
        const jsonKey = dataColsMap[displayLabel];
        const dataPoints = filteredData.map(d => ({
            x: d.date,
            y: d[jsonKey]
        }));

        // Filter out datasets that have no valid data points (all null/undefined)
        const hasValidData = dataPoints.some(dp => dp.y !== null && dp.y !== undefined);
        if (hasValidData) {
            datasets.push({
                label: displayLabel,
                data: dataPoints,
                borderColor: colors[colorIndex++],
                tension: 0.1,
                fill: false,
                pointRadius: 0 // Ensure points are removed
            });
        } else {
            console.warn(`Dataset for '${displayLabel}' in ${chartId} has no valid data and will not be displayed.`);
        }
    }

    console.log(`${chartId} Datasets (before setup):`, datasets);

    if (datasets.length === 0 || datasets.every(dataset => dataset.data.every(d => d.y === null || d.y === undefined))) {
        console.warn(`${chartId} Datasets are empty or have no data.`);
        return;
    }

    new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels, // Chart.js time scale handles labels automatically from data.x
            datasets: datasets
        },
        options: commonLineChartOptions
    });
}

// Function to create a bar chart (specifically for Blank Sailing)
function createBarChart(chartId, rawData, dataColsMap) {
    const ctx = document.getElementById(chartId);
    if (!ctx) {
        console.warn(`Canvas element with ID '${chartId}' not found.`);
        return;
    }

    // Aggregate data by month for the last 12 months
    const aggregatedData = aggregateDataByMonth(rawData, 'date', Object.values(dataColsMap).filter(key => key !== 'date'));

    console.log(`${chartId} Aggregated Data (last 12 months):`, aggregatedData);

    const labels = aggregatedData.map(d => dayjs(d.month).format('MM/01/yyyy')); // X-axis labels
    const datasets = [];
    const colors = getChartColors(Object.keys(dataColsMap).length);
    let colorIndex = 0;

    for (const displayLabel in dataColsMap) {
        if (displayLabel === "날짜") continue;
        const jsonKey = dataColsMap[displayLabel];
        datasets.push({
            label: displayLabel,
            data: aggregatedData.map(d => d[jsonKey]),
            backgroundColor: colors[colorIndex++],
            borderColor: colors[colorIndex - 1],
            borderWidth: 1
        });
    }

    console.log(`${chartId} Datasets (before setup):`, datasets);

    if (datasets.length === 0 || datasets.every(dataset => dataset.data.every(d => d === null || d === undefined))) {
        console.warn(`${chartId} Datasets are empty or have no data.`);
        return;
    }

    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: datasets
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: {
                    stacked: true,
                    ticks: {
                        color: '#666'
                    },
                    grid: {
                        display: false // Remove X-axis grid lines
                    }
                },
                y: {
                    stacked: true,
                    ticks: {
                        maxTicksLimit: 5, // Limit to 5 ticks
                        color: '#666'
                    },
                    grid: {
                        display: true, // Display Y-axis grid lines
                        drawOnChartArea: true, // Draw on chart area
                        drawTicks: false, // Do not draw ticks on grid lines
                        color: 'rgba(200, 200, 200, 0.2)', // Light grey color
                        z: -1 // Draw behind the graph
                    }
                }
            },
            plugins: {
                legend: {
                    position: 'right', // Legend on the right
                    labels: {
                        color: '#333'
                    }
                },
                tooltip: {
                    callbacks: {
                        title: function(context) {
                            // For bar charts, the label is already formatted as MM/01/yyyy
                            return context[0].label;
                        }
                    }
                }
            },
            animation: {
                duration: 0 // Disable animation for faster rendering
            }
        }
    });
}

// Function to aggregate data by month (for Blank Sailing)
function aggregateDataByMonth(data, dateKey, valueKeys) {
    const monthlyData = {};
    const today = dayjs();

    // Initialize 12 months with null values
    for (let i = 0; i < 12; i++) {
        const month = today.subtract(i, 'month').startOf('month').format('YYYY-MM-01');
        monthlyData[month] = {};
        valueKeys.forEach(key => {
            monthlyData[month][key] = null; // Initialize with null
        });
        monthlyData[month]['count'] = 0; // For averaging
    }

    data.forEach(item => {
        const date = dayjs(item[dateKey]);
        if (date.isValid()) {
            const month = date.startOf('month').format('YYYY-MM-01');
            if (monthlyData[month]) { // Only process if within the last 12 months
                valueKeys.forEach(key => {
                    const value = parseFloat(item[key]);
                    if (!isNaN(value)) {
                        if (monthlyData[month][key] === null) {
                            monthlyData[month][key] = 0; // Initialize sum if first valid value
                        }
                        monthlyData[month][key] += value;
                    }
                });
                monthlyData[month]['count']++;
            }
        }
    });

    // Convert sums to averages (for Blank Sailing, it's sum/average depending on interpretation, let's stick to sum as per previous)
    // The user requirements for Blank Sailing stated "월별 집계된 평균값 (또는 합계)을 누적 막대 차트 형태로 표시"
    // Let's assume sum for now, as it's a "total" for alliance blank sailings.
    const aggregatedArray = Object.keys(monthlyData).map(month => {
        const result = { month: month };
        valueKeys.forEach(key => {
            result[key] = monthlyData[month][key]; // Use sum directly
        });
        return result;
    });

    // Sort by month
    aggregatedArray.sort((a, b) => dayjs(a.month).diff(dayjs(b.month)));

    return aggregatedArray;
}

// Function to populate table data
function populateTableData(sectionKey, tableData) {
    const tableElement = document.getElementById(SECTION_MAPPINGS_FRONTEND[sectionKey].tableId);
    if (!tableElement) {
        console.warn(`Table element with ID '${SECTION_MAPPINGS_FRONTEND[sectionKey].tableId}' not found.`);
        return;
    }

    const tbody = tableElement.querySelector('tbody');
    if (!tbody) {
        console.error(`Table body not found for ${sectionKey} table.`);
        return;
    }
    tbody.innerHTML = ''; // Clear existing rows

    // Update table headers with dates
    const currentHeader = document.getElementById(SECTION_MAPPINGS_FRONTEND[sectionKey].tableCurrentHeaderId);
    const previousHeader = document.getElementById(SECTION_MAPPINGS_FRONTEND[sectionKey].tablePreviousHeaderId);

    if (currentHeader && tableData.rows.length > 0 && tableData.rows[0].current_index !== null) {
        const currentDate = tableData.rows[0].route_date_current; // Assuming route_date_current is available
        currentHeader.textContent = `Current Index (${dayjs(currentDate).format('MM/DD/YYYY')})`;
    } else if (currentHeader) {
        currentHeader.textContent = `Current Index`;
    }

    if (previousHeader && tableData.rows.length > 0 && tableData.rows[0].previous_index !== null) {
        const previousDate = tableData.rows[0].route_date_previous; // Assuming route_date_previous is available
        previousHeader.textContent = `Previous Index (${dayjs(previousDate).format('MM/DD/YYYY')})`;
    } else if (previousHeader) {
        previousHeader.textContent = `Previous Index`;
    }

    tableData.rows.forEach(rowData => {
        const row = tbody.insertRow();
        row.insertCell().textContent = rowData.route;
        row.insertCell().textContent = rowData.current_index !== null ? rowData.current_index.toLocaleString() : '-';
        row.insertCell().textContent = rowData.previous_index !== null ? rowData.previous_index.toLocaleString() : '-';

        const weeklyChangeCell = row.insertCell();
        if (rowData.weekly_change) {
            const changeSpan = document.createElement('span');
            changeSpan.classList.add(rowData.weekly_change.color_class);
            changeSpan.textContent = `${rowData.weekly_change.value} (${rowData.weekly_change.percentage})`;
            weeklyChangeCell.appendChild(changeSpan);
        } else {
            weeklyChangeCell.textContent = '-';
        }
    });
}


// --- Main Data Loading and Display Function ---
async function loadAndDisplayData() {
    try {
        const response = await fetch('data/crawling_data.json');
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        dashboardData = await response.json();
        console.log("Loaded all dashboard data:", dashboardData);

        // --- Top Info Carousel ---

        // World Time
        const worldTimeDisplay = document.getElementById('worldTimeDisplay');
        const timezones = {
            "로스앤젤레스": "America/Los_Angeles",
            "뉴욕": "America/New_York",
            "파리": "Europe/Paris",
            "상하이": "Asia/Shanghai",
            "서울": "Asia/Seoul",
            "시드니": "Australia/Sydney"
        };

        function updateWorldTimes() {
            worldTimeDisplay.innerHTML = '';
            for (const city in timezones) {
                const time = dayjs().tz(timezones[city]).format('HH:mm:ss');
                const div = document.createElement('div');
                div.className = 'flex flex-col items-center';
                div.innerHTML = `<span class="font-semibold">${city}</span><span class="text-xl">${time}</span>`;
                worldTimeDisplay.appendChild(div);
            }
        }
        updateWorldTimes();
        setInterval(updateWorldTimes, 1000); // Update every second

        // LA Weather
        const weather = dashboardData.weather_data.current;
        document.getElementById('laTemperature').textContent = weather.LA_Temperature !== null ? `${weather.LA_Temperature}°F` : '-';
        document.getElementById('laWeatherStatus').textContent = weather.LA_WeatherStatus || '-';
        document.getElementById('laHumidity').textContent = weather.LA_Humidity !== null ? weather.LA_Humidity : '-';
        document.getElementById('laWindSpeed').textContent = weather.LA_WindSpeed !== null ? weather.LA_WindSpeed : '-';
        document.getElementById('laPressure').textContent = weather.LA_Pressure !== null ? weather.LA_Pressure : '-';
        document.getElementById('laVisibility').textContent = weather.LA_Visibility !== null ? weather.LA_Visibility : '-';
        document.getElementById('laSunrise').textContent = weather.LA_Sunrise || '-';
        document.getElementById('laSunset').textContent = weather.LA_Sunset || '-';

        // Simple icon mapping (you might want a more robust solution)
        const weatherIconElement = document.getElementById('weatherIcon');
        if (weather.LA_WeatherStatus) {
            if (weather.LA_WeatherStatus.includes('맑음')) {
                weatherIconElement.innerHTML = '☀️';
            } else if (weather.LA_WeatherStatus.includes('흐림') || weather.LA_WeatherStatus.includes('구름')) {
                weatherIconElement.innerHTML = '☁️';
            } else if (weather.LA_WeatherStatus.includes('비')) {
                weatherIconElement.innerHTML = '🌧️';
            } else if (weather.LA_WeatherStatus.includes('눈')) {
                weatherIconElement.innerHTML = '❄️';
            } else {
                weatherIconElement.innerHTML = '❓'; // Default unknown
            }
        } else {
            weatherIconElement.innerHTML = '❓';
        }

        // Forecast Weather Table
        const forecastTableBody = document.getElementById('forecastTableBody');
        forecastTableBody.innerHTML = '';
        dashboardData.weather_data.forecast.forEach(day => {
            const row = forecastTableBody.insertRow();
            row.insertCell().textContent = day.date || '-';
            row.insertCell().textContent = day.min_temp !== null ? day.min_temp : '-';
            row.insertCell().textContent = day.max_temp !== null ? day.max_temp : '-';
            row.insertCell().textContent = day.status || '-';
        });

        // USD-KRW Exchange Rate
        const exchangeRates = dashboardData.exchange_rates;
        if (exchangeRates && exchangeRates.length > 0) {
            document.getElementById('currentExchangeRate').textContent = exchangeRates[exchangeRates.length - 1].rate.toLocaleString();

            // Filter for last 1 month (approx 30 days) for exchange rate chart
            const oneMonthAgo = dayjs().subtract(1, 'month').startOf('day');
            const filteredExchangeRates = exchangeRates.filter(d => dayjs(d.date).isAfter(oneMonthAgo) || dayjs(d.date).isSame(oneMonthAgo, 'day'));

            console.log("Exchange Rate Chart Datasets (before setup):", filteredExchangeRates);
            console.log("Exchange Rate Chart Data Sample (first 5 points):", filteredExchangeRates.slice(0, 5));

            const exchangeRateCtx = document.getElementById('exchangeRateChart').getContext('2d');
            new Chart(exchangeRateCtx, {
                type: 'line',
                data: {
                    labels: filteredExchangeRates.map(d => d.date),
                    datasets: [{
                        label: 'USD-KRW 환율',
                        data: filteredExchangeRates.map(d => ({ x: d.date, y: d.rate })),
                        borderColor: '#4299E1',
                        tension: 0.1,
                        fill: false,
                        pointRadius: 0 // Remove data points
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        x: {
                            type: 'time',
                            time: {
                                unit: 'month', // Changed to month for X-axis labels
                                displayFormats: {
                                    month: 'MM/01/yyyy' // User requested format
                                },
                                tooltipFormat: 'M/d/yyyy' // Tooltip format
                            },
                            ticks: {
                                autoSkip: true,
                                maxRotation: 0,
                                minRotation: 0,
                                source: 'auto',
                                color: '#666'
                            },
                            grid: {
                                display: false // Remove X-axis grid lines
                            }
                        },
                        y: {
                            ticks: {
                                maxTicksLimit: 5, // Limit to 5 ticks
                                color: '#666'
                            },
                            grid: {
                                display: true, // Display Y-axis grid lines
                                drawOnChartArea: true, // Draw on chart area
                                drawTicks: false, // Do not draw ticks on grid lines
                                color: 'rgba(200, 200, 200, 0.2)', // Light grey color
                                z: -1 // Draw behind the graph
                            }
                        }
                    },
                    plugins: {
                        legend: {
                            display: false // No legend for single line chart
                        },
                        tooltip: {
                            callbacks: {
                                title: function(context) {
                                    return dayjs(context[0].parsed.x).format('M/D/YYYY'); // Tooltip date format
                                }
                            }
                        }
                    },
                    animation: {
                        duration: 0 // Disable animation for faster rendering
                    }
                }
            });
        } else {
            console.warn("No exchange rate data available.");
        }

        // --- Main Charts Carousel ---
        for (const sectionKey in SECTION_MAPPINGS_FRONTEND) {
            const chartData = dashboardData.chart_data[sectionKey];
            const tableData = dashboardData.table_data[sectionKey];
            const mapping = SECTION_MAPPINGS_FRONTEND[sectionKey];

            if (chartData) {
                if (sectionKey === "BLANK_SAILING") {
                    createBarChart(mapping.chartId, chartData, mapping.data_cols_map);
                } else {
                    createLineChart(mapping.chartId, chartData, mapping.data_cols_map);
                }
            } else {
                console.warn(`No chart data found for section: ${sectionKey}`);
            }

            if (tableData) {
                // Enhance tableData with current/previous dates from the first row of tableData.rows
                if (tableData.rows.length > 0) {
                    const firstRow = dashboardData.table_data[sectionKey].rows[0];
                    // Find the current and previous dates from the table data itself
                    // Assuming the table data's 'route' is the first column, and the dates are in the header cells
                    // We need to fetch the dates from the original Google Sheet data if they are not in tableData.rows
                    // For simplicity, let's assume the tableData.rows[0] has the dates from the python script
                    // which it does not. The Python script provides the dates in `TABLE_DATA_CELL_MAPPINGS`.
                    // So, we need to pass those dates from the Python script to the frontend.
                    // For now, I'll use a placeholder or try to infer from the chart data if possible.

                    // Re-fetching dates for table headers from the original `Crawling_Data2`
                    const tableMapping = TABLE_DATA_CELL_MAPPINGS[sectionKey];
                    let currentHeaderDate = '';
                    let previousHeaderDate = '';

                    if (dashboardData.table_data[sectionKey].current_date_cell_value) {
                        currentHeaderDate = dashboardData.table_data[sectionKey].current_date_cell_value;
                    } else {
                        // Fallback: try to get the date from the first chart data point's date
                        if (chartData && chartData.length > 0) {
                            currentHeaderDate = chartData[chartData.length - 1].date; // Latest date from chart data
                        }
                    }

                    if (dashboardData.table_data[sectionKey].previous_date_cell_value) {
                        previousHeaderDate = dashboardData.table_data[sectionKey].previous_date_cell_value;
                    } else {
                        // Fallback: try to get the date from the second to last chart data point's date
                        if (chartData && chartData.length > 1) {
                            previousHeaderDate = chartData[chartData.length - 2].date; // Second latest date from chart data
                        }
                    }

                    // Update table headers
                    const currentHeaderElement = document.getElementById(mapping.tableCurrentHeaderId);
                    const previousHeaderElement = document.getElementById(mapping.tablePreviousHeaderId);

                    if (currentHeaderElement) {
                        currentHeaderElement.textContent = `Current Index (${dayjs(currentHeaderDate).format('MM/DD/YYYY')})`;
                    }
                    if (previousHeaderElement) {
                        previousHeaderElement.textContent = `Previous Index (${dayjs(previousHeaderDate).format('MM/DD/YYYY')})`;
                    }
                }

                populateTableData(sectionKey, tableData);
            } else {
                console.warn(`No table data found for section: ${sectionKey}`);
            }
        }

        // Initialize carousels
        topInfoCarouselInterval = setupCarousel('topInfoCarouselInner', 'topInfoCarouselDots', 10000);
        mainChartsCarouselInterval = setupCarousel('mainChartsCarouselInner', 'mainChartsCarouselDots', 10000);

    } catch (error) {
        console.error("Failed to load or display dashboard data:", error);
    }
}

// --- World Timezone Setup (using dayjs timezone plugin) ---
// Load dayjs timezone plugin
// Need to load the timezone data. For simplicity, will use a small subset or rely on system defaults.
// For full timezone support, you'd typically load 'dayjs/plugin/timezone' and 'dayjs/plugin/utc'
// and then `dayjs.tz.setDefault('America/Los_Angeles');` or load specific data.
// For this example, let's assume dayjs can infer basic timezones or use a simple UTC offset.
// If more robust timezone handling is needed, `dayjs.extend(window.dayjs_plugin_utc); dayjs.extend(window.dayjs_plugin_timezone);`
// and then `dayjs.tz.add(timezoneData);` would be required.
// For now, I'll rely on browser's Intl.DateTimeFormat which Day.js uses.

// Call the main function when the DOM is fully loaded
document.addEventListener('DOMContentLoaded', loadAndDisplayData);

// Clean up intervals on page unload
window.addEventListener('beforeunload', () => {
    clearInterval(topInfoCarouselInterval);
    clearInterval(mainChartsCarouselInterval);
});
