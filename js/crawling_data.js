import { setupChart, getNextColor, getNextBorderColor, resetColorIndex } from './chart_utilities.js';
import { aggregateDataByMonth } from './data_aggregation.js';
import { renderTable } from './table_renderer.js';

// Chart instances (managed here, but can be passed from main if needed for global access)
let KCCIChart;
let SCFIChart;
let WCIChart;
let IACIChart;
let blankSailingChart;
let FBXChart;
let XSIChart;
let MBCIChart;

// --- Mapping between table route names and chart data keys ---
// The keys here MUST match the exact 'route_names' from TABLE_DATA_CELL_MAPPINGS in Python
// (i.e., the part after the section prefix, e.g., "종합지수" for KCCI_종합지수)
// The values here MUST match the final JSON keys from SECTION_COLUMN_MAPPINGS in Python
// (i.e., the fully prefixed IndexName_RouteName)
const routeToDataKeyMap = {
    KCCI: {
        "종합지수": "KCCI_종합지수",
        "미주서안": "KCCI_미주서안",
        "미주동안": "KCCI_미주동안",
        "유럽": "KCCI_유럽",
        "지중해": "KCCI_지중해",
        "중동": "KCCI_중동",
        "호주": "KCCI_호주",
        "남미동안": "KCCI_남미동안",
        "남미서안": "KCCI_남미서안",
        "남아프리카": "KCCI_남아프리카",
        "서아프리카": "KCCI_서아프리카",
        "중국": "KCCI_중국",
        "일본": "KCCI_일본",
        "동남아시아": "KCCI_동남아시아"
    },
    SCFI: {
        "종합지수": "SCFI_종합지수",
        "유럽 (기본항)": "SCFI_유럽 (기본항)",
        "지중해 (기본항)": "SCFI_지중해 (기본항)",
        "미주서안 (기본항)": "SCFI_미주서안 (기본항)",
        "미주동안 (기본항)": "SCFI_미주동안 (기본항)",
        "페르시아만/홍해 (두바이)": "SCFI_페르시아만/홍해 (두바이)",
        "호주/뉴질랜드 (멜버른)": "SCFI_호주/뉴질랜드 (멜버른)",
        "동/서 아프리카 (라고스)": "SCFI_동/서 아프리카 (라고스)",
        "남아프리카 (더반)": "SCFI_남아프리카 (더반)",
        "서일본 (기본항)": "SCFI_서일본 (기본항)",
        "동일본 (기본항)": "SCFI_동일본 (기본항)",
        "동남아시아 (싱가포르)": "SCFI_동남아시아 (싱가포르)",
        "한국 (부산)": "SCFI_한국 (부산)",
        "중남미서안 (만사니요)": "SCFI_중남미서안 (만사니요)"
    },
    WCI: {
        "종합지수": "WCI_종합지수",
        "상하이 → 로테르담": "WCI_상하이 → 로테르담",
        "로테르담 → 상하이": "WCI_로테르담 → 상하이",
        "상하이 → 제노바": "WCI_상하이 → 제노바",
        "상하이 → 로스엔젤레스": "WCI_상하이 → 로스엔젤레스",
        "로스엔젤레스 → 상하이": "WCI_로스엔젤레스 → 상하이",
        "상하이 → 뉴욕": "WCI_상하이 → 뉴욕",
        "뉴욕 → 로테르담": "WCI_뉴욕 → 로테르담",
        "로테르담 → 뉴욕": "WCI_로테르담 → 뉴욕",
    },
    IACI: {
        "종합지수": "IACI_종합지수"
    },
    BLANK_SAILING: {
        // 파이썬의 data_cols_map 값과 정확히 일치하도록 수정
        "Gemini Cooperation": "BLANK_SAILING_Gemini_Cooperation",
        "MSC": "BLANK_SAILING_MSC",
        "OCEAN Alliance": "BLANK_SAILING_OCEAN_Alliance",
        "Premier Alliance": "BLANK_SAILING_Premier_Alliance",
        "Others/Independent": "BLANK_SAILING_Others_Independent",
        "Total": "BLANK_SAILING_종합지수" // 파이썬에서 Total을 종합지수로 매핑
    },
    FBX: {
        // 파이썬의 data_cols_map 값과 정확히 일치하도록 수정
        "종합지수": "FBX_종합지수", // "글로벌 컨테이너 운임 지수" 대신 "종합지수" 사용
        "중국/동아시아 → 미주서안": "FBX_중국/동아시아 → 미주서안",
        "미주서안 → 중국/동아시아": "FBX_미주서안 → 중국/동아시아",
        "중국/동아시아 → 미주동안": "FBX_중국/동아시아 → 미주동안",
        "미주동안 → 중국/동아시아": "FBX_미주동안 → 중국/동아시아",
        "중국/동아시아 → 북유럽": "FBX_중국/동아시아 → 북유럽",
        "북유럽 → 중국/동아시아": "FBX_북유럽 → 중국/동아시아",
        "중국/동아시아 → 지중해": "FBX_중국/동아시아 → 지중해",
        "지중해 → 중국/동아시아": "FBX_지중해 → 중국/동아시아",
        "미주동안 → 북유럽": "FBX_미주동안 → 북유럽",
        "북유럽 → 미주동안": "FBX_북유럽 → 미주동안",
        "유럽 → 남미동안": "FBX_유럽 → 남미동안",
        "유럽 → 남미서안": "FBX_유럽 → 남미서안"
    },
    XSI: {
        "동아시아 → 북유럽": "XSI_동아시아 → 북유럽",
        "북유럽 → 동아시아": "XSI_북유럽 → 동아시아",
        "동아시아 → 미주서안": "XSI_동아시아 → 미주서안",
        "미주서안 → 동아시아": "XSI_미주서안 → 동아시아",
        "동아시아 → 남미동안": "XSI_동아시아 → 남미동안",
        "북유럽 → 미주동안": "XSI_북유럽 → 미주동안",
        "미주동안 → 북유럽": "XSI_미주동안 → 북유럽",
        "북유럽 → 남미동안": "XSI_북유럽 → 남미동안"
    },
    MBCI: {
        "MBCI": "MBCI_MBCI" // 파이썬에서 MBCI를 MBCI_MBCI로 매핑
    }
};

// --- Helper function to create datasets based on table rows and chart data ---
const createDatasetsFromTableRows = (indexType, chartData, tableRows) => {
    const datasets = [];
    const mapping = routeToDataKeyMap[indexType];
    if (!mapping) {
        console.warn(`No data key mapping found for index type: ${indexType}`);
        return datasets;
    }

    tableRows.forEach(row => {
        // row.route 값은 이미 "KCCI_종합지수"와 같이 섹션_라우트명 형태입니다.
        // mapping 객체의 키는 "종합지수"와 같이 라우트명만 필요합니다.
        // 따라서, 섹션 접두사를 제거해야 합니다.
        const originalRouteNameFromTable = row.route.split('_').slice(1).join('_');

        // 매핑에서 실제 데이터 키를 찾습니다.
        const dataKey = mapping[originalRouteNameFromTable];
        
        // Only create a dataset if a corresponding data key exists and is not null
        // and the current_index from the table is not empty (meaning there's data for this route)
        if (dataKey !== null && dataKey !== undefined && row.current_index !== "") { 
            const mappedData = chartData.map(item => {
                const xVal = item.date; // This should be a string like 'YYYY-MM-DD'
                const yVal = item[dataKey];
                return { x: xVal, y: yVal };
            });

            // Filter out data points where y is null or undefined
            const filteredMappedData = mappedData.filter(point => point.y !== null && point.y !== undefined);

            // Only add dataset if there's actual data after filtering
            if (filteredMappedData.length > 0) {
                datasets.push({
                    label: originalRouteNameFromTable, // Use the original route name for the legend
                    data: filteredMappedData,
                    backgroundColor: getNextColor(),
                    borderColor: getNextBorderColor(),
                    borderWidth: (originalRouteNameFromTable.includes('종합지수') || originalRouteNameFromTable.includes('글로벌 컨테이너 운임 지수') || originalRouteNameFromTable.includes('US$/40ft') || originalRouteNameFromTable.includes('MBCI') || originalRouteNameFromTable.includes('Total')) ? 2 : 1, // Make composite index lines thicker
                    fill: false
                });
            } else {
                console.warn(`WARNING: No valid data points found for ${indexType} - route: '${originalRouteNameFromTable}' (dataKey: '${dataKey}'). Skipping dataset.`);
            }
        } else if (dataKey === null) {
            console.info(`INFO: Skipping chart dataset for route '${row.route}' in ${indexType} as it's explicitly mapped to null (no chart data expected).`);
        } else {
            console.warn(`WARNING: No dataKey found or current_index is empty for ${indexType} - route: '${row.route}'. Skipping dataset.`);
        }
    });
    return datasets;
};

export async function initializeCrawlingDataChartsAndTables(dataJsonUrl) {
    try {
        const response = await fetch(dataJsonUrl);
        if (!response.ok) {
            throw new Error(`HTTP error! Status: ${response.status} for ${dataJsonUrl}`);
        }
        const allDashboardData = await response.json();
        console.log("Loaded crawling data for charts and tables:", allDashboardData);

        const chartDataBySection = allDashboardData.chart_data || {};
        const tableDataBySection = allDashboardData.table_data || {};

        if (Object.keys(chartDataBySection).length === 0) {
            console.warn("No chart data sections found in the JSON file.");
            document.querySelector('.chart-slider-container').innerHTML = '<p class="placeholder-text">No chart data available.</p>';
        }

        // --- Prepare Chart Data and Initialize Charts ---
        resetColorIndex(); // Reset color index for each chart initialization

        // Chart 1: KCCI - All relevant indices (Granular Data)
        const KCCIData = chartDataBySection.KCCI || [];
        KCCIData.sort((a, b) => new Date(a.date) - new Date(b.date)); // Sort specific section data
        const KCCITableRows = tableDataBySection.KCCI ? tableDataBySection.KCCI.rows : [];
        const KCCIDatasets = createDatasetsFromTableRows('KCCI', KCCIData, KCCITableRows);
        KCCIChart = setupChart('KCCIChart', 'line', KCCIDatasets, {}, false);
        renderTable('KCCITableContainer', tableDataBySection.KCCI.headers, KCCITableRows);


        // Chart 2: SCFI - All relevant indices (Granular Data)
        resetColorIndex(); // Reset color index for each chart
        const SCFIData = chartDataBySection.SCFI || [];
        SCFIData.sort((a, b) => new Date(a.date) - new Date(b.date));
        const SCFITableRows = tableDataBySection.SCFI ? tableDataBySection.SCFI.rows : [];
        const SCFIDatasets = createDatasetsFromTableRows('SCFI', SCFIData, SCFITableRows);
        SCFIChart = setupChart('SCFIChart', 'line', SCFIDatasets, {}, false);
        renderTable('SCFITableContainer', tableDataBySection.SCFI.headers, SCFITableRows);


        // Chart 3: WCI - All relevant indices (Granular Data)
        resetColorIndex(); // Reset color index for each chart
        const WCIData = chartDataBySection.WCI || [];
        WCIData.sort((a, b) => new Date(a.date) - new Date(b.date));
        const WCITableRows = tableDataBySection.WCI ? tableDataBySection.WCI.rows : [];
        const WCIDatasets = createDatasetsFromTableRows('WCI', WCIData, WCITableRows);
        WCIChart = setupChart('WCIChart', 'line', WCIDatasets, {}, false);
        renderTable('WCITableContainer', tableDataBySection.WCI.headers, WCITableRows);


        // Chart 4: IACI Composite Index (Granular Data)
        resetColorIndex(); // Reset color index for each chart
        const IACIData = chartDataBySection.IACI || [];
        IACIData.sort((a, b) => new Date(a.date) - new Date(b.date));
        const IACITableRows = tableDataBySection.IACI ? tableDataBySection.IACI.rows : [];
        const IACIDatasets = createDatasetsFromTableRows('IACI', IACIData, IACITableRows);
        IACIChart = setupChart('IACIChart', 'line', IACIDatasets, {}, false);
        renderTable('IACITableContainer', tableDataBySection.IACI.headers, IACITableRows);


        // Chart 5: BLANK_SAILING Stacked Bar Chart (Keep Aggregated)
        const blankSailingRawData = chartDataBySection.BLANK_SAILING || [];
        const { aggregatedData: aggregatedBlankSailingData, monthlyLabels: blankSailingChartDates } = aggregateDataByMonth(blankSailingRawData, 12);
        
        // Blank Sailing datasets are still manually defined as they are stacked bar
        resetColorIndex(); // Reset color index for each chart
        const blankSailingDatasets = [
            // BLANK_SAILING_제미니_협력 -> BLANK_SAILING_Gemini_Cooperation
            { label: 'Gemini Cooperation', data: aggregatedBlankSailingData.map(item => ({ x: item.date, y: item.BLANK_SAILING_Gemini_Cooperation })), backgroundColor: getNextColor(), borderColor: getNextBorderColor(), borderWidth: 1 },
            { label: 'MSC', data: aggregatedBlankSailingData.map(item => ({ x: item.date, y: item.BLANK_SAILING_MSC })), backgroundColor: getNextColor(), borderColor: getNextBorderColor(), borderWidth: 1 },
            // BLANK_SAILING_오션_얼라이언스 -> BLANK_SAILING_OCEAN_Alliance
            { label: 'OCEAN Alliance', data: aggregatedBlankSailingData.map(item => ({ x: item.date, y: item.BLANK_SAILING_OCEAN_Alliance })), backgroundColor: getNextColor(), borderColor: getNextBorderColor(), borderWidth: 1 },
            // BLANK_SAILING_프리미어_얼라이언스 -> BLANK_SAILING_Premier_Alliance
            { label: 'Premier Alliance', data: aggregatedBlankSailingData.map(item => ({ x: item.date, y: item.BLANK_SAILING_Premier_Alliance })), backgroundColor: getNextColor(), borderColor: getNextBorderColor(), borderWidth: 1 },
            // BLANK_SAILING_기타_독립 -> BLANK_SAILING_Others_Independent
            { label: 'Others/Independent', data: aggregatedBlankSailingData.map(item => ({ x: item.date, y: item.BLANK_SAILING_Others_Independent })), backgroundColor: getNextColor(), borderColor: getNextBorderColor(), borderWidth: 1 },
            // BLANK_SAILING_총계 -> BLANK_SAILING_종합지수
            { label: 'Total', data: aggregatedBlankSailingData.map(item => ({ x: item.date, y: item.BLANK_SAILING_종합지수 })), backgroundColor: getNextColor(), borderColor: getNextBorderColor(), borderWidth: 1 }
        ].filter(dataset => dataset.data.some(point => point.y !== null && point.y !== undefined)); // Filter out datasets with no valid data

        blankSailingChart = setupChart(
            'blankSailingChart', 'bar',
            blankSailingDatasets,
            {
                scales: {
                    x: {
                        stacked: true,
                        type: 'time', // Use time scale for aggregated data
                        time: {
                            unit: 'month',
                            displayFormats: { month: 'MMM \'yy' },
                            tooltipFormat: 'M/d/yyyy'
                        },
                        ticks: { maxTicksLimit: 12, autoSkipPadding: 10 }
                    },
                    y: {
                        stacked: true,
                        beginAtZero: true,
                        title: { display: true, text: 'Blank Sailings' },
                        ticks: { count: 5 }
                    }
                },
                plugins: {
                    tooltip: { mode: 'index', intersect: false }
                }
            },
            true // This chart is aggregated by month
        );
        renderTable('blankSailingTableContainer', tableDataBySection.BLANK_SAILING.headers, tableDataBySection.BLANK_SAILING.rows);


        // Chart 6: FBX - All relevant indices (Granular Data)
        resetColorIndex(); // Reset color index for each chart
        const FBXData = chartDataBySection.FBX || [];
        FBXData.sort((a, b) => new Date(a.date) - new Date(b.date));
        const FBXTableRows = tableDataBySection.FBX ? tableDataBySection.FBX.rows : [];
        const FBXDatasets = createDatasetsFromTableRows('FBX', FBXData, FBXTableRows);
        FBXChart = setupChart('FBXChart', 'line', FBXDatasets, {}, false);
        renderTable('FBXTableContainer', tableDataBySection.FBX.headers, FBXTableRows);


        // Chart 7: XSI - All relevant indices (Granular Data)
        resetColorIndex(); // Reset color index for each chart
        const XSIData = chartDataBySection.XSI || [];
        XSIData.sort((a, b) => new Date(a.date) - new Date(b.date));
        const XSITableRows = tableDataBySection.XSI ? tableDataBySection.XSI.rows : [];
        const XSIDatasets = createDatasetsFromTableRows('XSI', XSIData, XSITableRows);
        XSIChart = setupChart('XSIChart', 'line', XSIDatasets, {}, false);
        renderTable('XSITableContainer', tableDataBySection.XSI.headers, XSITableRows);

        // Chart 8: MBCI - Composite Index only (Granular Data)
        resetColorIndex(); // Reset color index for each chart
        const MBCIData = chartDataBySection.MBCI || [];
        MBCIData.sort((a, b) => new Date(a.date) - new Date(b.date));
        const MBCITableRows = tableDataBySection.MBCI ? tableDataBySection.MBCI.rows : [];
        const MBCIDatasets = createDatasetsFromTableRows('MBCI', MBCIData, MBCITableRows);
        MBCIChart = setupChart('MBCIChart', 'line', MBCIDatasets, {}, false);
        renderTable('MBCITableContainer', tableDataBySection.MBCI.headers, MBCITableRows);

    } catch (error) {
        console.error("Error loading or processing crawling data:", error);
        document.querySelector('.chart-slider-container').innerHTML = '<p class="placeholder-text text-red-500">Failed to load chart data. Please check the data source.</p>';
        document.querySelectorAll('.table-container').forEach(container => {
            container.innerHTML = '<p class="text-red-500 text-center">Failed to load table data.</p>';
        });
    }
}
