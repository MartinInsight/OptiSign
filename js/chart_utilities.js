// js/chart_utilities.js

// Chart.js 인스턴스를 관리하고 차트 색상을 순환하는 유틸리티 함수입니다.

// 차트 데이터셋에 사용할 색상 팔레트입니다.
const CHART_COLORS = [
    'rgb(255, 99, 132)', // Red
    'rgb(54, 162, 235)', // Blue
    'rgb(255, 205, 86)', // Yellow
    'rgb(75, 192, 192)', // Green
    'rgb(153, 102, 255)', // Purple
    'rgb(255, 159, 64)', // Orange
    'rgb(201, 203, 207)', // Grey
    'rgb(255, 99, 255)', // Pink
    'rgb(99, 255, 132)', // Light Green
    'rgb(132, 99, 255)', // Dark Purple
    'rgb(255, 132, 99)', // Light Orange
    'rgb(99, 132, 255)'  // Indigo
];

// 차트 테두리 색상 팔레트입니다.
const CHART_BORDER_COLORS = [
    'rgb(255, 99, 132)',
    'rgb(54, 162, 235)',
    'rgb(255, 205, 86)',
    'rgb(75, 192, 192)',
    'rgb(153, 102, 255)',
    'rgb(255, 159, 64)',
    'rgb(201, 203, 207)',
    'rgb(255, 99, 255)',
    'rgb(99, 255, 132)',
    'rgb(132, 99, 255)',
    'rgb(255, 132, 99)',
    'rgb(99, 132, 255)'
];

let colorIndex = 0; // 현재 색상 인덱스

/**
 * 다음 배경 색상을 반환하고 인덱스를 증가시킵니다.
 * @returns {string} 다음 배경 색상 (RGB 문자열)
 */
export function getNextColor() {
    const color = CHART_COLORS[colorIndex % CHART_COLORS.length];
    colorIndex++;
    return color;
}

/**
 * 다음 테두리 색상을 반환하고 인덱스를 증가시킵니다.
 * @returns {string} 다음 테두리 색상 (RGB 문자열)
 */
export function getNextBorderColor() {
    // 테두리 색상도 배경 색상과 동일한 순서로 순환하도록 별도의 인덱스를 사용하거나,
    // 배경 색상 인덱스를 재활용할 수 있습니다. 여기서는 배경 색상과 동일하게 순환합니다.
    return CHART_BORDER_COLORS[(colorIndex - 1) % CHART_BORDER_COLORS.length];
}

/**
 * 색상 인덱스를 0으로 재설정합니다.
 * 새로운 차트를 그릴 때 색상 순환을 처음부터 시작하고 싶을 때 사용합니다.
 */
export function resetColorIndex() {
    colorIndex = 0;
}

/**
 * Chart.js 차트를 설정하고 초기화합니다.
 * @param {string} canvasId - 차트를 그릴 캔버스 요소의 ID.
 * @param {string} type - 차트 유형 ('line', 'bar' 등).
 * @param {Array<Object>} datasets - 차트 데이터셋 배열.
 * @param {Object} options - Chart.js 옵션 객체.
 * @param {boolean} isAggregatedByMonth - 데이터가 월별로 집계되었는지 여부 (x축 타입 설정에 사용).
 * @returns {Chart} 초기화된 Chart.js 인스턴스.
 */
export function setupChart(canvasId, type, datasets, options = {}, isAggregatedByMonth = false) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) {
        console.error(`Canvas element with ID '${canvasId}' not found.`);
        return null;
    }

    // 기존 차트 인스턴스가 있다면 파괴하여 메모리 누수를 방지합니다.
    if (Chart.getChart(canvasId)) {
        Chart.getChart(canvasId).destroy();
    }

    const defaultOptions = {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: {
                display: true,
                position: 'top',
                labels: {
                    font: {
                        family: 'Inter',
                        size: 10 // 범례 텍스트 크기 조정
                    }
                }
            },
            tooltip: {
                mode: 'index',
                intersect: false,
                callbacks: {
                    label: function(context) {
                        let label = context.dataset.label || '';
                        if (label) {
                            label += ': ';
                        }
                        if (context.parsed.y !== null) {
                            label += new Intl.NumberFormat('en-US').format(context.parsed.y);
                        }
                        return label;
                    }
                }
            }
        },
        scales: {
            x: {
                type: 'time', // 기본적으로 시간 축 사용
                time: {
                    unit: isAggregatedByMonth ? 'month' : 'day', // 월별 집계 여부에 따라 단위 설정
                    tooltipFormat: 'yyyy-MM-dd',
                    displayFormats: {
                        day: 'MMM dd',
                        month: 'MMM \'yy',
                        year: 'yyyy'
                    }
                },
                ticks: {
                    autoSkip: true,
                    maxTicksLimit: 10, // 표시될 최대 눈금 수
                    font: {
                        family: 'Inter',
                        size: 10
                    }
                },
                grid: {
                    display: false // x축 그리드 라인 숨기기
                }
            },
            y: {
                beginAtZero: true,
                ticks: {
                    callback: function(value) {
                        return new Intl.NumberFormat('en-US').format(value);
                    },
                    font: {
                        family: 'Inter',
                        size: 10
                    }
                },
                grid: {
                    color: 'rgba(0, 0, 0, 0.05)' // y축 그리드 라인 색상
                }
            }
        }
    };

    // 사용자 정의 옵션을 기본 옵션과 병합합니다.
    const mergedOptions = Chart.helpers.merge(defaultOptions, options);

    const newChart = new Chart(ctx, {
        type: type,
        data: {
            datasets: datasets
        },
        options: mergedOptions
    });

    return newChart;
}
