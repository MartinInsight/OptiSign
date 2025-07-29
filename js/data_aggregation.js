// js/data_aggregation.js

// 데이터를 월별로 집계하는 유틸리티 함수입니다.
// 주로 Blank Sailing 차트와 같이 월별 합계가 필요한 경우에 사용됩니다.

/**
 * 주어진 데이터를 월별로 집계합니다.
 * 각 월의 마지막 날짜를 기준으로 데이터를 그룹화하고, 해당 월의 모든 숫자 값을 합산합니다.
 *
 * @param {Array<Object>} rawData - 원본 데이터 배열. 각 객체는 'date' 속성(YYYY-MM-DD 형식)과 숫자 값을 포함해야 합니다.
 * @param {number} monthsToDisplay - 표시할 월의 최대 개수 (최근 월부터).
 * @returns {{aggregatedData: Array<Object>, monthlyLabels: Array<string>}} 집계된 데이터와 월별 레이블 배열.
 */
export function aggregateDataByMonth(rawData, monthsToDisplay = 12) {
    if (!rawData || rawData.length === 0) {
        return { aggregatedData: [], monthlyLabels: [] };
    }

    // 날짜를 기준으로 데이터를 정렬합니다.
    rawData.sort((a, b) => new Date(a.date) - new Date(b.date));

    const monthlyDataMap = new Map();

    rawData.forEach(item => {
        const date = new Date(item.date);
        // 월의 시작일 (YYYY-MM-01)을 키로 사용합니다.
        const monthKey = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-01`;

        if (!monthlyDataMap.has(monthKey)) {
            // 새 월의 데이터를 초기화합니다.
            monthlyDataMap.set(monthKey, { date: monthKey });
            // 모든 숫자 필드를 0으로 초기화합니다.
            for (const key in item) {
                if (key !== 'date' && typeof item[key] === 'number') {
                    monthlyDataMap.get(monthKey)[key] = 0;
                }
            }
        }

        const currentMonthData = monthlyDataMap.get(monthKey);
        for (const key in item) {
            if (key !== 'date') {
                const value = parseFloat(item[key]);
                if (!isNaN(value)) {
                    // 해당 필드가 이미 맵에 숫자로 존재하면 더하고, 아니면 초기화합니다.
                    currentMonthData[key] = (currentMonthData[key] || 0) + value;
                }
            }
        }
    });

    // Map의 값을 배열로 변환하고 날짜를 기준으로 정렬합니다.
    let aggregatedData = Array.from(monthlyDataMap.values()).sort((a, b) => new Date(a.date) - new Date(b.date));

    // 최근 monthsToDisplay 개월의 데이터만 선택합니다.
    if (aggregatedData.length > monthsToDisplay) {
        aggregatedData = aggregatedData.slice(-monthsToDisplay);
    }

    // 차트 레이블을 위한 월별 날짜 배열을 생성합니다.
    const monthlyLabels = aggregatedData.map(item => item.date);

    return { aggregatedData, monthlyLabels };
}
