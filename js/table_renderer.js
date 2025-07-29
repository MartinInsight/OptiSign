// js/table_renderer.js

// 데이터를 HTML 테이블로 렌더링하는 유틸리티 함수입니다.

/**
 * 주어진 데이터를 사용하여 HTML 테이블을 렌더링합니다.
 * @param {string} containerId - 테이블을 렌더링할 컨테이너 요소의 ID.
 * @param {Array<string>} headers - 테이블 헤더 문자열 배열.
 * @param {Array<Object>} rows - 테이블 행 데이터 배열. 각 객체는 'route', 'current_index', 'previous_index', 'weekly_change' 속성을 포함해야 합니다.
 */
export function renderTable(containerId, headers, rows) {
    const container = document.getElementById(containerId);
    if (!container) {
        console.error(`Table container element with ID '${containerId}' not found.`);
        return;
    }

    // 데이터가 없으면 플레이스홀더 메시지를 표시합니다.
    if (!rows || rows.length === 0) {
        container.innerHTML = '<p class="text-gray-500 text-center py-4">No table data available.</p>';
        return;
    }

    let tableHtml = `
        <table class="min-w-full bg-white rounded-lg shadow overflow-hidden">
            <thead class="bg-gray-200">
                <tr>
                    ${headers.map(header => `<th class="py-2 px-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">${header}</th>`).join('')}
                </tr>
            </thead>
            <tbody class="divide-y divide-gray-200">
    `;

    rows.forEach(row => {
        // 'KCCI_종합지수'와 같은 형식에서 '종합지수'만 추출하여 표시합니다.
        const displayName = row.route.split('_').slice(1).join('_');

        const changeValue = row.weekly_change.value !== null ? parseFloat(row.weekly_change.value).toFixed(2) : '--';
        const percentage = row.weekly_change.percentage !== null ? ` (${row.weekly_change.percentage})` : '';
        const colorClass = row.weekly_change.color_class || 'text-gray-700';

        tableHtml += `
            <tr>
                <td class="py-2 px-3 whitespace-nowrap text-sm font-medium text-gray-900">${displayName}</td>
                <td class="py-2 px-3 whitespace-nowrap text-sm text-gray-700">${row.current_index || '--'}</td>
                <td class="py-2 px-3 whitespace-nowrap text-sm text-gray-700">${row.previous_index || '--'}</td>
                <td class="py-2 px-3 whitespace-nowrap text-sm ${colorClass}">
                    ${changeValue}${percentage}
                </td>
            </tr>
        `;
    });

    tableHtml += `
            </tbody>
        </table>
    `;

    container.innerHTML = tableHtml;
}
