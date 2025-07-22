// 이 파일은 현재 비어 있습니다.
// 나중에 Google Sheets에서 데이터를 가져와 대시보드를 동적으로 업데이트하는 JavaScript 코드를 여기에 추가할 것입니다.

// 예시: 차트 초기화 (데이터 없이 레이아웃만 확인)
document.addEventListener('DOMContentLoaded', () => {
    const ctx = document.getElementById('salesChart');
    if (ctx) {
        new Chart(ctx, {
            type: 'bar',
            data: {
                labels: ['월', '화', '수', '목', '금', '토', '일'],
                datasets: [{
                    label: '매출 (예시)',
                    data: [10, 15, 7, 20, 12, 18, 5], // 임시 데이터
                    backgroundColor: 'rgba(75, 192, 192, 0.6)',
                    borderColor: 'rgba(75, 192, 192, 1)',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: { beginAtZero: true }
                },
                plugins: {
                    legend: {
                        display: true
                    }
                }
            }
        });
    }
});
