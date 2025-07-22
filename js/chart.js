// 차트 인스턴스를 저장할 전역 변수들
let salesChart;
let expensesChart;
let productSalesChart;
let inventoryChart; // 네 번째 차트
let satisfactionChart; // 다섯 번째 차트
let trafficChart; // 여섯 번째 차트
let productivityChart; // 일곱 번째 차트


document.addEventListener('DOMContentLoaded', () => {
    // --- 차트 초기화 함수 ---
    const setupChart = (chartId, type, label, data, backgroundColor, borderColor, labels = ['1주차', '2주차', '3주차', '4주차']) => {
        const ctx = document.getElementById(chartId);
        if (ctx) {
            return new Chart(ctx, {
                type: type,
                data: {
                    labels: labels, // 레이블을 매개변수로 받을 수 있도록 수정
                    datasets: [{
                        label: label,
                        data: data,
                        backgroundColor: backgroundColor,
                        borderColor: borderColor,
                        borderWidth: 1,
                        fill: type === 'line'
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
                            display: true,
                            position: 'top'
                        }
                    }
                }
            });
        }
        return null;
    };

    // --- 모든 차트 초기화 ---
    salesChart = setupChart(
        'salesChart', 'bar', '주간 매출 (억)', [10, 15, 7, 20],
        'rgba(75, 192, 192, 0.6)', 'rgba(75, 192, 192, 1)'
    );

    expensesChart = setupChart(
        'expensesChart', 'line', '월별 비용 (만원)', [50, 45, 60, 55],
        'rgba(255, 99, 132, 0.6)', 'rgba(255, 99, 132, 1)'
    );

    productSalesChart = setupChart(
        'productSalesChart', 'doughnut', '제품별 판매량', [300, 200, 150, 100],
        [
            'rgba(255, 206, 86, 0.6)', 'rgba(54, 162, 235, 0.6)',
            'rgba(153, 102, 255, 0.6)', 'rgba(201, 203, 207, 0.6)'
        ],
        [
            'rgba(255, 206, 86, 1)', 'rgba(54, 162, 235, 1)',
            'rgba(153, 102, 255, 1)', 'rgba(201, 203, 207, 1)'
        ],
        ['제품A', '제품B', '제품C', '제품D'] // 도넛 차트 레이블 예시
    );

    // 나머지 4개 차트 초기화 (index.html의 id에 맞춰 추가)
    inventoryChart = setupChart(
        'inventoryChart', 'polarArea', '재고 현황', [120, 80, 50, 150],
        ['rgba(255, 159, 64, 0.6)', 'rgba(75, 192, 192, 0.6)', 'rgba(153, 102, 255, 0.6)', 'rgba(255, 99, 132, 0.6)'],
        ['rgba(255, 159, 64, 1)', 'rgba(75, 192, 192, 1)', 'rgba(153, 102, 255, 1)', 'rgba(255, 99, 132, 1)'],
        ['부품A', '부품B', '부품C', '부품D']
    );

    satisfactionChart = setupChart(
        'satisfactionChart', 'bar', '고객 만족도', [4.5, 4.2, 4.8, 4.1],
        'rgba(50, 205, 50, 0.6)', 'rgba(50, 205, 50, 1)',
        ['1분기', '2분기', '3분기', '4분기']
    );

    trafficChart = setupChart(
        'trafficChart', 'line', '웹사이트 트래픽', [1200, 1500, 1300, 1800, 1600],
        'rgba(255, 206, 86, 0.6)', 'rgba(255, 206, 86, 1)',
        ['월', '화', '수', '목', '금']
    );

    productivityChart = setupChart(
        'productivityChart', 'radar', '직원 생산성', [85, 90, 75, 88],
        'rgba(153, 102, 255, 0.4)', 'rgba(153, 102, 255, 1)',
        ['개발', '마케팅', '영업', '지원']
    );

    // --- 차트 슬라이더 로직 시작 ---
    const chartSlides = document.querySelectorAll('.chart-slide');
    let currentChartSlide = 0;
    const chartSlideInterval = 10000; // 10초 (밀리초 단위)

    function showChartSlide(index) {
        chartSlides.forEach((slide, i) => {
            if (i === index) {
                slide.classList.add('active');
            } else {
                slide.classList.remove('active');
            }
        });
    }

    function nextChartSlide() {
        currentChartSlide = (currentChartSlide + 1) % chartSlides.length;
        showChartSlide(currentChartSlide);
    }

    // 초기 차트 슬라이드 표시
    showChartSlide(currentChartSlide);

    // 10초마다 차트 슬라이드 전환
    setInterval(nextChartSlide, chartSlideInterval);
    // --- 차트 슬라이더 로직 끝 ---

    // --- 세계 시간 로직 (기존 코드 유지) ---
    const cityTimezones = {
        'LA': 'America/Los_Angeles',
        '뉴욕': 'America/New_York',
        '파리': 'Europe/Paris',
        '상하이': 'Asia/Shanghai',
        '서울': 'Asia/Seoul',
        '시드니': 'Australia/Sydney'
    };

    function updateWorldClocks() {
        const now = new Date();

        for (const city in cityTimezones) {
            const timezone = cityTimezones[city];
            const options = {
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit',
                hour12: false,
                timeZone: timezone
            };
            const timeString = new Intl.DateTimeFormat('ko-KR', options).format(now);
            
            const elementId = `time-${city.toLowerCase().replace(/[^a-z0-9]/g, '')}`;
            const timeElement = document.getElementById(elementId);
            if (timeElement) {
                timeElement.textContent = timeString;
            }
        }
    }

    // 초기 로드 시 시간 업데이트 및 1초마다 업데이트
    updateWorldClocks();
    setInterval(updateWorldClocks, 1000);

    // --- 마지막 업데이트 시간 표시 (기존 코드 유지) ---
    document.getElementById('last-updated').textContent = `마지막 업데이트: ${new Date().toLocaleTimeString()}`;
});
