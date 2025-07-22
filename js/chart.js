// 차트 인스턴스를 저장할 전역 변수들
let salesChart;
let expensesChart;
let productSalesChart;
let inventoryChart;
let satisfactionChart;
let trafficChart;
let productivityChart;

// Google Sheet에서 가져올 JSON 데이터 파일 경로
const DATA_JSON_URL = 'data/crawling_data.json'; // Python 스크립트가 생성할 파일 경로

document.addEventListener('DOMContentLoaded', () => {
    // --- 차트 초기화 함수 ---
    const setupChart = (chartId, type, label, data, backgroundColor, borderColor, labels = []) => {
        const ctx = document.getElementById(chartId);
        if (ctx) {
            // 차트 인스턴스가 이미 있으면 파괴하여 새로 그릴 준비
            if (Chart.getChart(chartId)) {
                Chart.getChart(chartId).destroy();
            }
            return new Chart(ctx, {
                type: type,
                data: {
                    labels: labels,
                    datasets: [{
                        label: label,
                        data: data,
                        backgroundColor: backgroundColor,
                        borderColor: borderColor,
                        borderWidth: 1,
                        fill: type === 'line' || type === 'radar' || type === 'polarArea' // 라인, 레이더, 폴라 차트만 채움
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

    // --- 차트 슬라이더 로직 ---
    const chartSlides = document.querySelectorAll('.chart-slide');
    let currentChartSlide = 0;
    const chartSlideInterval = 10000; // 10초

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

    // --- 세계 시간 로직 (버그 수정) ---
    const cityTimezones = {
        'la': 'America/Los_Angeles', // HTML ID 접미사와 일치하도록 소문자로 변경
        'ny': 'America/New_York',
        'paris': 'Europe/Paris',
        'shanghai': 'Asia/Shanghai',
        'seoul': 'Asia/Seoul',
        'sydney': 'Australia/Sydney'
    };

    function updateWorldClocks() {
        const now = new Date();

        for (const cityKey in cityTimezones) { // cityKey 사용 ('la', 'ny' 등)
            const timezone = cityTimezones[cityKey];
            const options = {
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit',
                hour12: false,
                timeZone: timezone
            };
            const timeString = new Intl.DateTimeFormat('ko-KR', options).format(now);
            
            const elementId = `time-${cityKey}`; // HTML ID와 직접 일치
            const timeElement = document.getElementById(elementId);
            if (timeElement) {
                timeElement.textContent = timeString;
            }
        }
    }

    // --- 데이터 로딩 및 대시보드 업데이트 함수 ---
    async function loadAndDisplayData() {
        let rawData = [];
        try {
            const response = await fetch(DATA_JSON_URL);
            if (!response.ok) {
                throw new Error(`HTTP 오류! 상태: ${response.status}`);
            }
            rawData = await response.json();
            console.log("로드된 데이터:", rawData);

            if (rawData.length === 0) {
                console.warn("JSON 파일에 데이터가 없습니다.");
                return;
            }

            // --- 날씨 정보 업데이트 (가장 최근 데이터 사용) ---
            // 가정: 날씨 정보는 rawData의 마지막 행에 있고, '도시', '온도', '날씨설명', '아이콘코드' 컬럼이 있음
            const latestData = rawData[rawData.length - 1];
            if (latestData['도시'] && latestData['온도']) {
                document.getElementById('city-name').textContent = latestData['도시'];
                document.getElementById('temperature').textContent = `${parseFloat(latestData['온도']).toFixed(1)}°C`;
                document.getElementById('description').textContent = latestData['날씨설명'];
                const iconCode = latestData['아이콘코드'];
                if (iconCode) {
                    document.getElementById('weather-icon').src = `https://openweathermap.org/img/wn/${iconCode}@2x.png`;
                    document.getElementById('weather-icon').alt = latestData['날씨설명'];
                }
            } else {
                console.warn("날씨 데이터를 찾을 수 없습니다. JSON 컬럼 이름을 확인하세요.");
            }

            // --- 환율 정보 업데이트 ---
            // 가정: 환율 정보도 rawData에 있고, '통화쌍', '환율' 컬럼이 있음
            const usdKrwRate = rawData.find(row => row['통화쌍'] === 'USD/KRW');
            const jpyKrwRate = rawData.find(row => row['통화쌍'] === 'JPY/KRW');
            const eurKrwRate = rawData.find(row => row['통화쌍'] === 'EUR/KRW'); // EUR/KRW 추가

            if (usdKrwRate && usdKrwRate['환율']) {
                document.getElementById('usd-krw').textContent = parseFloat(usdKrwRate['환율']).toFixed(2);
            } else { console.warn("USD/KRW 환율 데이터를 찾을 수 없습니다."); }
            if (jpyKrwRate && jpyKrwRate['환율']) {
                document.getElementById('jpy-krw').textContent = parseFloat(jpyKrwRate['환율']).toFixed(2);
            } else { console.warn("JPY/KRW 환율 데이터를 찾을 수 없습니다."); }
            if (eurKrwRate && eurKrwRate['환율']) {
                document.getElementById('eur-krw').textContent = parseFloat(eurKrwRate['환율']).toFixed(2);
            } else { console.warn("EUR/KRW 환율 데이터를 찾을 수 없습니다."); }


            // --- 차트 데이터 준비 및 초기화 ---
            // 모든 차트 데이터를 동적으로 준비합니다.
            // 각 차트가 어떤 컬럼의 데이터를 사용할지 여기에 정의합니다.
            // 예시: 'date'를 레이블로, '종합지수'를 데이터로 사용
            const dates = rawData.map(item => item['date'] ? new Date(item['date']).toLocaleDateString('ko-KR') : '');
            
            // 각 차트별 데이터셋을 정의합니다.
            // 이 부분은 당신의 Google Sheet 데이터 구조와 차트로 보여주고 싶은 내용에 따라 크게 달라집니다.
            // 예시로 몇 가지 차트를 구성해 두었습니다.
            
            // 1. 주간 매출 추이 (종합지수 예시)
            salesChart = setupChart(
                'salesChart', 'line', '종합지수', rawData.map(item => item['종합지수']),
                'rgba(75, 192, 192, 0.6)', 'rgba(75, 192, 192, 1)', dates
            );

            // 2. 월별 비용 추이 (미주서안 예시)
            expensesChart = setupChart(
                'expensesChart', 'bar', '미주서안 ($/FEU)', rawData.map(item => item['미주서안']),
                'rgba(255, 99, 132, 0.6)', 'rgba(255, 99, 132, 1)', dates
            );

            // 3. 제품별 판매량 (미주동안 예시)
            productSalesChart = setupChart(
                'productSalesChart', 'line', '미주동안 ($/FEU)', rawData.map(item => item['미주동안']),
                'rgba(54, 162, 235, 0.6)', 'rgba(54, 162, 235, 1)', dates
            );

            // 4. 재고 현황 (유럽 예시)
            inventoryChart = setupChart(
                'inventoryChart', 'bar', '유럽 ($/FEU)', rawData.map(item => item['유럽']),
                'rgba(255, 159, 64, 0.6)', 'rgba(255, 159, 64, 1)', dates
            );

            // 5. 고객 만족도 (지중해 예시)
            satisfactionChart = setupChart(
                'satisfactionChart', 'line', '지중해 ($/FEU)', rawData.map(item => item['지중해']),
                'rgba(153, 102, 255, 0.6)', 'rgba(153, 102, 255, 1)', dates
            );

            // 6. 웹사이트 트래픽 (중동 예시)
            trafficChart = setupChart(
                'trafficChart', 'bar', '중동 ($/FEU)', rawData.map(item => item['중동']),
                'rgba(255, 206, 86, 0.6)', 'rgba(255, 206, 86, 1)', dates
            );

            // 7. 직원 생산성 (호주 예시)
            productivityChart = setupChart(
                'productivityChart', 'line', '호주 ($/FEU)', rawData.map(item => item['호주']),
                'rgba(75, 192, 192, 0.6)', 'rgba(75, 192, 192, 1)', dates
            );


            // 차트 슬라이더 시작
            showChartSlide(currentChartSlide); // 초기 차트 슬라이드 표시
            setInterval(nextChartSlide, chartSlideInterval); // 10초마다 차트 전환

        } catch (error) {
            console.error("JSON 데이터 로드 또는 처리 중 오류 발생:", error);
            // 오류 발생 시 플레이스홀더 텍스트 유지
        }
    }

    // 초기 로드 시 세계 시간 업데이트
    updateWorldClocks();
    // 1초마다 세계 시간 업데이트
    setInterval(updateWorldClocks, 1000);

    // 마지막 업데이트 시간 표시
    document.getElementById('last-updated').textContent = `마지막 업데이트: ${new Date().toLocaleTimeString()}`;

    // JSON 데이터 로드 및 대시보드 표시 시작
    loadAndDisplayData();
});
