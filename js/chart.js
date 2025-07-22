// 차트 초기화 (기존 코드 유지)
let salesChart;

document.addEventListener('DOMContentLoaded', () => {
    // 기존 차트 초기화 로직 (생략)
    const ctx = document.getElementById('salesChart');
    if (ctx) {
        salesChart = new Chart(ctx, { /* ... 차트 설정 ... */ });
    }

    // --- 슬라이더 로직 (기존 코드 유지) ---
    const slides = document.querySelectorAll('.slide');
    let currentSlide = 0;
    const slideInterval = 10000; // 10초

    function showSlide(index) { /* ... 기존 슬라이더 로직 ... */ }
    function nextSlide() { /* ... 기존 슬라이더 로직 ... */ }

    showSlide(currentSlide);
    setInterval(nextSlide, slideInterval);

    // --- 세계 시간 로직 시작 ---
    const cityTimezones = {
        'LA': 'America/Los_Angeles',
        '뉴욕': 'America/New_York',
        '파리': 'Europe/Paris',
        '상하이': 'Asia/Shanghai',
        '서울': 'Asia/Seoul',
        '시드니': 'Australia/Sydney'
    };

    function updateWorldClocks() {
        const now = new Date(); // 현재 시간

        for (const city in cityTimezones) {
            const timezone = cityTimezones[city];
            const options = {
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit',
                hour12: false, // 24시간 형식
                timeZone: timezone
            };
            const timeString = new Intl.DateTimeFormat('ko-KR', options).format(now);
            
            // HTML ID에 맞게 동적으로 접근
            const elementId = `time-${city.toLowerCase().replace(/[^a-z0-9]/g, '')}`; // 예: 'LA' -> 'time-la'
            const timeElement = document.getElementById(elementId);
            if (timeElement) {
                timeElement.textContent = timeString;
            }
        }
    }

    // 초기 로드 시 시간 업데이트
    updateWorldClocks();
    // 1초마다 시간 업데이트 (더 정확한 실시간 표시를 위해)
    setInterval(updateWorldClocks, 1000);

    // --- 세계 시간 로직 끝 ---

    // 마지막 업데이트 시간 표시 (기존 코드 유지)
    document.getElementById('last-updated').textContent = `마지막 업데이트: ${new Date().toLocaleTimeString()}`;
});
