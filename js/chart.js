// 차트 초기화 (데이터 없이 레이아웃만 확인)
let salesChart; // 차트 인스턴스를 저장할 변수

document.addEventListener('DOMContentLoaded', () => {
    const ctx = document.getElementById('salesChart');
    if (ctx) {
        salesChart = new Chart(ctx, { // salesChart 변수에 할당
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

    // --- 슬라이더 로직 시작 ---
    const slides = document.querySelectorAll('.slide');
    let currentSlide = 0;
    const slideInterval = 10000; // 10초 (밀리초 단위)

    function showSlide(index) {
        slides.forEach((slide, i) => {
            if (i === index) {
                slide.classList.add('active');
            } else {
                slide.classList.remove('active');
            }
        });
    }

    function nextSlide() {
        currentSlide = (currentSlide + 1) % slides.length;
        showSlide(currentSlide);
    }

    // 초기 슬라이드 표시
    showSlide(currentSlide);

    // 10초마다 슬라이드 전환
    setInterval(nextSlide, slideInterval);

    // --- 슬라이더 로직 끝 ---

    // 마지막 업데이트 시간 표시 (예시)
    document.getElementById('last-updated').textContent = `마지막 업데이트: ${new Date().toLocaleTimeString()}`;
});
