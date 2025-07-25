import { setupChart } from './chart_utilities.js'; // setupChart 함수는 환율 차트를 위해 필요합니다.

// Path to the JSON data files
const WEATHER_JSON_URL = 'data/la_weather_data.json'; // LA 날씨 데이터 경로
const EXCHANGE_RATE_JSON_URL = 'data/exchange_rate_data.json'; // 환율 데이터 경로

// Chart instance for exchange rates
let exchangeRateChart;

// --- World Clock Logic ---
const cityTimezones = {
    'la': 'America/Los_Angeles',
    'ny': 'America/New_York',
    'paris': 'Europe/Paris',
    'shanghai': 'Asia/Shanghai',
    'seoul': 'Asia/Seoul',
    'sydney': 'Australia/Sydney'
};

export function updateWorldClocks() {
    const now = new Date();
    for (const cityKey in cityTimezones) {
        const timezone = cityTimezones[cityKey];
        const options = {
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: false,
            timeZone: timezone
        };
        const timeString = new Intl.DateTimeFormat('en-US', options).format(now);
        const elementId = `time-${cityKey}`;
        const timeElement = document.getElementById(elementId);
        if (timeElement) {
            timeElement.textContent = timeString;
        }
    }
}

// --- Weather and Exchange Rate Data Loading and Display Function ---
export async function loadAndDisplayAuxiliaryData() {
    let weatherData = {};
    let exchangeRatesData = {}; // Expecting an object with 'exchange_rate_history'

    try {
        // Fetch weather data
        const weatherResponse = await fetch(WEATHER_JSON_URL);
        if (!weatherResponse.ok) {
            throw new Error(`HTTP error! Status: ${weatherResponse.status} for ${WEATHER_JSON_URL}`);
        }
        weatherData = await weatherResponse.json();
        console.log("Loaded weather data:", weatherData);

        // Fetch exchange rate data
        const exchangeRateResponse = await fetch(EXCHANGE_RATE_JSON_URL);
        if (!exchangeRateResponse.ok) {
            throw new Error(`HTTP error! Status: ${exchangeRateResponse.status} for ${EXCHANGE_RATE_JSON_URL}`);
        }
        exchangeRatesData = await exchangeRateResponse.json();
        console.log("Loaded exchange rate data:", exchangeRatesData);

        // --- Update Weather Info ---
        const currentWeatherData = weatherData.current_weather || {};
        const forecastWeatherData = weatherData.forecast_weather || [];

        document.getElementById('temperature-current').textContent = currentWeatherData.LA_Temperature ? `${currentWeatherData.LA_Temperature}°F` : '--°F';
        document.getElementById('status-current').textContent = currentWeatherData.LA_WeatherStatus || 'Loading...';
        // Simple icon mapping (you might want a more robust one)
        const weatherIconUrl = (status) => {
            const base = 'https://placehold.co/80x80/';
            const defaultColor = 'cccccc';
            const textColor = 'ffffff';
            if (status) {
                const lowerStatus = status.toLowerCase();
                if (lowerStatus.includes('clear') || lowerStatus.includes('맑음')) return `${base}00657e/${textColor}?text=SUN`;
                if (lowerStatus.includes('cloud') || lowerStatus.includes('구름')) return `${base}003A52/${textColor}?text=CLOUD`;
                if (lowerStatus.includes('rain') || lowerStatus.includes('비')) return `${base}28A745/${textColor}?text=RAIN`;
                if (lowerStatus.includes('snow') || lowerStatus.includes('눈')) return `${base}17A2B8/${textColor}?text=SNOW`;
            }
            return `${base}${defaultColor}/${textColor}?text=Icon`; // Default placeholder
        };
        document.getElementById('weather-icon-current').src = weatherIconUrl(currentWeatherData.LA_WeatherStatus);

        document.getElementById('humidity-current').textContent = currentWeatherData.LA_Humidity ? `${currentWeatherData.LA_Humidity}%` : '--%';
        document.getElementById('wind-speed-current').textContent = currentWeatherData.LA_WindSpeed ? `${currentWeatherData.LA_WindSpeed} mph` : '-- mph';
        document.getElementById('pressure-current').textContent = currentWeatherData.LA_Pressure ? `${currentWeatherData.LA_Pressure} hPa` : '-- hPa';
        document.getElementById('visibility-current').textContent = currentWeatherData.LA_Visibility ? `${currentWeatherData.LA_Visibility} mile` : '-- mile';
        document.getElementById('sunrise-time').textContent = currentWeatherData.LA_Sunrise || '--';
        document.getElementById('sunset-time').textContent = currentWeatherData.LA_Sunset || '--';

        const forecastBody = document.getElementById('forecast-body');
        forecastBody.innerHTML = ''; // Clear existing rows
        if (forecastWeatherData.length > 0) {
            forecastWeatherData.slice(0, 7).forEach(day => { // Display up to 7 days
                const row = forecastBody.insertRow();
                row.insertCell().textContent = day.date || '--';
                row.insertCell().textContent = day.min_temp ? `${day.min_temp}°F` : '--';
                row.insertCell().textContent = day.max_temp ? `${day.max_temp}°F` : '--';
                row.insertCell().textContent = day.status || '--';
            });
        } else {
            forecastBody.innerHTML = '<tr><td colspan="4">No forecast data available.</td></tr>';
        }

        // --- Update Exchange Rate Info ---
        const exchangeRateHistory = exchangeRatesData.exchange_rate_history || [];
        const filteredExchangeRates = exchangeRateHistory.slice(Math.max(exchangeRateHistory.length - 30, 0)); // Latest 1 month (approx 30 days)
        
        const currentExchangeRate = filteredExchangeRates.length > 0 ? filteredExchangeRates[filteredExchangeRates.length - 1].USD : null;
        document.getElementById('current-exchange-rate-value').textContent = currentExchangeRate ? `${currentExchangeRate.toFixed(2)} KRW` : 'Loading...';

        if (exchangeRateChart) exchangeRateChart.destroy();
        
        const exchangeRateDatasets = [{
            label: 'USD/KRW Exchange Rate',
            data: filteredExchangeRates.map(item => ({ x: item.date, y: item.USD })), // Use x,y for time scale, assuming 'USD' is the key for rate
            backgroundColor: 'rgba(253, 126, 20, 0.5)',
            borderColor: '#e68a00',
            borderWidth: 2,
            fill: false,
            pointRadius: 0 // No points on exchange rate chart
        }];
        console.log("Exchange Rate Chart Datasets (before setup):", exchangeRateDatasets);
        console.log("Exchange Rate Chart Data Sample (first 5 points):", exchangeRateDatasets[0].data.slice(0, 5));

        exchangeRateChart = setupChart(
            'exchangeRateChartCanvas', 'line',
            exchangeRateDatasets,
            {
                scales: {
                    x: {
                        type: 'time', // Changed to time scale
                        time: { 
                            unit: 'day', 
                            displayFormats: { day: 'MM/dd' }, // Changed MM-dd to MM/dd
                            tooltipFormat: 'M/d/yyyy' // Consistent tooltip format
                        },
                        ticks: { autoSkipPadding: 10 } // Removed maxTicksLimit
                    },
                    y: {
                        beginAtZero: false, // Exchange rates might not start at zero
                        ticks: { count: 5 }, // Enforce 5 ticks on Y-axis
                        grid: { display: false } // Remove grid lines
                    }
                },
                plugins: {
                    legend: { display: false } // No legend for this small chart
                }
            },
            false // Exchange rate chart is granular (not aggregated by month)
        );

    } catch (error) {
        console.error("Error loading or processing weather or exchange rate data:", error);
        // Update UI to show error for weather/exchange rate sections
        document.getElementById('weather-info-container').innerHTML = '<p class="text-red-500">Failed to load weather data.</p>';
        document.getElementById('exchangeRateChartContainer').innerHTML = '<p class="text-red-500">Failed to load exchange rate data.</p>';
    }
}
