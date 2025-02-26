// route_form.js
// 경로 검색 폼 제출 및 결과 렌더링 처리
function initRouteForm() {
  $('#route-form').on('submit', async (e) => {
    e.preventDefault();
    console.log("경로 검색 폼 제출됨");
    const formData = new FormData(e.target);
    for (const [key, value] of formData.entries()) {
      console.log(key, value);
    }
    $('#loading').removeClass('d-none');
    $('#result').addClass('d-none');
    try {
      const response = await fetch('api/find_route', {
        method: 'POST',
        body: formData
      });
      console.log("find_route 응답 상태:", response.status);
      const data = await response.json();
      console.log("find_route 데이터:", data);
      if (data.error) {
        alert(data.error);
        return;
      }
      // 총 소요시간 표시
      $('#total-time').text(data.total_time);

      // 경로 상세 표시: arrival/ departure 분리하여 표시
      $('#route-info').html(`
        <div class="route-timeline">
          ${data.route_info.map(stop => `
            <div class="timeline-segment" style="border-left: 8px solid ${getLineColor(stop.operator, stop.line)}">
              <div class="segment-time">
                ${stop.arrival ? `<span>도착: ${stop.arrival}</span><br/>` : ''}
                ${stop.departure ? `<span>출발: ${stop.departure}</span>` : ''}
              </div>
              <div class="segment-station">
                <strong>${stop.station}</strong>
                <span class="line-info" style="color:${getLineColor(stop.operator, stop.line)}">
                  ${stop.line_info ? `(${stop.line_info})` : ''}
                </span>
              </div>
            </div>
          `).join('')}
        </div>
      `);

      // 지도 iframe 표시
      const mapFrame = $('<iframe>', {
        src: 'static/route_result.html',
        css: {
          width: '100%',
          height: '100%',
          border: 'none'
        }
      });
      $('#map').empty().append(mapFrame);
      $('#result').removeClass('d-none');
    } catch (error) {
      console.error("find_route 오류:", error);
      alert("경로 검색 중 오류가 발생했습니다.");
    } finally {
      $('#loading').addClass('d-none');
    }
  });
}

if (typeof module !== 'undefined' && module.exports) {
  module.exports = { initRouteForm };
} else {
  window.initRouteForm = initRouteForm;
}