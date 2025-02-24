// 노선 색상 매핑 (제공된 코드)
const LINE_COLORS = {
  'A1': {
    '서울1호선': '#0052A4',
    '서울2호선': '#00A84D',
    '서울3호선': '#EF7C1C',
    '서울4호선': '#00A5DE',
    '서울5호선': '#996CAC',
    '서울6호선': '#CD7C2F',
    '서울7호선': '#747F00',
    '서울8호선': '#E6186C',
    '서울9호선': '#BDB092',
    '경의중앙선': '#77C4A3',
    '수인분당선': '#F5A200',
    '경강선': '#003DA5',
    '경춘선': '#0C8E72',
    'KTX': '#204080',
    'ITX': '#505457',
    'ITX-새마을': '#C30E2F',
    'ITX-청춘': '#1CAE4C',
    '새마을호': '#5288F5',
    '무궁화호': '#E06040',
    '누리로': '#3D99C2',
    '통근열차': '#80E080',
    '중부내륙순환열차': '#3D860B',
    '백두대간협곡열차': '#3698D2',
    '남도해양열차': '#074286',
    '평화열차': '#1D2A56',
    '정선아리랑열차': '#753778',
    '서해금빛열차': '#F9BE00',
    '동해산타열차': '#139DA7',
    '공항철도': '#0090D2',
    '김포도시철도': '#A17800',
    '신분당선': '#D4003B',
    '서해선': '#81A914',
    '인천1호선': '#7CA8D5',
    '인천2호선': '#ED8B00',
    '의정부경전철': '#FDA600',
    '우이신설경전철': '#B0CE18',
    '용인경전철': '#509F22',
    '자기부상열차': '#FFCD12'
  }
};

function getLineColor(operator, line) {
  return LINE_COLORS[operator]?.[line] || '#CCCCCC';
}

// Select2 템플릿 함수 (노선용)
function formatLine(state) {
  if (!state.id) {
    return state.text;
  }
  const station = stations.find(s => s.line === state.id);
  const operator = station ? station.operator : 'A1';
  const color = getLineColor(operator, state.id);
  // 노선 앞에 "● "를 붙임
  return $('<span>', { style: `color: ${color}; font-weight: bold;` }).text(`● ${state.text}`);
}

// Select2 템플릿 함수 (역용)
function formatStation(state) {
  if (!state.id) return state.text;
  const color = $(state.element).data('color') || '#CCCCCC';
  return $('<span>', { style: `color: ${color}; font-weight: bold;` }).text(state.text);
}

let stations = [];

document.addEventListener('DOMContentLoaded', () => {
  console.log("DOM fully loaded. Fetching station list...");

  const $fromLineSelect = $('#from-line');
  const $fromStationSelect = $('#from-station');
  const $toLineSelect = $('#to-line');
  const $toStationSelect = $('#to-station');

  // 노선 Select2 초기화 (템플릿 적용)
  $fromLineSelect.select2({
    placeholder: "호선 선택",
    templateResult: formatLine,
    templateSelection: formatLine
  });
  $toLineSelect.select2({
    placeholder: "호선 선택",
    templateResult: formatLine,
    templateSelection: formatLine
  });
  // 역 Select2 초기화 (템플릿 적용)
  $fromStationSelect.select2({
    placeholder: "역 선택",
    templateResult: formatStation,
    templateSelection: formatStation
  });
  $toStationSelect.select2({
    placeholder: "역 선택",
    templateResult: formatStation,
    templateSelection: formatStation
  });

  // 역 목록 불러오기
  fetch('/stations')
    .then(response => response.json())
    .then(data => {
      console.log("Received stations:", data);
      stations = data;
      const lines = [...new Set(stations.map(station => station.line))];
      lines.sort((a, b) => a.localeCompare(b));
      lines.forEach(line => {
        $fromLineSelect.append($('<option>', { value: line, text: line }));
        $toLineSelect.append($('<option>', { value: line, text: line }));
      });
      $fromLineSelect.trigger('change');
      $toLineSelect.trigger('change');
    })
    .catch(error => {
      console.error("Error fetching stations:", error);
      alert("역 목록 불러오기에 실패했습니다.");
    });

  // 출발 호선 선택 시 해당 호선의 역만 표시
  $fromLineSelect.on('change', () => {
    const selectedLine = $fromLineSelect.val();
    if (!selectedLine) {
      $fromStationSelect.empty().append('<option value="">역 선택</option>').prop('disabled', true).trigger('change');
      return;
    }
    $fromStationSelect.prop('disabled', false);
    const filteredStations = stations
      .filter(station => station.line === selectedLine)
      .sort((a, b) => a.stop_name.localeCompare(b.stop_name));
    $fromStationSelect.empty().append('<option value="">역 선택</option>');
    filteredStations.forEach(station => {
      const $option = $('<option>')
        .val(station.stop_id)
        .text(`${station.stop_name}`)
        .data('color', getLineColor(station.operator, station.line));
      $fromStationSelect.append($option);
    });
    $fromStationSelect.trigger('change');
  });

  // 도착 호선 선택 시 해당 호선의 역만 표시
  $toLineSelect.on('change', () => {
    const selectedLine = $toLineSelect.val();
    if (!selectedLine) {
      $toStationSelect.empty().append('<option value="">역 선택</option>').prop('disabled', true).trigger('change');
      return;
    }
    $toStationSelect.prop('disabled', false);
    const filteredStations = stations
      .filter(station => station.line === selectedLine)
      .sort((a, b) => a.stop_name.localeCompare(b.stop_name));
    $toStationSelect.empty().append('<option value="">역 선택</option>');
    filteredStations.forEach(station => {
      const $option = $('<option>')
        .val(station.stop_id)
        .text(`${station.stop_name}`)
        .data('color', getLineColor(station.operator, station.line));
      $toStationSelect.append($option);
    });
    $toStationSelect.trigger('change');
  });

  // 경로 검색 폼 제출 이벤트 처리
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
      const response = await fetch('/find_route', {
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
      $('#total-time').text(data.total_time);
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
      const mapFrame = $('<iframe>', {
        src: '/route_result.html',
        css: { width: '100%', height: '100%', border: 'none' }
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
});