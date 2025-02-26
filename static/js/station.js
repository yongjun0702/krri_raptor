// station.js
// 역 목록 불러오기 및 Select2 초기화, 호선/역 필터링 처리
let stations = [];

function initStationSelectors() {
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
  fetch('api/stations')
    .then(response => response.json())
    .then(data => {
      console.log("Received stations:", data);
      stations = data;
      const lines = [...new Set(stations.map(s => s.line))];
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
      $fromStationSelect.empty().append('<option value="">역 선택</option>')
        .prop('disabled', true)
        .trigger('change');
      return;
    }
    $fromStationSelect.prop('disabled', false);
    const filteredStations = stations
      .filter(s => s.line === selectedLine)
      .sort((a, b) => a.stop_name.localeCompare(b.stop_name));
    $fromStationSelect.empty().append('<option value="">역 선택</option>');
    filteredStations.forEach(s => {
      const $option = $('<option>')
        .val(s.stop_id)
        .text(s.stop_name)
        .data('color', getLineColor(s.operator, s.line));
      $fromStationSelect.append($option);
    });
    $fromStationSelect.trigger('change');
  });

  // 도착 호선 선택 시 해당 호선의 역만 표시
  $toLineSelect.on('change', () => {
    const selectedLine = $toLineSelect.val();
    if (!selectedLine) {
      $toStationSelect.empty().append('<option value="">역 선택</option>')
        .prop('disabled', true)
        .trigger('change');
      return;
    }
    $toStationSelect.prop('disabled', false);
    const filteredStations = stations
      .filter(s => s.line === selectedLine)
      .sort((a, b) => a.stop_name.localeCompare(b.stop_name));
    $toStationSelect.empty().append('<option value="">역 선택</option>');
    filteredStations.forEach(s => {
      const $option = $('<option>')
        .val(s.stop_id)
        .text(s.stop_name)
        .data('color', getLineColor(s.operator, s.line));
      $toStationSelect.append($option);
    });
    $toStationSelect.trigger('change');
  });
}
