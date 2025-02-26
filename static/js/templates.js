// templates.js
// Select2 템플릿 함수 (노선용)
function formatLine(state) {
  if (!state.id) {
    return state.text;
  }
  // 전역 변수 stations에서 해당 노선을 탐색
  const selectedStation = stations.find(s => s.line === state.id);
  const operator = selectedStation ? selectedStation.operator : 'A1';
  const color = getLineColor(operator, state.id);
  // 노선 앞에 "● " 추가
  return $('<span>', { style: `color: ${color}; font-weight: bold;` }).text(`● ${state.text}`);
}

// Select2 템플릿 함수 (역용)
function formatStation(state) {
  if (!state.id) return state.text;
  const color = $(state.element).data('color') || '#CCCCCC';
  return $('<span>', { style: `color: ${color}; font-weight: bold;` }).text(state.text);
}
