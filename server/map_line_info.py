# server/map_line_info.py
import folium
from assets.color import getLineColor  # 노선 색상 함수

def draw_route_on_map(feed, route, route_info):
    """
    feed: GTFS 피드 (stops 정보를 포함)
    route: 최종 경로에 포함된 stop_id 리스트 (예: ['RS_ACC1_S-1-4928', 'RS_ACC1_S-1-4929', ...])
    route_info: 각 정류장의 추가 정보 리스트 (각 정류장의 operator, line 등)

    각 인접 구간에서 operator 또는 line 정보가 바뀌면 새로운 구간(segment)으로 분리,
    각 구간마다 getLineColor()로 색상을 지정한 후 Folium 지도에 표시.
    """
    # 첫 정류장을 기준으로 지도를 생성
    first_stop = feed.stops[feed.stops['stop_id'] == route[0]].iloc[0]
    m = folium.Map(location=[first_stop.stop_lat, first_stop.stop_lon], zoom_start=11)

    # 구간 분할: 현재 operator, line이 바뀌면 새로운 구간으로 분리
    segments = []
    current_segment = [route[0]]
    current_operator = route_info[0]['operator']
    current_line = route_info[0]['line']
    current_color = getLineColor(current_operator, current_line)

    for i in range(1, len(route)):
        info = route_info[i]
        op = info['operator']
        ln = info['line']
        if op != current_operator or ln != current_line:
            segments.append((current_segment, current_color))
            current_segment = [route[i]]
            current_operator = op
            current_line = ln
            current_color = getLineColor(current_operator, current_line)
        else:
            current_segment.append(route[i])
    segments.append((current_segment, current_color))

    all_coords = []
    # 각 구간을 지도에 추가
    for seg, seg_color in segments:
        seg_coords = []
        for stop_id in seg:
            stop_row = feed.stops[feed.stops['stop_id'] == stop_id].iloc[0]
            coord = [stop_row.stop_lat, stop_row.stop_lon]
            seg_coords.append(coord)
            all_coords.append(coord)
            folium.CircleMarker(
                location=coord,
                radius=5,
                color=seg_color,
                fill=True,
                fill_opacity=1.0
            ).add_to(m)
        if len(seg_coords) > 1:
            folium.PolyLine(
                seg_coords,
                weight=5,
                color=seg_color,
                opacity=0.8
            ).add_to(m)
    if all_coords:
        m.fit_bounds(all_coords)
    return m