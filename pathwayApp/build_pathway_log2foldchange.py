"""Gera pathwaylog2foldchange.html a partir de pathway.html + silver_pathway_areas CSV."""

from __future__ import annotations

import csv
import json
import re
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PATHWAY_HTML = ROOT / "pathway.html"
CSV_PATH = ROOT / "silver_pathway_areasv1.csv"
OUTPUT = ROOT / "pathwaylog2foldchange.html"

EXTRA_CSS = """
.log2-legend {
	margin: 8px 0 12px 0;
	font-size: 13px;
	max-width: 520px;
}
.log2-legend-bar {
	height: 14px;
	border-radius: 3px;
	background: linear-gradient(to right, #2166ac, #f7f7f7, #b2182b);
	border: 1px solid #999;
}
.log2-legend-labels {
	display: flex;
	justify-content: space-between;
	margin-top: 4px;
	color: #333;
}
#pathway-wrap {
	position: relative;
	display: inline-block;
}
#log2-overlay {
	position: absolute;
	left: 0;
	top: 0;
	pointer-events: none;
}
"""

LEGEND_HTML = """
<div class="log2-legend" id="log2-legend">
	<strong>log2 fold change</strong> (silver_geo_nodes × map05171)
	<div class="log2-legend-bar"></div>
	<div class="log2-legend-labels">
		<span id="log2-min-label">min</span>
		<span>0</span>
		<span id="log2-max-label">max</span>
	</div>
</div>
"""

IMG_WRAP_OLD = """<div class="image-block" style="position:static;">
	
	<img src="map05171@2x_20260606_220006.png" id="pathwayimage"""

IMG_WRAP_NEW = """<div class="image-block" style="position:static;">
	<div id="pathway-wrap">
	<img src="map05171@2x_20260606_220006.png" id="pathwayimage"""

IMG_CLOSE_OLD = """ alt="KEGG map05171 Coronavirus disease" />
	<map id="mapdata\""""

IMG_CLOSE_NEW = """ alt="KEGG map05171 Coronavirus disease" />
	<svg id="log2-overlay" xmlns="http://www.w3.org/2000/svg"></svg>
	</div>
	<map id="mapdata\""""


def load_log2_by_area(csv_path: Path) -> dict[str, float]:
    buckets: dict[str, list[float]] = defaultdict(list)
    with csv_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            buckets[row["idarea"]].append(float(row["log2foldchange"]))
    return {area_id: sum(values) / len(values) for area_id, values in buckets.items()}


def color_script(log2_by_area: dict[str, float]) -> str:
    values = list(log2_by_area.values())
    min_v = min(values)
    max_v = max(values)
    payload = json.dumps(log2_by_area, ensure_ascii=False)

    return f"""
<script>
(function () {{
  var LOG2_BY_AREA = {payload};
  var LOG2_MIN = {min_v};
  var LOG2_MAX = {max_v};

  function colorForLog2(value) {{
    if (LOG2_MAX === LOG2_MIN) {{
      return 'rgba(200,200,200,0.5)';
    }}
    var t = (value - LOG2_MIN) / (LOG2_MAX - LOG2_MIN);
    var r = Math.round(33 + t * (178 - 33));
    var g = Math.round(102 + t * (24 - 102));
    var b = Math.round(172 + t * (43 - 172));
    return 'rgba(' + r + ',' + g + ',' + b + ',0.62)';
  }}

  function paintLog2Overlay() {{
    var img = document.getElementById('pathwayimage');
    var svg = document.getElementById('log2-overlay');
    var map = document.getElementById('mapdata');
    if (!img || !svg || !map) return;

    var w = img.clientWidth;
    var h = img.clientHeight;
    svg.setAttribute('width', String(w));
    svg.setAttribute('height', String(h));
    svg.setAttribute('viewBox', '0 0 ' + w + ' ' + h);
    svg.innerHTML = '';

    Object.keys(LOG2_BY_AREA).forEach(function (idarea) {{
      var area = map.querySelector('area#' + CSS.escape(idarea));
      if (!area) return;
      var shape = (area.getAttribute('shape') || '').toLowerCase();
      var coords = (area.getAttribute('coords') || '')
        .split(',')
        .map(function (v) {{ return parseFloat(v); }});
      if (!coords.length || coords.some(isNaN)) return;

      var color = colorForLog2(LOG2_BY_AREA[idarea]);
      var el;

      if (shape === 'rect' && coords.length >= 4) {{
        el = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
        el.setAttribute('x', String(Math.min(coords[0], coords[2])));
        el.setAttribute('y', String(Math.min(coords[1], coords[3])));
        el.setAttribute('width', String(Math.abs(coords[2] - coords[0])));
        el.setAttribute('height', String(Math.abs(coords[3] - coords[1])));
      }} else if (shape === 'circle' && coords.length >= 3) {{
        el = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        el.setAttribute('cx', String(coords[0]));
        el.setAttribute('cy', String(coords[1]));
        el.setAttribute('r', String(coords[2]));
      }} else {{
        return;
      }}

      el.setAttribute('fill', color);
      el.setAttribute('stroke', 'none');
      svg.appendChild(el);
    }});
  }}

  function updateLegendLabels() {{
    var minEl = document.getElementById('log2-min-label');
    var maxEl = document.getElementById('log2-max-label');
    if (minEl) minEl.textContent = LOG2_MIN.toFixed(2) + ' (azul)';
    if (maxEl) maxEl.textContent = LOG2_MAX.toFixed(2) + ' (vermelho)';
  }}

  function hookPaint() {{
    updateLegendLabels();
    paintLog2Overlay();
  }}

  window.paintLog2Overlay = paintLog2Overlay;

  document.addEventListener('DOMContentLoaded', function () {{
    updateLegendLabels();
    var img = document.getElementById('pathwayimage');
    if (img) {{
      img.addEventListener('load', hookPaint);
    }}
    setTimeout(hookPaint, 50);
  }});
}})();
</script>
"""


def main() -> None:
    html = PATHWAY_HTML.read_text(encoding="utf-8")
    log2_by_area = load_log2_by_area(CSV_PATH)

    html = html.replace(
        "<title>\nKEGG PATHWAY: Coronavirus disease - Reference pathway\n</title>",
        "<title>map05171 — log2 fold change overlay</title>",
        1,
    )
    html = html.replace("<h1 class=\"page-title\">\n\tTestes 123 \n</h1>", """<h1 class="page-title">
\tmap05171 — gradiente log2 fold change (azul ↓ · vermelho ↑)
</h1>""", 1)

    if EXTRA_CSS.strip() not in html:
        html = html.replace("//-->\n</style>", EXTRA_CSS + "\n//-->\n</style>", 1)

    if 'id="log2-legend"' not in html:
        html = html.replace(
            "<!-- pathway image start -->",
            "<!-- pathway image start -->\n" + LEGEND_HTML,
            1,
        )

    if 'id="pathway-wrap"' not in html:
        html = html.replace(IMG_WRAP_OLD, IMG_WRAP_NEW, 1)
        html = html.replace(IMG_CLOSE_OLD, IMG_CLOSE_NEW, 1)

    html = html.replace(
        "    img.setAttribute('usemap', '#mapdata');\n  }",
        "    img.setAttribute('usemap', '#mapdata');\n"
        "    if (window.paintLog2Overlay) window.paintLog2Overlay();\n"
        "  }",
        1,
    )

    marker = "</body>"
    script = color_script(log2_by_area)
    html = re.sub(
        r"<script>\s*\(function \(\) \{\s*var LOG2_BY_AREA.*?</script>\s*",
        "",
        html,
        count=1,
        flags=re.DOTALL,
    )
    if "var LOG2_BY_AREA" not in html:
        html = html.replace(marker, script + "\n" + marker, 1)

    OUTPUT.write_text(html, encoding="utf-8")
    print(f"Gerado {OUTPUT} ({len(log2_by_area)} areas coloridas)")


if __name__ == "__main__":
    main()
