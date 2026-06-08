"""Gera pathwaylog2foldchange3d.html — visão isométrica com barras 3D por idarea."""

from __future__ import annotations

import csv
import json
import re
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PATHWAY_HTML = ROOT / "pathway.html"
CSV_PATH = ROOT / "silver_pathway_areasv1.csv"
OUTPUT = ROOT / "pathwaylog2foldchange3d.html"
IMG_PATH = "map05171@2x_20260606_220006.png"
MAP_W_1X = 3538 / 2
MAP_H_1X = 5276 / 2


def load_area_data(csv_path: Path) -> dict[str, dict]:
    """Agrupa linhas do CSV por idarea (mesma lógica do overlay 2D)."""
    buckets: dict[str, list[dict[str, str]]] = defaultdict(list)
    with csv_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            buckets[row["idarea"]].append(row)

    result: dict[str, dict] = {}
    for area_id, rows in buckets.items():
        log2_values = [float(row["log2foldchange"]) for row in rows]
        symbols = [row["symbol"] for row in rows]
        entries = [
            {"symbol": row["symbol"], "log2": float(row["log2foldchange"])}
            for row in rows
        ]
        result[area_id] = {
            "log2": sum(log2_values) / len(log2_values),
            "symbols": symbols,
            "entries": entries,
        }
    return result


def parse_area_geometry(shape: str, coords: str) -> dict | None:
    parts = [float(v) for v in coords.split(",")]
    if shape == "circle" and len(parts) >= 3:
        cx, cy, r = parts[0], parts[1], parts[2]
        return {"cx": cx, "cy": cy, "w": r * 2, "h": r * 2, "shape": "circle"}
    if shape == "rect" and len(parts) >= 4:
        x1, y1, x2, y2 = parts[0], parts[1], parts[2], parts[3]
        return {
            "cx": (x1 + x2) / 2,
            "cy": (y1 + y2) / 2,
            "w": abs(x2 - x1),
            "h": abs(y2 - y1),
            "shape": "rect",
        }
    return None


def load_map_bounds(html_path: Path) -> tuple[float, float]:
    return MAP_W_1X, MAP_H_1X


def load_areas_from_html(
    html_path: Path, area_data: dict[str, dict]
) -> list[dict]:
    html = html_path.read_text(encoding="utf-8")
    pattern = re.compile(
        r'<area id="([^"]+)" shape="([^"]+)" data-coords="([^"]+)"'
    )
    areas: list[dict] = []

    for match in pattern.finditer(html):
        area_id, shape, coords = match.group(1), match.group(2), match.group(3)
        if area_id not in area_data:
            continue
        geom = parse_area_geometry(shape, coords)
        if not geom:
            continue
        data = area_data[area_id]
        log2 = data["log2"]
        areas.append(
            {
                "id": area_id,
                "symbols": data["symbols"],
                "entries": data["entries"],
                "log2": log2,
                "abs": abs(log2),
                "cx": geom["cx"],
                "cy": geom["cy"],
                "w": geom["w"],
                "h": geom["h"],
            }
        )

    return areas


def render_html(areas: list[dict], map_w: float, map_h: float) -> str:
    values = [a["log2"] for a in areas]
    log2_min = min(values)
    log2_max = max(values)
    abs_max = max(abs(log2_min), abs(log2_max))
    payload = json.dumps(areas, ensure_ascii=False)

    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>map05171 — log2 fold change 3D isométrico</title>
<style>
* {{ box-sizing: border-box; }}
body {{
	margin: 0;
	font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
	background: #1a1f2e;
	color: #eee;
	overflow: hidden;
}}
#ui {{
	position: fixed;
	top: 0;
	left: 0;
	right: 0;
	z-index: 10;
	padding: 12px 16px;
	background: linear-gradient(180deg, rgba(26,31,46,0.95) 70%, transparent);
	pointer-events: none;
}}
#ui h1 {{
	margin: 0 0 6px 0;
	font-size: 18px;
	font-weight: 600;
}}
#ui p {{
	margin: 0 0 10px 0;
	font-size: 13px;
	color: #aab;
	max-width: 720px;
}}
.legend {{
	display: flex;
	gap: 20px;
	flex-wrap: wrap;
	font-size: 12px;
}}
.legend-item {{
	display: flex;
	align-items: center;
	gap: 6px;
}}
.swatch {{
	width: 14px;
	height: 14px;
	border-radius: 2px;
	border: 1px solid rgba(255,255,255,0.3);
}}
.swatch.pos {{ background: #b2182b; }}
.swatch.neg {{ background: #2166ac; }}
#tooltip {{
	position: fixed;
	display: none;
	padding: 8px 10px;
	background: rgba(0,0,0,0.85);
	border: 1px solid #555;
	border-radius: 4px;
	font-size: 12px;
	pointer-events: none;
	z-index: 20;
	white-space: nowrap;
}}
#canvas-host {{
	width: 100vw;
	height: 100vh;
}}
#hint {{
	position: fixed;
	bottom: 12px;
	right: 16px;
	font-size: 11px;
	color: #889;
	z-index: 10;
}}
</style>
</head>
<body>
<div id="ui">
	<h1>map05171 — barras 3D isométricas (log2 fold change)</h1>
	<p>Altura da barra = módulo |log2FC|. Vermelho = positivo · Azul = negativo. Dados: silver_pathway_areasv1.csv ({len(areas)} áreas).</p>
	<div class="legend">
		<div class="legend-item"><span class="swatch pos"></span> log2FC &gt; 0 (até {log2_max:.2f})</div>
		<div class="legend-item"><span class="swatch neg"></span> log2FC &lt; 0 (até {log2_min:.2f})</div>
	</div>
</div>
<div id="tooltip"></div>
<div id="hint">Arraste para girar · scroll para zoom</div>
<div id="canvas-host"></div>

<script type="importmap">
{{
  "imports": {{
    "three": "https://unpkg.com/three@0.160.0/build/three.module.js",
    "three/addons/": "https://unpkg.com/three@0.160.0/examples/jsm/"
  }}
}}
</script>
<script type="module">
import * as THREE from 'three';
import {{ OrbitControls }} from 'three/addons/controls/OrbitControls.js';

const AREAS = {payload};
const MAP_W = {map_w};
const MAP_H = {map_h};
const ABS_MAX = {abs_max};
const IMG = '{IMG_PATH}';

const PLANE_W = 40;
const PLANE_H = PLANE_W * (MAP_H / MAP_W);
const BAR_HEIGHT_SCALE = 6 / (ABS_MAX || 1);
const MIN_BAR = 0.08;

const host = document.getElementById('canvas-host');
const tooltip = document.getElementById('tooltip');

const scene = new THREE.Scene();
scene.background = new THREE.Color(0x1a1f2e);
scene.fog = new THREE.Fog(0x1a1f2e, 80, 140);

const camera = new THREE.PerspectiveCamera(45, window.innerWidth / window.innerHeight, 0.1, 500);
const ISO_DIST = 55;
camera.position.set(ISO_DIST, ISO_DIST * 0.82, ISO_DIST);

const renderer = new THREE.WebGLRenderer({{ antialias: true }});
renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
renderer.setSize(window.innerWidth, window.innerHeight);
renderer.shadowMap.enabled = true;
host.appendChild(renderer.domElement);

const controls = new OrbitControls(camera, renderer.domElement);
controls.enableDamping = true;
controls.dampingFactor = 0.06;
controls.target.set(0, 0, 0);
controls.minDistance = 20;
controls.maxDistance = 120;
controls.maxPolarAngle = Math.PI / 2.2;
controls.minPolarAngle = 0.35;
controls.update();

scene.add(new THREE.AmbientLight(0xffffff, 0.55));
const sun = new THREE.DirectionalLight(0xffffff, 0.85);
sun.position.set(30, 50, 20);
sun.castShadow = true;
sun.shadow.mapSize.set(1024, 1024);
scene.add(sun);
const fill = new THREE.DirectionalLight(0xaaccff, 0.35);
fill.position.set(-20, 15, -25);
scene.add(fill);

const isoGroup = new THREE.Group();
isoGroup.rotation.x = -Math.PI / 2;
scene.add(isoGroup);

const barsGroup = new THREE.Group();
isoGroup.add(barsGroup);

const textureLoader = new THREE.TextureLoader();
textureLoader.load(IMG, function (texture) {{
	texture.colorSpace = THREE.SRGBColorSpace;
	texture.anisotropy = renderer.capabilities.getMaxAnisotropy();

	const planeGeo = new THREE.PlaneGeometry(PLANE_W, PLANE_H);
	const planeMat = new THREE.MeshStandardMaterial({{
		map: texture,
		roughness: 0.92,
		metalness: 0.02,
	}});
	const plane = new THREE.Mesh(planeGeo, planeMat);
	plane.receiveShadow = true;
	isoGroup.add(plane);

	const edgeGeo = new THREE.EdgesGeometry(planeGeo);
	const edge = new THREE.LineSegments(
		edgeGeo,
		new THREE.LineBasicMaterial({{ color: 0x334455, transparent: true, opacity: 0.5 }})
	);
	isoGroup.add(edge);

	buildBars();
}});

function mapX(cx) {{
	return (cx / MAP_W - 0.5) * PLANE_W;
}}
function mapY(cy) {{
	return (0.5 - cy / MAP_H) * PLANE_H;
}}

function formatTooltip(area) {{
	if (area.entries.length === 1) {{
		var e = area.entries[0];
		return (
			'<strong>' + e.symbol + '</strong><br>' +
			'log2FC: ' + e.log2.toFixed(3) + '<br>' +
			'|log2FC|: ' + Math.abs(e.log2).toFixed(3)
		);
	}}
	return area.entries.map(function (e) {{
		return '<strong>' + e.symbol + '</strong>: ' + e.log2.toFixed(3);
	}}).join('<br>');
}}

function colorForLog2(value) {{
	return value >= 0 ? 0xb2182b : 0x2166ac;
}}

const barMeshes = [];

function buildBars() {{
	const sorted = AREAS.slice().sort(function (a, b) {{ return a.abs - b.abs; }});

	sorted.forEach(function (area) {{
		const barW = Math.max(0.15, Math.min(0.55, (area.w / MAP_W) * PLANE_W * 0.85));
		const barD = Math.max(0.15, Math.min(0.55, (area.h / MAP_H) * PLANE_H * 0.85));
		const height = Math.max(MIN_BAR, area.abs * BAR_HEIGHT_SCALE);

		const geo = new THREE.BoxGeometry(barW, barD, height);
		geo.translate(0, 0, height / 2);

		const mat = new THREE.MeshStandardMaterial({{
			color: colorForLog2(area.log2),
			roughness: 0.45,
			metalness: 0.08,
		}});
		const mesh = new THREE.Mesh(geo, mat);
		mesh.position.set(mapX(area.cx), mapY(area.cy), 0.01);
		mesh.castShadow = true;
		mesh.receiveShadow = true;
		mesh.userData = area;
		barsGroup.add(mesh);
		barMeshes.push(mesh);
	}});
}}

const raycaster = new THREE.Raycaster();
const pointer = new THREE.Vector2();

function onPointerMove(event) {{
	pointer.x = (event.clientX / window.innerWidth) * 2 - 1;
	pointer.y = -(event.clientY / window.innerHeight) * 2 + 1;
	raycaster.setFromCamera(pointer, camera);
	const hits = raycaster.intersectObjects(barMeshes, false);
	if (hits.length) {{
		const a = hits[0].object.userData;
		tooltip.style.display = 'block';
		tooltip.style.left = (event.clientX + 12) + 'px';
		tooltip.style.top = (event.clientY + 12) + 'px';
		tooltip.innerHTML = formatTooltip(a);
		renderer.domElement.style.cursor = 'pointer';
	}} else {{
		tooltip.style.display = 'none';
		renderer.domElement.style.cursor = 'grab';
	}}
}}

renderer.domElement.addEventListener('pointermove', onPointerMove);

function onResize() {{
	camera.aspect = window.innerWidth / window.innerHeight;
	camera.updateProjectionMatrix();
	renderer.setSize(window.innerWidth, window.innerHeight);
}}
window.addEventListener('resize', onResize);

function animate() {{
	requestAnimationFrame(animate);
	controls.update();
	renderer.render(scene, camera);
}}
animate();
</script>
</body>
</html>
"""


def main() -> None:
    area_data = load_area_data(CSV_PATH)
    map_w, map_h = load_map_bounds(PATHWAY_HTML)
    areas = load_areas_from_html(PATHWAY_HTML, area_data)
    html = render_html(areas, map_w, map_h)
    OUTPUT.write_text(html, encoding="utf-8")
    print(f"Gerado {OUTPUT} ({len(areas)} barras 3D, mapa {map_w:.0f}×{map_h:.0f})")


if __name__ == "__main__":
    main()
