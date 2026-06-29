#!/usr/bin/env python3
"""Generate the VizX multi-layer + decision-ladder diagram.

Re-render after editing:

    python3 resources/images/generators/vizx-layers.py
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
        --headless --disable-gpu --hide-scrollbars \
        --force-device-scale-factor=2 --window-size=1500,820 \
        --screenshot=resources/images/diagrams/vizx/vizx-layers.png \
        resources/images/diagrams/vizx/vizx-layers.svg
    python3 -c "from PIL import Image, ImageChops; \
      p='resources/images/diagrams/vizx/vizx-layers.png'; \
      img=Image.open(p).convert('RGB'); \
      bb=ImageChops.difference(img, Image.new('RGB',img.size,(255,255,255))).getbbox(); \
      img.crop(bb).save(p)"
"""
import os

# Palette (matching helios.py)
C_INK    = "#0F1B2A"
C_MUTED  = "#3F4D5E"
C_MUTED3 = "#7A8794"
C_BORDER = "#E5E7EB"

ACCENT_BLUE   = "#1F6FB5"
TINT_BLUE     = "#E3EEF8"
ACCENT_ORANGE = "#E04E2A"
TINT_ORANGE   = "#FCE9E2"
ACCENT_TEAL   = "#0F8E8B"
TINT_TEAL     = "#D5ECEC"
ACCENT_VIOLET = "#6B4FA0"
TINT_VIOLET   = "#EDE8F5"
ACCENT_GREEN  = "#1E8E3E"
TINT_GREEN    = "#DFF0E5"

CANVAS_W = 1480
CANVAS_H = 720
PAD = 32

MONO = "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace"
SANS = "Inter, -apple-system, system-ui, sans-serif"


def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def rect(x, y, w, h, *, fill="#FFF", stroke=C_BORDER, r=10, stroke_w=1):
    return (f'<rect x="{x}" y="{y}" rx="{r}" ry="{r}" width="{w}" height="{h}" '
            f'fill="{fill}" stroke="{stroke}" stroke-width="{stroke_w}"/>')


def text(x, y, s, *, size=13, weight=400, fill=C_INK, family=SANS, anchor="middle"):
    return (f'<text x="{x}" y="{y}" font-family="{family}" font-size="{size}" '
            f'font-weight="{weight}" fill="{fill}" text-anchor="{anchor}">{esc(s)}</text>')


def arrow_h(x1, y, x2, *, color=C_MUTED3, head=9):
    return (f'<line x1="{x1}" y1="{y}" x2="{x2-head}" y2="{y}" '
            f'stroke="{color}" stroke-width="2" stroke-linecap="round"/>'
            f'<polygon points="{x2},{y} {x2-head},{y-head//2} {x2-head},{y+head//2}" '
            f'fill="{color}"/>')


def render():
    parts = []
    parts.append(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {CANVAS_W} {CANVAS_H}" '
                 f'width="{CANVAS_W}" height="{CANVAS_H}" '
                 f'style="font-family:{SANS};">')
    parts.append('''<defs>
  <filter id="sh" x="-5%" y="-5%" width="110%" height="115%">
    <feDropShadow dx="0" dy="2" stdDeviation="5" flood-color="#0F1B2A" flood-opacity="0.07"/>
  </filter>
  <linearGradient id="bg" x1="0" y1="0" x2="0" y2="1">
    <stop offset="0" stop-color="#FAFBFC"/>
    <stop offset="1" stop-color="#F1F4F8"/>
  </linearGradient>
</defs>''')
    parts.append(f'<rect x="0" y="0" width="{CANVAS_W}" height="{CANVAS_H}" fill="url(#bg)"/>')

    # -----------------------------------------------------------------------
    # Header
    # -----------------------------------------------------------------------
    parts.append(text(CANVAS_W // 2, 46, "VizX Multi-Layer Compositor",
                      size=26, weight=800, fill=C_INK))
    parts.append(text(CANVAS_W // 2, 72, "Layer types  ->  unified renderer  ->  embed-size ladder  ->  output",
                      size=14, fill=C_MUTED))

    # -----------------------------------------------------------------------
    # Left panel: Layer types (4 cards stacked)
    # -----------------------------------------------------------------------
    LY_X = PAD
    LY_W = 220
    LY_TOP = 102
    LY_H = 64
    LY_GAP = 14

    layer_defs = [
        ("vector_layer(data)", ACCENT_BLUE, TINT_BLUE, "GeoDataFrame / Spark DF / WKT"),
        ("raster_layer(data)", ACCENT_ORANGE, TINT_ORANGE, "Path / bytes / ndarray"),
        ("grid_layer(data, grid_system=)", ACCENT_TEAL, TINT_TEAL, "H3 / BNG / Quadbin cell ids"),
        ("pmtiles_layer(data)", ACCENT_VIOLET, TINT_VIOLET, ".pmtiles bytes, path, or URL"),
    ]

    layer_card_centers = []
    for i, (fn, accent, tint, desc) in enumerate(layer_defs):
        cy = LY_TOP + i * (LY_H + LY_GAP)
        parts.append(f'<rect x="{LY_X}" y="{cy}" rx="8" ry="8" width="{LY_W}" height="{LY_H}" '
                     f'fill="{tint}" stroke="{accent}" stroke-width="1.5" filter="url(#sh)"/>')
        # Color stripe on left edge
        parts.append(f'<rect x="{LY_X}" y="{cy}" rx="8" ry="8" width="6" height="{LY_H}" fill="{accent}"/>')
        parts.append(f'<rect x="{LY_X+6}" y="{cy}" width="6" height="{LY_H}" fill="{accent}"/>')
        parts.append(text(LY_X + LY_W // 2 + 6, cy + 22, fn,
                          size=11, weight=700, fill=accent, family=MONO))
        parts.append(text(LY_X + LY_W // 2 + 6, cy + 40, desc,
                          size=10, fill=C_MUTED))
        layer_card_centers.append((LY_X + LY_W, cy + LY_H // 2))

    # -----------------------------------------------------------------------
    # Center: compositor box
    # -----------------------------------------------------------------------
    COMP_X = LY_X + LY_W + 60
    COMP_W = 180
    # Center the compositor box on the average Y of the 4 layer cards
    all_ys = [y for _, y in layer_card_centers]
    COMP_CY = int(sum(all_ys) / len(all_ys))
    COMP_H = 110
    COMP_Y = COMP_CY - COMP_H // 2

    parts.append(rect(COMP_X, COMP_Y, COMP_W, COMP_H, fill="#FFFFFF", stroke=C_BORDER, r=14))
    parts.append(f'<rect x="{COMP_X}" y="{COMP_Y}" rx="14" ry="14" width="{COMP_W}" height="6" fill="{C_INK}"/>')
    parts.append(f'<rect x="{COMP_X}" y="{COMP_Y+6}" width="{COMP_W}" height="6" fill="{C_INK}"/>')
    parts.append(text(COMP_X + COMP_W // 2, COMP_Y + 38, "Compositor",
                      size=16, weight=800, fill=C_INK))
    parts.append(text(COMP_X + COMP_W // 2, COMP_Y + 56, "plot_static /",
                      size=12, fill=C_MUTED))
    parts.append(text(COMP_X + COMP_W // 2, COMP_Y + 72, "plot_interactive",
                      size=12, fill=C_MUTED))
    parts.append(text(COMP_X + COMP_W // 2, COMP_Y + 94, "Layers drawn in list order",
                      size=9, fill=C_MUTED3))

    # Arrows from each layer card to compositor
    for lx, ly in layer_card_centers:
        parts.append(arrow_h(lx + 6, ly, COMP_X, color=ACCENT_BLUE))

    # -----------------------------------------------------------------------
    # Center-right: embed-size ladder
    # -----------------------------------------------------------------------
    LADDER_X = COMP_X + COMP_W + 60
    LADDER_W = 280
    LADDER_TOP = 102
    RUNG_H = 70
    RUNG_GAP = 12

    rungs = [
        ("1  http(s):// URL", "FetchSource -- range-read (any size)", ACCENT_GREEN, TINT_GREEN, "indefinite size"),
        ("2  fits <= 64 MB",  "Embed inline (base64 FileSource)",   ACCENT_BLUE,   TINT_BLUE,   "no server needed"),
        ("3  simplify_tiles_spec=", "tippecanoe -> budget PMTiles",  ACCENT_ORANGE, TINT_ORANGE,  "scale to budget"),
        ("4  static fallback",  "plot_static (matplotlib / PNG)",   C_MUTED,       "#F1F4F8",    "always works"),
    ]

    rung_centers_y = []
    for i, (label, desc, accent, tint, note) in enumerate(rungs):
        ry = LADDER_TOP + i * (RUNG_H + RUNG_GAP)
        parts.append(rect(LADDER_X, ry, LADDER_W, RUNG_H, fill=tint, stroke=accent, r=10))
        # Number badge
        bsize = 28
        parts.append(f'<rect x="{LADDER_X+10}" y="{ry+10}" rx="6" ry="6" '
                     f'width="{bsize}" height="{bsize}" fill="{accent}"/>')
        parts.append(text(LADDER_X + 10 + bsize // 2, ry + 10 + bsize // 2 + 4,
                          str(i + 1), size=13, weight=800, fill="#FFFFFF"))
        parts.append(text(LADDER_X + 50, ry + 26, label, size=12, weight=700,
                          fill=accent, anchor="start"))
        parts.append(text(LADDER_X + 50, ry + 44, desc, size=10, fill=C_MUTED, anchor="start"))
        # Note pill on right
        pill_txt = note
        pw = int(len(pill_txt) * 6.6) + 16
        parts.append(f'<rect x="{LADDER_X+LADDER_W-pw-8}" y="{ry+RUNG_H-22}" '
                     f'rx="9" width="{pw}" height="18" fill="{accent}" fill-opacity="0.15" '
                     f'stroke="{accent}" stroke-width="0.8"/>')
        parts.append(text(LADDER_X + LADDER_W - pw // 2 - 8, ry + RUNG_H - 10,
                          pill_txt, size=9, fill=accent, family=SANS))
        rung_centers_y.append(ry + RUNG_H // 2)

    # Vertical dashed connectors between rungs (fallback direction)
    for i in range(len(rungs) - 1):
        y1 = LADDER_TOP + i * (RUNG_H + RUNG_GAP) + RUNG_H
        y2 = y1 + RUNG_GAP
        mid_x = LADDER_X - 22
        parts.append(f'<line x1="{mid_x}" y1="{y1}" x2="{mid_x}" y2="{y2}" '
                     f'stroke="{C_MUTED3}" stroke-width="1.5" stroke-dasharray="3,3"/>')
        parts.append(text(mid_x, y1 + RUNG_GAP // 2 + 4, "else", size=9, fill=C_MUTED3))

    # Arrow from compositor to ladder
    parts.append(arrow_h(COMP_X + COMP_W + 6, COMP_CY, LADDER_X, color=C_INK))

    # -----------------------------------------------------------------------
    # Right panel: outputs
    # -----------------------------------------------------------------------
    OUT_X = LADDER_X + LADDER_W + 60
    OUT_W = 200
    OUT_Y_STATIC = LADDER_TOP + 50
    OUT_Y_INTER  = LADDER_TOP + 200

    # Static output
    parts.append(rect(OUT_X, OUT_Y_STATIC, OUT_W, 80, fill=TINT_BLUE, stroke=ACCENT_BLUE, r=10))
    parts.append(text(OUT_X + OUT_W // 2, OUT_Y_STATIC + 26, "plot_static",
                      size=14, weight=700, fill=ACCENT_BLUE, family=MONO))
    parts.append(text(OUT_X + OUT_W // 2, OUT_Y_STATIC + 46, "matplotlib Axes",
                      size=11, fill=C_MUTED))
    parts.append(text(OUT_X + OUT_W // 2, OUT_Y_STATIC + 62, "GitHub-renderable PNG",
                      size=10, fill=C_MUTED3))

    # Interactive output
    parts.append(rect(OUT_X, OUT_Y_INTER, OUT_W, 100, fill=TINT_VIOLET, stroke=ACCENT_VIOLET, r=10))
    parts.append(text(OUT_X + OUT_W // 2, OUT_Y_INTER + 26, "plot_interactive",
                      size=14, weight=700, fill=ACCENT_VIOLET, family=MONO))
    parts.append(text(OUT_X + OUT_W // 2, OUT_Y_INTER + 46, "MapLibre GL HTML",
                      size=11, fill=C_MUTED))
    parts.append(text(OUT_X + OUT_W // 2, OUT_Y_INTER + 62, "self-contained, no server",
                      size=10, fill=C_MUTED3))
    parts.append(text(OUT_X + OUT_W // 2, OUT_Y_INTER + 80, "Serverless / classic DBR",
                      size=10, fill=C_MUTED3))

    # Arrow from ladder rung 4 (static) to plot_static
    parts.append(arrow_h(LADDER_X + LADDER_W + 6, rung_centers_y[3],
                         OUT_X, color=ACCENT_BLUE))
    # Arrow from center of rungs 1-3 to plot_interactive
    mid_inter_y = (rung_centers_y[0] + rung_centers_y[1] + rung_centers_y[2]) // 3
    parts.append(arrow_h(LADDER_X + LADDER_W + 6, mid_inter_y,
                         OUT_X, color=ACCENT_VIOLET))

    # -----------------------------------------------------------------------
    # Bottom strip: audit_layers / dry_run proactive check
    # -----------------------------------------------------------------------
    STRIP_Y = LADDER_TOP + len(rungs) * (RUNG_H + RUNG_GAP) + 10
    STRIP_H = 60
    STRIP_W = LADDER_X + LADDER_W - LY_X
    parts.append(rect(LY_X, STRIP_Y, STRIP_W, STRIP_H, fill="#FFFFF0",
                      stroke="#C8B800", r=10, stroke_w=1))
    parts.append(text(LY_X + 20, STRIP_Y + 22, "Proactive check -- run before rendering:",
                      size=12, weight=700, fill="#7A6800", anchor="start"))
    code_txt = "audit_layers(layers)  or  plot_interactive(layers, dry_run=True)"
    parts.append(text(LY_X + 20, STRIP_Y + 44, code_txt,
                      size=11, fill="#7A6800", family=MONO, anchor="start"))

    # -----------------------------------------------------------------------
    # Footer
    # -----------------------------------------------------------------------
    FOOTER_Y = CANVAS_H - 30
    parts.append(text(CANVAS_W // 2, FOOTER_Y,
                      "databrickslabs/geobrix  .  gbx.vizx  .  MapLibre GL JS + pmtiles.js",
                      size=11, fill=C_MUTED3))

    parts.append("</svg>")
    return "\n".join(parts)


if __name__ == "__main__":
    here = os.path.dirname(os.path.abspath(__file__))
    out_dir = os.path.join(here, "..", "diagrams", "vizx")
    os.makedirs(out_dir, exist_ok=True)
    svg_path = os.path.join(out_dir, "vizx-layers.svg")
    with open(svg_path, "w") as f:
        f.write(render())
    print(f"wrote {svg_path}")
