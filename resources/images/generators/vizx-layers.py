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

STRIPE_W = 10   # full-height accent stripe width on layer cards
BAND_H   = 12   # top band height on Compositor


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
    # -----------------------------------------------------------------------
    # Layout constants — compute card positions up-front so we can build
    # all <defs> in one block at the top of the SVG.
    # -----------------------------------------------------------------------
    LY_X   = PAD
    LY_W   = 220
    LY_TOP = 102
    LY_H   = 64
    LY_GAP = 14

    layer_defs = [
        ("vector_layer(data)",          ACCENT_BLUE,   TINT_BLUE,   "GeoDataFrame / Spark DF / WKT"),
        ("raster_layer(data)",          ACCENT_ORANGE, TINT_ORANGE, "Path / bytes / ndarray"),
        ("grid_layer(data, grid_system=)", ACCENT_TEAL, TINT_TEAL, "H3 / BNG / Quadbin cell ids"),
        ("pmtiles_layer(data)",         ACCENT_VIOLET, TINT_VIOLET, ".pmtiles bytes, path, or URL"),
    ]

    # Pre-compute card Y positions
    card_ys = [LY_TOP + i * (LY_H + LY_GAP) for i in range(len(layer_defs))]

    # Compositor geometry (needed for clipPath definition)
    COMP_X  = LY_X + LY_W + 80
    COMP_W  = 180
    COMP_CY = LY_TOP + (len(layer_defs) * LY_H + (len(layer_defs) - 1) * LY_GAP) // 2
    COMP_H  = 110
    COMP_Y  = COMP_CY - COMP_H // 2

    # Ladder geometry
    LADDER_X   = COMP_X + COMP_W + 70
    LADDER_W   = 280
    LADDER_TOP = 102
    RUNG_H     = 70
    RUNG_GAP   = 12

    rungs_data = [
        ("URL stream", "FetchSource -- range-read (any size)", ACCENT_GREEN,  TINT_GREEN,  "indefinite size"),
        ("Embed",      "Embed inline (base64 FileSource)",     ACCENT_BLUE,   TINT_BLUE,   "no server needed"),
        ("Simplify",   "tippecanoe -> budget PMTiles",         ACCENT_ORANGE, TINT_ORANGE, "scale to budget"),
        ("Static",     "plot_static (matplotlib / PNG)",       C_MUTED,       "#F1F4F8",   "always works"),
    ]
    rung_ys = [LADDER_TOP + i * (RUNG_H + RUNG_GAP) for i in range(len(rungs_data))]
    rung_centers_y = [ry + RUNG_H // 2 for ry in rung_ys]

    # Output geometry
    OUT_X          = LADDER_X + LADDER_W + 70
    OUT_W          = 200
    OUT_Y_INTER    = LADDER_TOP
    OUT_H_INTER    = 100
    STATIC_GAP     = 24
    OUT_Y_STATIC   = OUT_Y_INTER + OUT_H_INTER + STATIC_GAP
    OUT_H_STATIC   = 80

    # Group box padding
    GRP_PAD = 8

    # -----------------------------------------------------------------------
    # Build SVG — single top-level <defs> with all clipPaths
    # -----------------------------------------------------------------------
    parts = []
    parts.append(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {CANVAS_W} {CANVAS_H}" '
                 f'width="{CANVAS_W}" height="{CANVAS_H}" '
                 f'style="font-family:{SANS};">')

    # All defs in one block
    defs_parts = ['''<defs>
  <filter id="sh" x="-5%" y="-5%" width="110%" height="115%">
    <feDropShadow dx="0" dy="2" stdDeviation="5" flood-color="#0F1B2A" flood-opacity="0.07"/>
  </filter>
  <linearGradient id="bg" x1="0" y1="0" x2="0" y2="1">
    <stop offset="0" stop-color="#FAFBFC"/>
    <stop offset="1" stop-color="#F1F4F8"/>
  </linearGradient>''']

    # clipPaths for the 4 layer card stripes (fix a — full-height accent stripe)
    for i, cy in enumerate(card_ys):
        defs_parts.append(
            f'  <clipPath id="stripe_clip_{i}">'
            f'<rect x="{LY_X}" y="{cy}" rx="8" ry="8" width="{LY_W}" height="{LY_H}"/>'
            f'</clipPath>'
        )

    # clipPath for Compositor top band (fix b — full-width top band)
    defs_parts.append(
        f'  <clipPath id="comp_top_clip">'
        f'<rect x="{COMP_X}" y="{COMP_Y}" rx="14" ry="14" width="{COMP_W}" height="{COMP_H}"/>'
        f'</clipPath>'
    )

    defs_parts.append('</defs>')
    parts.append("\n".join(defs_parts))

    # Background
    parts.append(f'<rect x="0" y="0" width="{CANVAS_W}" height="{CANVAS_H}" fill="url(#bg)"/>')

    # -----------------------------------------------------------------------
    # Header
    # -----------------------------------------------------------------------
    parts.append(text(CANVAS_W // 2, 46, "VizX Multi-Layer Compositor",
                      size=26, weight=800, fill=C_INK))
    parts.append(text(CANVAS_W // 2, 72,
                      "Layer types  ->  unified renderer  ->  embed-size ladder  ->  output",
                      size=14, fill=C_MUTED))

    # -----------------------------------------------------------------------
    # Left panel: Layer types (4 cards stacked)  [fix a: full-height stripe]
    # -----------------------------------------------------------------------
    layer_card_right_centers = []

    for i, (fn, accent, tint, desc) in enumerate(layer_defs):
        cy = card_ys[i]
        # Card background
        parts.append(f'<rect x="{LY_X}" y="{cy}" rx="8" ry="8" width="{LY_W}" height="{LY_H}" '
                     f'fill="{tint}" stroke="{accent}" stroke-width="1.5" filter="url(#sh)"/>')
        # Full-height accent stripe clipped to rounded card corners
        parts.append(f'<rect x="{LY_X}" y="{cy}" width="{STRIPE_W}" height="{LY_H}" '
                     f'fill="{accent}" clip-path="url(#stripe_clip_{i})"/>')
        # Text starts after stripe
        tx = LY_X + STRIPE_W + (LY_W - STRIPE_W) // 2
        parts.append(text(tx, cy + 22, fn, size=11, weight=700, fill=accent, family=MONO))
        parts.append(text(tx, cy + 40, desc, size=10, fill=C_MUTED))
        layer_card_right_centers.append((LY_X + LY_W, cy + LY_H // 2))

    # Dashed group box around all 4 layer cards (single connector group)
    grp_top  = card_ys[0] - GRP_PAD
    grp_bot  = card_ys[-1] + LY_H + GRP_PAD
    grp_h    = grp_bot - grp_top
    grp_mid_y = grp_top + grp_h // 2
    grp_right = LY_X + LY_W + GRP_PAD

    parts.append(f'<rect x="{LY_X - GRP_PAD}" y="{grp_top}" rx="12" ry="12" '
                 f'width="{LY_W + GRP_PAD * 2}" height="{grp_h}" '
                 f'fill="none" stroke="{C_MUTED3}" stroke-width="1.5" stroke-dasharray="5,4"/>')

    # -----------------------------------------------------------------------
    # Center: compositor box  [fix b: full-width top band]
    # -----------------------------------------------------------------------
    parts.append(rect(COMP_X, COMP_Y, COMP_W, COMP_H, fill="#FFFFFF", stroke=C_BORDER, r=14))

    # Full-width top band clipped to compositor's rounded rect
    parts.append(f'<rect x="{COMP_X}" y="{COMP_Y}" width="{COMP_W}" height="{BAND_H}" '
                 f'fill="{C_INK}" clip-path="url(#comp_top_clip)"/>')

    parts.append(text(COMP_X + COMP_W // 2, COMP_Y + 38, "Compositor",
                      size=16, weight=800, fill=C_INK))
    parts.append(text(COMP_X + COMP_W // 2, COMP_Y + 56, "plot_static /",
                      size=12, fill=C_MUTED))
    parts.append(text(COMP_X + COMP_W // 2, COMP_Y + 72, "plot_interactive",
                      size=12, fill=C_MUTED))
    parts.append(text(COMP_X + COMP_W // 2, COMP_Y + 94, "Layers drawn in list order",
                      size=9, fill=C_MUTED3))

    # Single arrow: layer group → compositor
    parts.append(arrow_h(grp_right + 2, grp_mid_y, COMP_X, color=ACCENT_BLUE))

    # -----------------------------------------------------------------------
    # Center-right: embed-size ladder  [fix c: no leading numbers in labels]
    # -----------------------------------------------------------------------
    for i, (label, desc, accent, tint, note) in enumerate(rungs_data):
        ry = rung_ys[i]
        parts.append(rect(LADDER_X, ry, LADDER_W, RUNG_H, fill=tint, stroke=accent, r=10))
        # Number badge
        bsize = 28
        parts.append(f'<rect x="{LADDER_X+10}" y="{ry+10}" rx="6" ry="6" '
                     f'width="{bsize}" height="{bsize}" fill="{accent}"/>')
        parts.append(text(LADDER_X + 10 + bsize // 2, ry + 10 + bsize // 2 + 4,
                          str(i + 1), size=13, weight=800, fill="#FFFFFF"))
        # Label text only (no "N. " prefix) — fix c
        parts.append(text(LADDER_X + 50, ry + 26, label, size=12, weight=700,
                          fill=accent, anchor="start"))
        parts.append(text(LADDER_X + 50, ry + 44, desc, size=10, fill=C_MUTED, anchor="start"))
        # Note pill
        pw = int(len(note) * 6.6) + 16
        parts.append(f'<rect x="{LADDER_X+LADDER_W-pw-8}" y="{ry+RUNG_H-22}" '
                     f'rx="9" width="{pw}" height="18" fill="{accent}" fill-opacity="0.15" '
                     f'stroke="{accent}" stroke-width="0.8"/>')
        parts.append(text(LADDER_X + LADDER_W - pw // 2 - 8, ry + RUNG_H - 10,
                          note, size=9, fill=accent, family=SANS))

    # Vertical dashed "else" connectors between rungs
    for i in range(len(rungs_data) - 1):
        y1    = rung_ys[i] + RUNG_H
        y2    = y1 + RUNG_GAP
        mid_x = LADDER_X - 22
        parts.append(f'<line x1="{mid_x}" y1="{y1}" x2="{mid_x}" y2="{y2}" '
                     f'stroke="{C_MUTED3}" stroke-width="1.5" stroke-dasharray="3,3"/>')
        parts.append(text(mid_x, y1 + RUNG_GAP // 2 + 4, "else", size=9, fill=C_MUTED3))

    # Single arrow: compositor → ladder (enter at left-center of ladder)
    parts.append(arrow_h(COMP_X + COMP_W + 6, COMP_CY, LADDER_X, color=C_INK))

    # -----------------------------------------------------------------------
    # Right panel: output boxes + grouped connector boxes
    # -----------------------------------------------------------------------
    # plot_interactive output
    parts.append(rect(OUT_X, OUT_Y_INTER, OUT_W, OUT_H_INTER,
                      fill=TINT_VIOLET, stroke=ACCENT_VIOLET, r=10))
    parts.append(text(OUT_X + OUT_W // 2, OUT_Y_INTER + 26, "plot_interactive",
                      size=14, weight=700, fill=ACCENT_VIOLET, family=MONO))
    parts.append(text(OUT_X + OUT_W // 2, OUT_Y_INTER + 46, "MapLibre GL HTML",
                      size=11, fill=C_MUTED))
    parts.append(text(OUT_X + OUT_W // 2, OUT_Y_INTER + 62, "self-contained, no server",
                      size=10, fill=C_MUTED3))
    parts.append(text(OUT_X + OUT_W // 2, OUT_Y_INTER + 78, "Serverless / classic DBR",
                      size=10, fill=C_MUTED3))

    # plot_static output
    parts.append(rect(OUT_X, OUT_Y_STATIC, OUT_W, OUT_H_STATIC,
                      fill=TINT_BLUE, stroke=ACCENT_BLUE, r=10))
    parts.append(text(OUT_X + OUT_W // 2, OUT_Y_STATIC + 26, "plot_static",
                      size=14, weight=700, fill=ACCENT_BLUE, family=MONO))
    parts.append(text(OUT_X + OUT_W // 2, OUT_Y_STATIC + 46, "matplotlib Axes",
                      size=11, fill=C_MUTED))
    parts.append(text(OUT_X + OUT_W // 2, OUT_Y_STATIC + 62, "GitHub-renderable PNG",
                      size=10, fill=C_MUTED3))

    # Dashed group box around rungs 1-3 (interactive group)
    inter_grp_top = rung_ys[0] - GRP_PAD
    inter_grp_bot = rung_ys[2] + RUNG_H + GRP_PAD
    inter_grp_h   = inter_grp_bot - inter_grp_top
    inter_grp_mid = inter_grp_top + inter_grp_h // 2
    parts.append(f'<rect x="{LADDER_X - GRP_PAD}" y="{inter_grp_top}" rx="12" ry="12" '
                 f'width="{LADDER_W + GRP_PAD * 2}" height="{inter_grp_h}" '
                 f'fill="none" stroke="{ACCENT_VIOLET}" stroke-width="1.2" '
                 f'stroke-dasharray="5,4"/>')

    # Single arrow: rungs 1-3 group right edge → plot_interactive (mid-y of group)
    inter_arrow_x = LADDER_X + LADDER_W + GRP_PAD + 2
    inter_target_y = OUT_Y_INTER + OUT_H_INTER // 2
    # Straight horizontal at group mid-y, then we rely on vertical alignment being close.
    # Use a bent elbow: horizontal to OUT_X at inter_grp_mid level, but output may not
    # align. Simplest readable: draw at rung_centers_y[1] (middle rung's center ~ group mid).
    parts.append(arrow_h(inter_arrow_x, inter_grp_mid, OUT_X, color=ACCENT_VIOLET))

    # Single arrow: rung 4 right edge → plot_static
    parts.append(arrow_h(LADDER_X + LADDER_W + 6, rung_centers_y[3],
                         OUT_X, color=ACCENT_BLUE))

    # -----------------------------------------------------------------------
    # Bottom strip: audit_layers / dry_run proactive check
    # -----------------------------------------------------------------------
    STRIP_Y = LADDER_TOP + len(rungs_data) * (RUNG_H + RUNG_GAP) + 10
    STRIP_H = 60
    STRIP_W = LADDER_X + LADDER_W - LY_X
    parts.append(rect(LY_X, STRIP_Y, STRIP_W, STRIP_H, fill="#FFFFF0",
                      stroke="#C8B800", r=10, stroke_w=1))
    parts.append(text(LY_X + 20, STRIP_Y + 22, "Proactive check -- run before rendering:",
                      size=12, weight=700, fill="#7A6800", anchor="start"))
    parts.append(text(LY_X + 20, STRIP_Y + 44,
                      "audit_layers(layers)  or  plot_interactive(layers, dry_run=True)",
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
