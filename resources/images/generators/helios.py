#!/usr/bin/env python3
"""Generate the four Helios notebook conceptual diagrams.

One SVG per notebook (01–04). Each is a four-stage data→tile→PMTiles→view
pipeline diagram with a custom hero glyph per stage and a footer of GeoBrix /
Databricks function chips that the notebook actually uses.

Re-render after editing this script:

    python3 resources/images/generators/helios.py
    for n in 01 02 03 04; do
      # window-size is wider/taller than the SVG to absorb Chrome's default
      # body margin; the bbox-trim step below crops it back to SVG bounds.
      "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \\
          --headless --disable-gpu --hide-scrollbars \\
          --force-device-scale-factor=2 --window-size=1500,820 \\
          --screenshot=resources/images/diagrams/helios/helios-$n.png \\
          resources/images/diagrams/helios/helios-$n.svg
    done
    python3 -c "from PIL import Image, ImageChops; \\
      [Image.open(p).convert('RGB').crop(ImageChops.difference( \\
        Image.open(p).convert('RGB'), \\
        Image.new('RGB', Image.open(p).size, (255,255,255))).getbbox()).save(p) \\
       for p in [f'resources/images/diagrams/helios/helios-{n}.png' for n in ('01','02','03','04')]]"
"""
import math
import os
import sys
from dataclasses import dataclass, field
from textwrap import dedent

# --- Palette ------------------------------------------------------------------

C_INK     = "#0F1B2A"
C_INK_2   = "#1B3139"
C_MUTED   = "#3F4D5E"
C_MUTED_2 = "#5A6878"
C_MUTED_3 = "#7A8794"
C_BORDER  = "#E5E7EB"

# Per-notebook themes
THEMES = {
    1: {"accent": "#1F6FB5", "tint": "#E3EEF8"},   # blue   — vector engine
    2: {"accent": "#E04E2A", "tint": "#FCE9E2"},   # orange — raster basemap
    3: {"accent": "#0F8E8B", "tint": "#D5ECEC"},   # teal   — analytical core
    4: {"accent": "#6B4FA0", "tint": "#EDE8F5"},   # violet — distributed sharding
}

# --- Layout -------------------------------------------------------------------

CANVAS_W = 1480
CANVAS_H = 720
PAD      = 36

HEADER_H      = 96
STAGE_TOP_GAP = 28
STAGE_H       = 380
STAGE_GLYPH_H = 200
STAGE_LABEL_H = 30
STAGE_CAP_H   = 70
ARROW_W       = 56

FOOTER_TOP_GAP = 16
FOOTER_H       = 50

# --- Primitives ---------------------------------------------------------------

def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def text(x, y, s, *, size=13, weight=400, fill=C_INK,
         family="Inter, -apple-system, system-ui, sans-serif",
         anchor="start", letter_spacing=None):
    ls = f' letter-spacing="{letter_spacing}"' if letter_spacing else ""
    return (f'<text x="{x}" y="{y}" font-family="{family}" '
            f'font-size="{size}" font-weight="{weight}" fill="{fill}" '
            f'text-anchor="{anchor}"{ls}>{esc(s)}</text>')

def mono(x, y, s, *, size=13, weight=500, fill=C_INK, anchor="start"):
    return text(x, y, s, size=size, weight=weight, fill=fill, anchor=anchor,
                family="ui-monospace, SFMono-Regular, Menlo, Consolas, monospace")

def card(x, y, w, h, *, fill="#FFFFFF", stroke=C_BORDER, r=14, shadow=True):
    flt = ' filter="url(#card-shadow)"' if shadow else ""
    return (f'<rect x="{x}" y="{y}" rx="{r}" ry="{r}" width="{w}" height="{h}" '
            f'fill="{fill}" stroke="{stroke}" stroke-width="1"{flt}/>')

def top_stripe(x, y, w, color, *, r=14, h=5):
    return (f'<path d="M {x} {y + r} '
            f'A {r} {r} 0 0 1 {x + r} {y} '
            f'H {x + w - r} '
            f'A {r} {r} 0 0 1 {x + w} {y + r} '
            f'V {y + h} '
            f'H {x} Z" fill="{color}"/>')

def chip(x, y, txt, *, fg=C_INK, bg="#F1F4F8", border=None, mono_font=False, h=22):
    char_w = 7.0 if mono_font else 6.6
    pad_x = 12
    w = int(len(txt) * char_w) + pad_x * 2
    family = ("ui-monospace, SFMono-Regular, Menlo, Consolas, monospace"
              if mono_font else "Inter, -apple-system, system-ui, sans-serif")
    bs = f' stroke="{border}" stroke-width="1"' if border else ""
    svg = (f'<rect x="{x}" y="{y}" rx="{h/2:.0f}" ry="{h/2:.0f}" '
           f'width="{w}" height="{h}" fill="{bg}"{bs}/>'
           f'<text x="{x + w/2}" y="{y + h/2 + 4}" '
           f'text-anchor="middle" font-family="{family}" '
           f'font-size="12" font-weight="700" fill="{fg}">{esc(txt)}</text>')
    return svg, w

def arrow(x1, y, x2, *, color=C_MUTED_3, head=10):
    """Horizontal arrow from (x1, y) to (x2, y)."""
    return (f'<line x1="{x1}" y1="{y}" x2="{x2 - head}" y2="{y}" '
            f'stroke="{color}" stroke-width="2.5" stroke-linecap="round"/>'
            f'<polygon points="{x2},{y} {x2 - head},{y - head/1.5} '
            f'{x2 - head},{y + head/1.5}" fill="{color}"/>')

# --- Glyphs (each centered on cx, cy) ----------------------------------------

def g_building_footprints(cx, cy, color, tint):
    """City block of building footprint rectangles — Overture buildings glyph."""
    out = []
    # A small city block arrangement: 5 buildings of varying sizes
    buildings = [
        # (rel_x, rel_y, w, h)
        (-62, -44, 48, 52),
        (-4,  -44, 36, 36),
        (44,  -44, 32, 52),
        (-62,  20, 32, 38),
        (-18,   2, 54, 42),
        ( 44,  18, 32, 36),
    ]
    # Draw filled + stroked building footprints
    for (dx, dy, bw, bh) in buildings:
        x = cx + dx
        y = cy + dy
        out.append(
            f'<rect x="{x}" y="{y}" rx="3" ry="3" width="{bw}" height="{bh}" '
            f'fill="{tint}" stroke="{color}" stroke-width="2"/>'
        )
        # Add a small cross/+ in the center to imply an entry point
        mx = x + bw / 2
        my = y + bh / 2
        out.append(
            f'<line x1="{mx - 6}" y1="{my}" x2="{mx + 6}" y2="{my}" '
            f'stroke="{color}" stroke-width="1.4" stroke-opacity="0.5"/>'
            f'<line x1="{mx}" y1="{my - 6}" x2="{mx}" y2="{my + 6}" '
            f'stroke="{color}" stroke-width="1.4" stroke-opacity="0.5"/>'
        )
    return "".join(out)


def g_vector_tile_grid(cx, cy, color, tint):
    """A tile grid with vector polygon shapes inside — MVT encoding glyph."""
    out = []
    # Outer tile boundary
    tw, th = 140, 120
    tx = cx - tw / 2
    ty = cy - th / 2
    out.append(
        f'<rect x="{tx}" y="{ty}" rx="8" ry="8" width="{tw}" height="{th}" '
        f'fill="#FFFFFF" stroke="{color}" stroke-width="2.4"/>'
    )
    # Internal tile grid lines (2x2 subtiles)
    out.append(
        f'<line x1="{cx}" y1="{ty + 4}" x2="{cx}" y2="{ty + th - 4}" '
        f'stroke="{color}" stroke-width="1" stroke-opacity="0.3"/>'
        f'<line x1="{tx + 4}" y1="{cy}" x2="{tx + tw - 4}" y2="{cy}" '
        f'stroke="{color}" stroke-width="1" stroke-opacity="0.3"/>'
    )
    # Small polygon shapes inside each quadrant
    polys = [
        # top-left quadrant
        f'<polygon points="{cx-56},{cy-38} {cx-28},{cy-52} {cx-12},{cy-32} {cx-40},{cy-16}" '
        f'fill="{tint}" stroke="{color}" stroke-width="1.6"/>',
        # top-right quadrant
        f'<rect x="{cx+10}" y="{cy-50}" rx="2" width="36" height="28" '
        f'fill="{tint}" stroke="{color}" stroke-width="1.6"/>',
        # bottom-left quadrant
        f'<rect x="{cx-62}" y="{cy+8}" rx="2" width="42" height="28" '
        f'fill="{tint}" stroke="{color}" stroke-width="1.6"/>',
        # bottom-right quadrant
        f'<polygon points="{cx+14},{cy+10} {cx+46},{cy+6} {cx+50},{cy+36} {cx+12},{cy+40}" '
        f'fill="{tint}" stroke="{color}" stroke-width="1.6"/>',
    ]
    out.extend(polys)
    # Zoom-level badge
    out.append(
        f'<rect x="{cx + tw/2 - 28}" y="{ty - 16}" rx="8" width="28" height="16" '
        f'fill="{color}"/>'
        f'<text x="{cx + tw/2 - 14}" y="{ty - 4}" text-anchor="middle" '
        f'font-family="ui-monospace, Menlo, monospace" font-size="10" '
        f'font-weight="700" fill="#FFFFFF">z/x/y</text>'
    )
    return "".join(out)


def g_stacked_archive(cx, cy, color, tint):
    """Stacked-archive icon — PMTiles archive glyph."""
    out = []
    # Stack of 3 offset rectangles suggesting a layered archive
    layers = [
        (12, 20),   # back-most (bottom of stack)
        (6,  10),
        (0,   0),   # front (top)
    ]
    w, h = 120, 72
    for (dx, dy) in layers:
        x = cx - w / 2 + dx
        y = cy - h / 2 + dy - 16
        fill = tint if dx > 0 else "#FFFFFF"
        opacity = "0.7" if dx > 0 else "1.0"
        out.append(
            f'<rect x="{x}" y="{y}" rx="8" ry="8" width="{w}" height="{h}" '
            f'fill="{fill}" fill-opacity="{opacity}" '
            f'stroke="{color}" stroke-width="2"/>'
        )
    # Label on the top layer
    front_x = cx - w / 2
    front_y = cy - h / 2 - 16
    out.append(
        f'<rect x="{front_x}" y="{front_y}" width="{w}" height="20" '
        f'rx="8" ry="8" fill="{color}"/>'
        f'<rect x="{front_x}" y="{front_y + 10}" width="{w}" height="10" fill="{color}"/>'
        f'<text x="{cx}" y="{front_y + 14}" text-anchor="middle" '
        f'font-family="Inter, sans-serif" font-size="11" font-weight="800" '
        f'fill="#FFFFFF">.pmtiles</text>'
    )
    # Horizontal lines representing packed tile entries
    for i in range(3):
        lx = front_x + 10
        ly = front_y + 30 + i * 13
        out.append(
            f'<line x1="{lx}" y1="{ly}" x2="{front_x + w - 10}" y2="{ly}" '
            f'stroke="{color}" stroke-opacity="0.4" stroke-width="1.4"/>'
        )
    return "".join(out)


def g_map_pin(cx, cy, color, tint):
    """Map pin / location marker — the view / plot_pmtiles result glyph."""
    out = []
    # Pin body (teardrop shape via path)
    R = 36
    pin_top_y = cy - 60
    pin_tip_y = cy + 20
    # Circle part
    out.append(
        f'<circle cx="{cx}" cy="{pin_top_y + R}" r="{R}" '
        f'fill="{tint}" stroke="{color}" stroke-width="2.5"/>'
    )
    # Triangle tail
    out.append(
        f'<polygon points="{cx - 14},{pin_top_y + R * 1.6} '
        f'{cx + 14},{pin_top_y + R * 1.6} {cx},{pin_tip_y}" '
        f'fill="{tint}" stroke="{color}" stroke-width="2.5" '
        f'stroke-linejoin="round"/>'
    )
    # Inner dot
    out.append(
        f'<circle cx="{cx}" cy="{pin_top_y + R}" r="{R * 0.38:.1f}" '
        f'fill="{color}"/>'
    )
    # Shadow ellipse at base
    out.append(
        f'<ellipse cx="{cx}" cy="{pin_tip_y + 8}" rx="20" ry="6" '
        f'fill="{color}" fill-opacity="0.18"/>'
    )
    return "".join(out)


def g_aerial_swatch(cx, cy, color, tint):
    """Aerial imagery swatch — pixel grid with RGB-ish colors."""
    cells = 9
    s = 13
    span = cells * s
    x0 = cx - span / 2
    y0 = cy - span / 2
    out = [
        f'<rect x="{x0:.1f}" y="{y0:.1f}" width="{span}" height="{span}" '
        f'fill="{tint}" stroke="{color}" stroke-width="2"/>'
    ]
    # Pseudo aerial color field
    def aerial_color(r, c):
        # Create an interesting aerial-like pattern with greens/browns/grays
        seed = (r * 3 + c * 7 + 11) % 16
        palettes = [
            "#7DB87D", "#6AAA6A", "#8DC48D", "#5A9A5A",  # vegetation greens
            "#BFAA8E", "#B09070", "#C8B898", "#A89068",  # urban/soil tans
            "#B0B8C4", "#9AABB8", "#C0CAD4", "#A8B8C8",  # rooftops/roads
            "#7BA8D4", "#6898C4", "#8AB4E0", "#5888B8",  # water blues
        ]
        return palettes[seed]

    for r in range(cells):
        for c in range(cells):
            col = aerial_color(r, c)
            out.append(
                f'<rect x="{x0 + c*s:.1f}" y="{y0 + r*s:.1f}" '
                f'width="{s}" height="{s}" fill="{col}" fill-opacity="0.85"/>'
            )
    # Gridlines
    for i in range(1, cells):
        out.append(
            f'<line x1="{x0 + i*s:.1f}" y1="{y0:.1f}" '
            f'x2="{x0 + i*s:.1f}" y2="{y0 + span:.1f}" '
            f'stroke="#FFFFFF" stroke-width="0.6" stroke-opacity="0.5"/>'
        )
        out.append(
            f'<line x1="{x0:.1f}" y1="{y0 + i*s:.1f}" '
            f'x2="{x0 + span:.1f}" y2="{y0 + i*s:.1f}" '
            f'stroke="#FFFFFF" stroke-width="0.6" stroke-opacity="0.5"/>'
        )
    return "".join(out)


def g_webmercator_globe(cx, cy, color, tint):
    """Web-mercator globe with graticule lines."""
    out = []
    R = 62
    out.append(
        f'<circle cx="{cx}" cy="{cy}" r="{R}" '
        f'fill="{tint}" stroke="{color}" stroke-width="2.5"/>'
    )
    # Equator
    out.append(
        f'<line x1="{cx - R}" y1="{cy}" x2="{cx + R}" y2="{cy}" '
        f'stroke="{color}" stroke-width="1.6" stroke-opacity="0.5"/>'
    )
    # Prime meridian
    out.append(
        f'<line x1="{cx}" y1="{cy - R}" x2="{cx}" y2="{cy + R}" '
        f'stroke="{color}" stroke-width="1.6" stroke-opacity="0.5"/>'
    )
    # Latitude arcs (approximate ellipses for 30/60 deg)
    for lat_frac in [0.5, 0.86]:
        ry = int(R * 0.25)
        half_w = int(R * math.sqrt(1 - lat_frac ** 2))
        for sign in [-1, 1]:
            arc_cy = int(cy + sign * R * lat_frac)
            out.append(
                f'<ellipse cx="{cx}" cy="{arc_cy}" '
                f'rx="{half_w}" ry="{ry}" '
                f'fill="none" stroke="{color}" stroke-width="1.2" stroke-opacity="0.35"/>'
            )
    # Meridian arcs at 60 deg
    for lon_frac in [0.5, 0.86]:
        rx = int(R * 0.22)
        half_h = int(R * math.sqrt(1 - lon_frac ** 2))
        for sign in [-1, 1]:
            arc_cx = int(cx + sign * R * lon_frac)
            out.append(
                f'<ellipse cx="{arc_cx}" cy="{cy}" '
                f'rx="{rx}" ry="{half_h}" '
                f'fill="none" stroke="{color}" stroke-width="1.2" stroke-opacity="0.35"/>'
            )
    # Highlight: small square marker at SF location (~37N 122W)
    # In 0-360 lon: 122W = 238deg → x offset = R*cos(238-180) = R*cos(58) ~0.53*R to the left
    sfx = cx - int(R * 0.53)
    sfy = cy - int(R * 0.6)
    out.append(
        f'<circle cx="{sfx}" cy="{sfy}" r="5" fill="{color}"/>'
        f'<circle cx="{sfx}" cy="{sfy}" r="10" fill="none" '
        f'stroke="{color}" stroke-width="1.5" stroke-opacity="0.5"/>'
    )
    return "".join(out)


def g_xyz_pyramid(cx, cy, color, tint):
    """XYZ tile pyramid — stacked layers narrowing upward."""
    out = []
    # 4 pyramid levels: z0 (widest) to z3 (narrowest), arranged bottom-up
    levels = [
        # (level, rel_y_from_center, width, label)
        (3, -66, 32,  "z16"),
        (2, -36, 64,  "z14"),
        (1,  -4, 100, "z12"),
        (0,  28, 136, "z0 "),
    ]
    h_each = 22
    for (lvl, rel_y, w, label) in levels:
        x = cx - w / 2
        y = cy + rel_y
        # Color intensity by level (deeper = lighter)
        alpha = 0.5 + lvl * 0.15
        out.append(
            f'<rect x="{x}" y="{y}" rx="4" ry="4" width="{w}" height="{h_each}" '
            f'fill="{tint}" fill-opacity="{alpha:.2f}" stroke="{color}" stroke-width="1.8"/>'
        )
        out.append(
            f'<text x="{cx}" y="{y + h_each / 2 + 4}" text-anchor="middle" '
            f'font-family="ui-monospace, Menlo, monospace" font-size="10" '
            f'font-weight="700" fill="{color}">{label}</text>'
        )
    # Connecting lines between levels
    for (lvl, rel_y, w, _) in levels[:-1]:
        # Line from bottom of this level to top of next (wider) level
        next_idx = lvl
        next_rel_y = levels[next_idx][1] + h_each if next_idx < len(levels) - 1 else rel_y + h_each
        # Use the next level's entry (idx+1 since we iterate top-down)
        pass
    return "".join(out)


def g_contour_dem(cx, cy, color, tint):
    """Contour DEM — concentric contour-like curves representing elevation."""
    out = []
    # Background tile
    tw, th = 140, 120
    tx = cx - tw / 2
    ty = cy - th / 2
    out.append(
        f'<rect x="{tx}" y="{ty}" rx="8" ry="8" width="{tw}" height="{th}" '
        f'fill="{tint}" stroke="{color}" stroke-width="2"/>'
    )
    # Contour lines — concentric irregular ovals, offset up-left to imply a hill
    hill_cx = cx - 10
    hill_cy = cy - 8
    for i, (rx_v, ry_v, alpha) in enumerate(
        [(12, 8, 1.0), (26, 18, 0.85), (42, 28, 0.7),
         (58, 38, 0.55), (72, 46, 0.4)]
    ):
        out.append(
            f'<ellipse cx="{hill_cx}" cy="{hill_cy}" rx="{rx_v}" ry="{ry_v}" '
            f'fill="none" stroke="{color}" stroke-width="1.8" '
            f'stroke-opacity="{alpha:.2f}"/>'
        )
    # Summit dot
    out.append(
        f'<circle cx="{hill_cx}" cy="{hill_cy}" r="4" fill="{color}"/>'
    )
    # Elevation labels alongside contours
    labels = [("10m", 16), ("20m", 30), ("30m", 44)]
    for (lbl, offset) in labels:
        out.append(
            f'<text x="{hill_cx + offset}" y="{hill_cy + offset // 2 - 4}" '
            f'font-family="ui-monospace, Menlo, monospace" font-size="9" '
            f'fill="{color}" fill-opacity="0.7">{lbl}</text>'
        )
    return "".join(out)


def g_cog_catalog(cx, cy, color, tint):
    """COG catalog / STAC Delta — folder icon with COG items."""
    out = []
    # Folder shape
    fw, fh = 130, 90
    fx = cx - fw / 2
    fy = cy - fh / 2 + 4
    # Folder tab
    tab_w = 52
    out.append(
        f'<path d="M {fx} {fy} H {fx + tab_w} L {fx + tab_w + 10} {fy - 14} '
        f'H {fx + fw} V {fy + fh} Q {fx + fw} {fy + fh + 8} {fx + fw - 8} {fy + fh + 8} '
        f'H {fx + 8} Q {fx} {fy + fh + 8} {fx} {fy + fh} Z" '
        f'fill="{tint}" stroke="{color}" stroke-width="2" stroke-linejoin="round"/>'
    )
    # COG file icons inside folder — pushed down to clear the "STAC Delta"
    # tab label above and leave breathing room below it
    file_defs = [(-36, 32), (-6, 32), (24, 32)]
    for (fdx, fdy) in file_defs:
        fex = cx + fdx
        fey = fy + fdy
        fw2, fh2 = 24, 30
        # dog-ear corner
        ear = 7
        out.append(
            f'<path d="M {fex} {fey} H {fex + fw2 - ear} L {fex + fw2} {fey + ear} '
            f'V {fey + fh2} H {fex} Z" '
            f'fill="#FFFFFF" stroke="{color}" stroke-width="1.4"/>'
            f'<path d="M {fex + fw2 - ear} {fey} V {fey + ear} H {fex + fw2}" '
            f'fill="none" stroke="{color}" stroke-width="1.2"/>'
        )
        # ".cog" label
        out.append(
            f'<text x="{fex + fw2/2}" y="{fey + fh2 - 8}" text-anchor="middle" '
            f'font-family="ui-monospace, Menlo, monospace" font-size="8" '
            f'font-weight="700" fill="{color}">.cog</text>'
        )
    # Delta label on folder tab — sits below the folder top edge with headroom
    out.append(
        f'<text x="{fx + 12}" y="{fy + 18}" '
        f'font-family="Inter, sans-serif" font-size="9" '
        f'font-weight="700" fill="{color}">STAC Delta</text>'
    )
    return "".join(out)


def g_shard_grid(cx, cy, color, tint):
    """Tile grid partitioned into 4 coarse parent shards — conveys shiftright binning."""
    out = []
    # Outer boundary of the whole grid
    tw, th = 140, 120
    tx = cx - tw / 2
    ty = cy - th / 2
    out.append(
        f'<rect x="{tx}" y="{ty}" rx="8" ry="8" width="{tw}" height="{th}" '
        f'fill="#FFFFFF" stroke="{color}" stroke-width="2.4"/>'
    )
    # Four quadrant shards — each tinted a slightly different shade
    shard_fills = [
        (tint, "0.95"),   # top-left    — 11_326_791
        (tint, "0.65"),   # top-right   — 11_327_791
        (tint, "0.75"),   # bottom-left — 11_326_792
        (tint, "0.50"),   # bottom-right— 11_327_792
    ]
    hw, hh = tw / 2, th / 2
    quadrants = [
        (tx,      ty,      hw, hh, shard_fills[0]),  # TL
        (tx + hw, ty,      hw, hh, shard_fills[1]),  # TR
        (tx,      ty + hh, hw, hh, shard_fills[2]),  # BL
        (tx + hw, ty + hh, hw, hh, shard_fills[3]),  # BR
    ]
    for (qx, qy, qw, qh, (fill, opacity)) in quadrants:
        out.append(
            f'<rect x="{qx}" y="{qy}" width="{qw}" height="{qh}" '
            f'fill="{fill}" fill-opacity="{opacity}"/>'
        )
    # Fine sub-tile grid lines within each quadrant (3×3 sub-tiles per quadrant)
    sub = 3
    for qi in range(sub - 1):
        # Vertical sub-lines in each quadrant column
        for col in [0, 1]:
            lx = tx + col * hw + (qi + 1) * hw / sub
            out.append(
                f'<line x1="{lx:.1f}" y1="{ty + 2}" x2="{lx:.1f}" y2="{ty + th - 2}" '
                f'stroke="{color}" stroke-width="0.6" stroke-opacity="0.25"/>'
            )
        # Horizontal sub-lines in each quadrant row
        for row in [0, 1]:
            ly = ty + row * hh + (qi + 1) * hh / sub
            out.append(
                f'<line x1="{tx + 2}" y1="{ly:.1f}" x2="{tx + tw - 2}" y2="{ly:.1f}" '
                f'stroke="{color}" stroke-width="0.6" stroke-opacity="0.25"/>'
            )
    # Bold shard-boundary dividers (the two main cross-lines)
    out.append(
        f'<line x1="{cx}" y1="{ty + 4}" x2="{cx}" y2="{ty + th - 4}" '
        f'stroke="{color}" stroke-width="2" stroke-opacity="0.7"/>'
        f'<line x1="{tx + 4}" y1="{cy}" x2="{tx + tw - 4}" y2="{cy}" '
        f'stroke="{color}" stroke-width="2" stroke-opacity="0.7"/>'
    )
    # Small shard-id labels in each quadrant
    labels = [
        (tx + hw * 0.5, ty + hh * 0.5, "326/791"),
        (tx + hw * 1.5, ty + hh * 0.5, "327/791"),
        (tx + hw * 0.5, ty + hh * 1.5, "326/792"),
        (tx + hw * 1.5, ty + hh * 1.5, "327/792"),
    ]
    for (lx, ly, lbl) in labels:
        out.append(
            f'<text x="{lx:.1f}" y="{ly + 4:.1f}" text-anchor="middle" '
            f'font-family="ui-monospace, Menlo, monospace" font-size="9" '
            f'font-weight="700" fill="{color}" fill-opacity="0.85">{lbl}</text>'
        )
    # z11 badge (matches style of g_vector_tile_grid)
    out.append(
        f'<rect x="{cx + tw/2 - 28}" y="{ty - 16}" rx="8" width="28" height="16" '
        f'fill="{color}"/>'
        f'<text x="{cx + tw/2 - 14}" y="{ty - 4}" text-anchor="middle" '
        f'font-family="ui-monospace, Menlo, monospace" font-size="10" '
        f'font-weight="700" fill="#FFFFFF">z11</text>'
    )
    return "".join(out)


def g_multi_archive(cx, cy, color, tint):
    """Row of 4 small PMTiles archive icons — one archive per shard."""
    out = []
    # 4 small archive icons arranged in a 2×2 grid
    positions = [(-52, -28), (8, -28), (-52, 28), (8, 28)]
    aw, ah = 56, 38
    for (dx, dy) in positions:
        ax = cx + dx
        ay = cy + dy - 10
        # Back shadow layer
        out.append(
            f'<rect x="{ax + 4}" y="{ay + 4}" rx="5" ry="5" width="{aw}" height="{ah}" '
            f'fill="{tint}" fill-opacity="0.6" stroke="{color}" stroke-width="1"/>'
        )
        # Main archive body
        out.append(
            f'<rect x="{ax}" y="{ay}" rx="5" ry="5" width="{aw}" height="{ah}" '
            f'fill="#FFFFFF" stroke="{color}" stroke-width="1.8"/>'
        )
        # Header bar
        out.append(
            f'<rect x="{ax}" y="{ay}" rx="5" ry="5" width="{aw}" height="12" '
            f'fill="{color}"/>'
            f'<rect x="{ax}" y="{ay + 7}" width="{aw}" height="5" fill="{color}"/>'
        )
        # .pmtiles label in header
        out.append(
            f'<text x="{ax + aw/2}" y="{ay + 9}" text-anchor="middle" '
            f'font-family="ui-monospace, Menlo, monospace" font-size="7" '
            f'font-weight="800" fill="#FFFFFF">.pmtiles</text>'
        )
        # Two content lines
        for li in range(2):
            lx = ax + 5
            ly = ay + 19 + li * 9
            out.append(
                f'<line x1="{lx}" y1="{ly}" x2="{ax + aw - 5}" y2="{ly}" '
                f'stroke="{color}" stroke-opacity="0.35" stroke-width="1.2"/>'
            )
    # Central label
    out.append(
        f'<text x="{cx}" y="{cy + 66}" text-anchor="middle" '
        f'font-family="ui-monospace, Menlo, monospace" font-size="10" '
        f'font-weight="700" fill="{color}">4 shards</text>'
    )
    return "".join(out)


def g_mosaic_catalog(cx, cy, color, tint):
    """Reassembled mosaic tiles + mosaic.json label — shard catalog glyph."""
    out = []
    # Four shard tile squares arranged as a 2×2 mosaic map
    tile_s = 48
    gap = 4
    half = tile_s + gap / 2
    tiles = [
        (cx - half, cy - half - 8, tint, "0.90", "326\n791"),
        (cx + gap/2, cy - half - 8, tint, "0.65", "327\n791"),
        (cx - half, cy + gap/2 - 8, tint, "0.75", "326\n792"),
        (cx + gap/2, cy + gap/2 - 8, tint, "0.50", "327\n792"),
    ]
    for (tx, ty, fill, opacity, lbl) in tiles:
        out.append(
            f'<rect x="{tx:.1f}" y="{ty:.1f}" rx="5" ry="5" '
            f'width="{tile_s}" height="{tile_s}" '
            f'fill="{fill}" fill-opacity="{opacity}" stroke="{color}" stroke-width="1.8"/>'
        )
        # Small grid lines inside each tile (3×3 sub-tiles)
        for si in range(1, 3):
            sx = tx + si * tile_s / 3
            sy_h = ty + si * tile_s / 3
            out.append(
                f'<line x1="{sx:.1f}" y1="{ty + 2:.1f}" x2="{sx:.1f}" y2="{ty + tile_s - 2:.1f}" '
                f'stroke="{color}" stroke-opacity="0.2" stroke-width="0.8"/>'
                f'<line x1="{tx + 2:.1f}" y1="{sy_h:.1f}" x2="{tx + tile_s - 2:.1f}" y2="{sy_h:.1f}" '
                f'stroke="{color}" stroke-opacity="0.2" stroke-width="0.8"/>'
            )
        # Shard label (two-line: zone/row)
        rows = lbl.split("\n")
        for ri, row in enumerate(rows):
            out.append(
                f'<text x="{tx + tile_s/2:.1f}" y="{ty + tile_s/2 + (ri - 0.5) * 11:.1f}" '
                f'text-anchor="middle" font-family="ui-monospace, Menlo, monospace" '
                f'font-size="9" font-weight="700" fill="{color}" fill-opacity="0.9">{row}</text>'
            )
    # Divider lines between quadrants
    mid_x = cx
    mid_y = cy - 8
    out.append(
        f'<line x1="{mid_x}" y1="{mid_y - half - 2}" x2="{mid_x}" y2="{mid_y + half + tile_s - 2}" '
        f'stroke="{color}" stroke-width="2" stroke-opacity="0.5"/>'
        f'<line x1="{mid_x - half - 2}" y1="{mid_y}" x2="{mid_x + half + tile_s - 2}" y2="{mid_y}" '
        f'stroke="{color}" stroke-width="2" stroke-opacity="0.5"/>'
    )
    # mosaic.json badge below the grid
    badge_y = cy + tile_s + gap / 2 - 8 + 8
    badge_w = 96
    badge_h = 18
    out.append(
        f'<rect x="{cx - badge_w/2}" y="{badge_y}" rx="5" ry="5" '
        f'width="{badge_w}" height="{badge_h}" fill="{color}"/>'
        f'<text x="{cx}" y="{badge_y + 13}" text-anchor="middle" '
        f'font-family="ui-monospace, Menlo, monospace" font-size="10" '
        f'font-weight="800" fill="#FFFFFF">mosaic.json</text>'
    )
    return "".join(out)


def g_hillshade_relief(cx, cy, color, tint):
    """Hillshade relief visualization — shaded terrain grid."""
    cells = 8
    s = 14
    span = cells * s
    x0 = cx - span / 2
    y0 = cy - span / 2
    out = [
        f'<rect x="{x0:.1f}" y="{y0:.1f}" width="{span}" height="{span}" '
        f'fill="{tint}" stroke="{color}" stroke-width="2"/>'
    ]
    # Hillshade lit from the upper-left (where the sun glyph sits): cell
    # opacity encodes darkness, so it is LOW (light) near the top-left corner
    # and rises smoothly toward the bottom-right (in shadow) — a clean
    # diagonal relief gradient.
    def hillshade_val(r, c):
        t = (r + c) / (2.0 * (cells - 1))  # 0 at top-left → 1 at bottom-right
        return 0.12 + 0.70 * t

    for r in range(cells):
        for c in range(cells):
            v = hillshade_val(r, c)
            out.append(
                f'<rect x="{x0 + c*s:.1f}" y="{y0 + r*s:.1f}" '
                f'width="{s}" height="{s}" fill="{color}" '
                f'fill-opacity="{v:.2f}"/>'
            )
    # Gridlines
    for i in range(1, cells):
        out.append(
            f'<line x1="{x0 + i*s:.1f}" y1="{y0:.1f}" '
            f'x2="{x0 + i*s:.1f}" y2="{y0 + span:.1f}" '
            f'stroke="#FFFFFF" stroke-width="0.8" stroke-opacity="0.4"/>'
        )
        out.append(
            f'<line x1="{x0:.1f}" y1="{y0 + i*s:.1f}" '
            f'x2="{x0 + span:.1f}" y2="{y0 + i*s:.1f}" '
            f'stroke="#FFFFFF" stroke-width="0.8" stroke-opacity="0.4"/>'
        )
    # Sun icon in upper-left to indicate light direction
    sx, sy = x0 - 16, y0 - 16
    out.append(
        f'<circle cx="{sx}" cy="{sy}" r="8" fill="#F5C242" stroke="#D4A01A" stroke-width="1.5"/>'
    )
    for angle in range(0, 360, 45):
        rad = math.radians(angle)
        ray_x1 = sx + 10 * math.cos(rad)
        ray_y1 = sy + 10 * math.sin(rad)
        ray_x2 = sx + 16 * math.cos(rad)
        ray_y2 = sy + 16 * math.sin(rad)
        out.append(
            f'<line x1="{ray_x1:.1f}" y1="{ray_y1:.1f}" '
            f'x2="{ray_x2:.1f}" y2="{ray_y2:.1f}" '
            f'stroke="#D4A01A" stroke-width="1.5" stroke-linecap="round"/>'
        )
    return "".join(out)


# --- Stage container ----------------------------------------------------------

@dataclass
class Stage:
    title: str
    subtitle: str = ""
    glyph: callable = None
    chip_text: str = ""

def render_stage(x, y, w, stage, accent, tint):
    """Render a single pipeline-stage card."""
    h = STAGE_H
    out = [card(x, y, w, h)]
    out.append(top_stripe(x, y, w, accent))

    # Glyph (centered upper region)
    if stage.glyph:
        out.append(stage.glyph(x + w / 2, y + 20 + STAGE_GLYPH_H / 2, accent, tint))

    # Title
    title_y = y + 20 + STAGE_GLYPH_H + 16
    out.append(text(x + w / 2, title_y, stage.title,
                    size=16, weight=800, fill=C_INK, anchor="middle"))

    # Optional chip below title
    if stage.chip_text:
        chip_svg, chip_w = chip(0, 0, stage.chip_text,
                                fg=accent, bg=tint, mono_font=True)
        chip_x = x + (w - chip_w) / 2
        chip_y = title_y + 14
        out.append(f'<g transform="translate({chip_x}, {chip_y})">{chip_svg}</g>')
        cap_top = chip_y + 48
    else:
        cap_top = title_y + 34

    # Subtitle / caption (wrapped to up to 3 lines)
    if stage.subtitle:
        out.extend(_wrap_text(x + 14, cap_top, w - 28, stage.subtitle,
                              size=12, fill=C_MUTED, line_h=16))
    return "".join(out)

def _wrap_text(x, y, max_w, s, *, size=12, fill=C_MUTED, line_h=16):
    """Naive word-wrap for sans-serif text."""
    char_w = size * 0.55
    max_chars = max(8, int(max_w / char_w))
    words = s.split()
    lines = []
    cur = ""
    for w in words:
        test = (cur + " " + w).strip()
        if len(test) > max_chars and cur:
            lines.append(cur)
            cur = w
        else:
            cur = test
    if cur:
        lines.append(cur)
    out = []
    for i, line in enumerate(lines[:3]):
        out.append(
            f'<text x="{x + max_w / 2}" y="{y + i * line_h}" text-anchor="middle" '
            f'font-family="Inter, sans-serif" font-size="{size}" '
            f'fill="{fill}">{esc(line)}</text>'
        )
    return out

# --- Header / footer ----------------------------------------------------------

def render_header(num, title, subtitle, accent):
    out = []
    # Notebook number badge
    bsize = 60
    bx, by = PAD, PAD + 4
    out.append(
        f'<rect x="{bx}" y="{by}" rx="14" ry="14" width="{bsize}" height="{bsize}" '
        f'fill="{accent}"/>'
        f'<text x="{bx + bsize/2}" y="{by + bsize/2 + 12}" text-anchor="middle" '
        f'font-family="Inter, sans-serif" font-size="32" font-weight="900" '
        f'fill="#FFFFFF">{num:02d}</text>'
    )
    # Title + subtitle
    tx = bx + bsize + 18
    out.append(text(tx, by + 28, title, size=28, weight=800, fill=C_INK))
    out.append(text(tx, by + 54, subtitle, size=14, fill=C_MUTED))
    # Series pill (top-right)
    pill_text = f"Helios Series  ·  Notebook {num} of 4"
    pw = int(len(pill_text) * 6.6) + 24
    out.append(
        f'<rect x="{CANVAS_W - PAD - pw}" y="{PAD + 12}" rx="13" ry="13" '
        f'width="{pw}" height="26" fill="{C_INK}"/>'
        f'<text x="{CANVAS_W - PAD - pw/2}" y="{PAD + 30}" text-anchor="middle" '
        f'font-family="Inter, sans-serif" font-size="12" font-weight="700" '
        f'fill="#FFFFFF">{esc(pill_text)}</text>'
    )
    return "".join(out)

def render_footer(chips, accent, tint, label="KEY FUNCTIONS"):
    """Footer band of mono chips for the headline functions used in the notebook."""
    out = []
    fy = CANVAS_H - PAD - FOOTER_H
    out.append(text(PAD, fy + 16, label,
                    size=10, weight=700, fill=C_MUTED_3, letter_spacing="1.6"))
    cx = PAD + 130
    cy = fy + 6
    for c in chips:
        chip_svg, cw = chip(cx, cy, c, fg=accent, bg=tint, mono_font=True, h=24)
        out.append(chip_svg)
        cx += cw + 8
    out.append(text(CANVAS_W - PAD, fy + 16,
                    "databrickslabs/geobrix  ·  Overture Maps  ·  NAIP  ·  USGS 3DEP",
                    size=11, fill=C_MUTED_3, anchor="end"))
    return "".join(out)

# --- Per-notebook content -----------------------------------------------------

NOTEBOOKS = {
    1: {
        "title": "Vector Engine: building footprints to PMTiles",
        "subtitle": "Overture buildings → MVT pyramid → vector PMTiles archive → inline map",
        "stages": [
            Stage(title="Overture buildings",
                  subtitle="Download SF building footprints via OvertureClient — distributed, bbox-filtered",
                  glyph=g_building_footprints,
                  chip_text="OvertureClient"),
            Stage(title="MVT tile pyramid",
                  subtitle="gbx_st_asmvt_pyramid encodes footprints into z/x/y Mapbox Vector Tiles",
                  glyph=g_vector_tile_grid,
                  chip_text="gbx_st_asmvt_pyramid"),
            Stage(title="PMTiles archive",
                  subtitle="gbx_pmtiles_agg folds the full pyramid into one self-contained vector archive",
                  glyph=g_stacked_archive,
                  chip_text="gbx_pmtiles_agg"),
            Stage(title="Inline map view",
                  subtitle="plot_pmtiles renders the vector archive inline — static or interactive MapLibre",
                  glyph=g_map_pin,
                  chip_text="plot_pmtiles"),
        ],
        "footer_chips": [
            "OvertureClient.discover", "OvertureClient.download",
            "gbx_st_asmvt", "gbx_st_asmvt_pyramid",
            "gbx_pmtiles_agg", "plot_pmtiles",
        ],
    },
    2: {
        "title": "Visual Basemap: NAIP aerial imagery to PMTiles",
        "subtitle": "NAIP GeoTIFF → web-mercator reproject → XYZ pyramid → raster PMTiles → inline map",
        "stages": [
            Stage(title="NAIP aerial imagery",
                  subtitle="Stage a cloud-optimized NAIP GeoTIFF for SF via Planetary Computer STAC",
                  glyph=g_aerial_swatch,
                  chip_text="binaryFile / gtiff_gbx"),
            Stage(title="Web-mercator reproject",
                  subtitle="gbx_rst_to_webmercator aligns the raster to the slippy-map tile grid (EPSG:3857)",
                  glyph=g_webmercator_globe,
                  chip_text="gbx_rst_to_webmercator"),
            Stage(title="XYZ tile pyramid",
                  subtitle="gbx_rst_xyzpyramid slices the reprojected raster into z/x/y PNG tile rows",
                  glyph=g_xyz_pyramid,
                  chip_text="gbx_rst_xyzpyramid"),
            Stage(title="Raster PMTiles",
                  subtitle="gbx_pmtiles_agg bundles the XYZ pyramid; plot_pmtiles renders the basemap inline",
                  glyph=g_map_pin,
                  chip_text="plot_pmtiles"),
        ],
        "footer_chips": [
            "gbx_rst_to_webmercator", "gbx_rst_xyzpyramid",
            "gbx_pmtiles_agg", "pmtiles_info", "plot_pmtiles",
        ],
    },
    3: {
        "title": "Analytical Core: terrain to COG + PMTiles",
        "subtitle": "USGS 3DEP DEM → COG catalog → slope/hillshade → PMTiles → inline map",
        "stages": [
            Stage(title="USGS 3DEP DEM",
                  subtitle="StacClient fetches the SF elevation tile; gbx_rst_cog_convert writes COGs to a STAC Delta catalog",
                  glyph=g_contour_dem,
                  chip_text="gbx_rst_cog_convert"),
            Stage(title="COG + STAC catalog",
                  subtitle="COGs indexed in a queryable Delta table via StacClient — repeatable and auditable",
                  glyph=g_cog_catalog,
                  chip_text="StacClient"),
            Stage(title="Slope / hillshade",
                  subtitle="gbx_rst_slope + gbx_rst_hillshade derive solar-relevant terrain metrics per tile",
                  glyph=g_hillshade_relief,
                  chip_text="gbx_rst_slope"),
            Stage(title="PMTiles + COG view",
                  subtitle="gbx_rst_xyzpyramid + gbx_pmtiles_agg package results; plot_cog and plot_pmtiles render inline",
                  glyph=g_map_pin,
                  chip_text="plot_cog / plot_pmtiles"),
        ],
        "footer_chips": [
            "gbx_rst_cog_convert", "StacClient",
            "gbx_rst_slope", "gbx_rst_hillshade",
            "gbx_rst_xyzpyramid", "plot_cog", "plot_pmtiles",
        ],
    },
    4: {
        "title": "Distributed Sharding & Mosaic",
        "subtitle": "MVT pyramid → shard by parent tile → per-shard PMTiles archives → mosaic catalog + inline map",
        "stages": [
            Stage(title="MVT tile pyramid",
                  subtitle="gbx_st_asmvt_pyramid encodes SF building footprints into z11–z16 Mapbox Vector Tile rows",
                  glyph=g_vector_tile_grid,
                  chip_text="gbx_st_asmvt_pyramid"),
            Stage(title="Shard assignment",
                  subtitle="shiftright(x, z−11) maps every tile to a coarse z11 parent — four non-overlapping SF shards",
                  glyph=g_shard_grid,
                  chip_text="shiftright(x, z−shard_z)"),
            Stage(title="Per-shard archives",
                  subtitle="groupBy(shard).agg(gbx_pmtiles_agg) produces one bounded .pmtiles archive per shard key",
                  glyph=g_multi_archive,
                  chip_text="gbx_pmtiles_agg"),
            Stage(title="Mosaic catalog + map",
                  subtitle="pmtiles_info populates sf_building_shards (Delta) + mosaic.json; plot_pmtiles renders each shard inline",
                  glyph=g_mosaic_catalog,
                  chip_text="plot_pmtiles"),
        ],
        "footer_chips": [
            "gbx_st_asmvt_pyramid", "gbx_pmtiles_agg",
            "pmtiles_info", "plot_pmtiles",
        ],
    },
}

# --- Main render --------------------------------------------------------------

def render_notebook(num):
    nb = NOTEBOOKS[num]
    accent = THEMES[num]["accent"]
    tint = THEMES[num]["tint"]

    parts = []
    parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'viewBox="0 0 {CANVAS_W} {CANVAS_H}" '
        f'width="{CANVAS_W}" height="{CANVAS_H}" '
        f'style="font-family: Inter, -apple-system, system-ui, sans-serif;">'
    )
    parts.append(dedent('''\
        <defs>
          <filter id="card-shadow" x="-5%" y="-5%" width="110%" height="115%">
            <feDropShadow dx="0" dy="2" stdDeviation="6"
                          flood-color="#0F1B2A" flood-opacity="0.08"/>
          </filter>
          <linearGradient id="bg" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0" stop-color="#FAFBFC"/>
            <stop offset="1" stop-color="#F1F4F8"/>
          </linearGradient>
        </defs>
        '''))
    parts.append(f'<rect x="0" y="0" width="{CANVAS_W}" height="{CANVAS_H}" fill="url(#bg)"/>')

    # Header
    parts.append(render_header(num, nb["title"], nb["subtitle"], accent))

    # Stages
    stage_y = PAD + HEADER_H + STAGE_TOP_GAP
    inner_w = CANVAS_W - 2 * PAD
    n = len(nb["stages"])
    arrows_total = (n - 1) * ARROW_W
    stage_w = (inner_w - arrows_total) // n
    cur_x = PAD
    for i, stg in enumerate(nb["stages"]):
        parts.append(render_stage(cur_x, stage_y, stage_w, stg, accent, tint))
        cur_x += stage_w
        if i < n - 1:
            parts.append(arrow(cur_x + 8, stage_y + STAGE_H / 2 - 30,
                               cur_x + ARROW_W - 8, color=accent))
            cur_x += ARROW_W

    # Footer
    parts.append(render_footer(nb["footer_chips"], accent, tint))

    parts.append("</svg>")
    return "\n".join(parts)


if __name__ == "__main__":
    here = os.path.dirname(os.path.abspath(__file__))
    for num in (1, 2, 3, 4):
        out = os.path.join(here, "..", "diagrams", "helios", f"helios-{num:02d}.svg")
        with open(out, "w") as f:
            f.write(render_notebook(num))
        print(f"wrote {out}")
