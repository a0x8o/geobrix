#!/usr/bin/env python3
"""Generate the four EO Series notebook conceptual diagrams.

One SVG per notebook (01–04). Each is a four-stage pipeline diagram with a
custom hero glyph per stage and a footer of GeoBrix / Databricks function chips
that the notebook actually uses.

Re-render after editing this script:

    python3 resources/images/generators/eo-series.py
    for n in 01 02 03 04; do
      # window-size is wider/taller than the SVG to absorb Chrome's default
      # body margin; the bbox-trim step below crops it back to SVG bounds.
      "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \\
          --headless --disable-gpu --hide-scrollbars \\
          --force-device-scale-factor=2 --window-size=1500,820 \\
          --screenshot=resources/images/diagrams/eo-series/eo-series-$n.png \\
          resources/images/diagrams/eo-series/eo-series-$n.svg
    done
    python3 -c "from PIL import Image, ImageChops; \\
      [Image.open(p).convert('RGB').crop(ImageChops.difference( \\
        Image.open(p).convert('RGB'), \\
        Image.new('RGB', Image.open(p).size, (255,255,255))).getbbox()).save(p) \\
       for p in [f'resources/images/diagrams/eo-series/eo-series-{n}.png' for n in ('01','02','03','04')]]"
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
    1: {"accent": "#1F6FB5", "tint": "#E3EEF8"},   # blue   — discovery
    2: {"accent": "#E04E2A", "tint": "#FCE9E2"},   # orange — download
    3: {"accent": "#0F8E8B", "tint": "#D5ECEC"},   # teal   — H3 tessellation
    4: {"accent": "#7A4FD3", "tint": "#ECE6FA"},   # purple — stack + clip
}

# Sentinel band colors (used in 02 + 04)
BAND_COLORS = {
    "B02": "#2D6CDF",  # blue ~490 nm
    "B03": "#2FA56A",  # green ~560 nm
    "B04": "#E04E2A",  # red ~665 nm
    "B08": "#7A4FD3",  # NIR ~842 nm
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

def g_polygon_ak(cx, cy, color, tint):
    """Stylised Alaska/Ketchikan-ish irregular polygon."""
    pts_local = [(-60, -22), (-32, -48), (4, -42), (38, -32),
                 (62, -8), (44, 16), (52, 38), (24, 50),
                 (-12, 44), (-46, 30), (-58, 6)]
    pts = " ".join(f"{cx + dx},{cy + dy}" for dx, dy in pts_local)
    return (f'<polygon points="{pts}" fill="{tint}" stroke="{color}" '
            f'stroke-width="2" stroke-linejoin="round"/>'
            # interior accent dot — Ketchikan "pin"
            f'<circle cx="{cx + 16}" cy="{cy + 8}" r="5" fill="{color}"/>'
            f'<circle cx="{cx + 16}" cy="{cy + 8}" r="11" fill="none" '
            f'stroke="{color}" stroke-opacity="0.45" stroke-width="1.5"/>')

def g_hex_grid(cx, cy, color, tint, *, R=24, rows=3, cols=4):
    """Pointy-top H3-ish hex grid centered roughly on (cx, cy)."""
    out = []
    dx = R * math.sqrt(3)
    dy = R * 1.5
    x0 = cx - (cols - 1) * dx / 2 - dx / 4
    y0 = cy - (rows - 1) * dy / 2
    highlight_idx = (rows // 2, cols // 2)
    for r in range(rows):
        for c in range(cols):
            x = x0 + c * dx + (dx / 2 if r % 2 else 0)
            y = y0 + r * dy
            pts = []
            for i in range(6):
                a = math.radians(60 * i - 90)
                pts.append(f"{x + R*math.cos(a):.1f},{y + R*math.sin(a):.1f}")
            fill = color if (r, c) == highlight_idx else tint
            opacity = "1.0" if (r, c) == highlight_idx else "0.85"
            out.append(
                f'<polygon points="{" ".join(pts)}" fill="{fill}" '
                f'fill-opacity="{opacity}" stroke="{color}" stroke-width="1.6"/>'
            )
    return "".join(out)

def g_single_hex(cx, cy, color, tint, *, R=46):
    pts = []
    for i in range(6):
        a = math.radians(60 * i - 90)
        pts.append(f"{cx + R*math.cos(a):.1f},{cy + R*math.sin(a):.1f}")
    return (f'<polygon points="{" ".join(pts)}" fill="{tint}" '
            f'stroke="{color}" stroke-width="2.4"/>')

def g_stac_cloud(cx, cy, color, tint):
    """Cloud silhouette with small stacked rectangles representing STAC items."""
    # Cloud
    cloud = (
        f'<path d="'
        f'M {cx-58} {cy+10} '
        f'a 22 22 0 0 1 14 -36 '
        f'a 28 28 0 0 1 50 -8 '
        f'a 22 22 0 0 1 38 14 '
        f'a 18 18 0 0 1 -8 36 '
        f'L {cx-58} {cy+18} Z" '
        f'fill="{tint}" stroke="{color}" stroke-width="2" stroke-linejoin="round"/>'
    )
    # 3 mini "items"
    items = []
    base_y = cy + 30
    for i, dx in enumerate([-32, -8, 16]):
        items.append(
            f'<rect x="{cx + dx}" y="{base_y}" width="20" height="16" rx="3" ry="3" '
            f'fill="#FFFFFF" stroke="{color}" stroke-width="1.6"/>'
            f'<line x1="{cx + dx + 4}" y1="{base_y + 6}" x2="{cx + dx + 16}" y2="{base_y + 6}" '
            f'stroke="{color}" stroke-width="1.4"/>'
            f'<line x1="{cx + dx + 4}" y1="{base_y + 11}" x2="{cx + dx + 12}" y2="{base_y + 11}" '
            f'stroke="{color}" stroke-opacity="0.5" stroke-width="1.4"/>'
        )
    return cloud + "".join(items)

def g_delta_table(cx, cy, color, tint, *, label="Delta"):
    """Stacked Delta-table icon."""
    w, h = 110, 80
    x = cx - w / 2
    y = cy - h / 2
    out = [
        f'<rect x="{x}" y="{y}" rx="8" ry="8" width="{w}" height="{h}" '
        f'fill="{tint}" stroke="{color}" stroke-width="2"/>'
    ]
    # header
    out.append(
        f'<rect x="{x}" y="{y}" width="{w}" height="22" rx="8" ry="8" fill="{color}"/>'
        f'<rect x="{x}" y="{y+12}" width="{w}" height="10" fill="{color}"/>'
        f'<text x="{cx}" y="{y + 16}" text-anchor="middle" '
        f'font-family="Inter, sans-serif" font-size="11" font-weight="700" '
        f'fill="#FFFFFF">{label}</text>'
    )
    # rows
    for i in range(3):
        ry = y + 30 + i * 14
        out.append(
            f'<line x1="{x + 10}" y1="{ry}" x2="{x + w - 10}" y2="{ry}" '
            f'stroke="{color}" stroke-opacity="0.45" stroke-width="1.4"/>'
        )
    return "".join(out)

def g_items_list(cx, cy, color, tint):
    """Vertical list of 4 STAC "item" cards."""
    w, h_each, gap = 130, 22, 6
    total_h = 4 * h_each + 3 * gap
    x = cx - w / 2
    y0 = cy - total_h / 2
    out = []
    for i in range(4):
        ry = y0 + i * (h_each + gap)
        out.append(
            f'<rect x="{x}" y="{ry}" rx="6" ry="6" width="{w}" height="{h_each}" '
            f'fill="#FFFFFF" stroke="{color}" stroke-width="1.6"/>'
            f'<rect x="{x}" y="{ry}" width="6" height="{h_each}" rx="2" ry="2" fill="{color}"/>'
            f'<text x="{x + 16}" y="{ry + 14}" font-family="ui-monospace, Menlo, monospace" '
            f'font-size="10" font-weight="600" fill="{color}">S2A_T08VML…</text>'
        )
    return "".join(out)

def g_band_fanout(cx, cy):
    """4 colored band circles arranged vertically."""
    out = []
    R = 22
    spacing = 56
    for i, b in enumerate(["B02", "B03", "B04", "B08"]):
        col = BAND_COLORS[b]
        y = cy - 1.5 * spacing + i * spacing
        out.append(
            f'<circle cx="{cx}" cy="{y}" r="{R}" fill="{col}" '
            f'fill-opacity="0.18" stroke="{col}" stroke-width="2.4"/>'
            f'<text x="{cx}" y="{y + 5}" text-anchor="middle" '
            f'font-family="ui-monospace, Menlo, monospace" '
            f'font-size="13" font-weight="700" fill="{col}">{b}</text>'
        )
    return "".join(out)

def g_band_tables(cx, cy, color):
    """4 small Delta tables stacked vertically, each color-tagged per band."""
    out = []
    w, h, gap = 140, 30, 8
    total_h = 4 * h + 3 * gap
    x = cx - w / 2
    y0 = cy - total_h / 2
    bands = ["B02", "B03", "B04", "B08"]
    for i, b in enumerate(bands):
        col = BAND_COLORS[b]
        ry = y0 + i * (h + gap)
        out.append(
            f'<rect x="{x}" y="{ry}" rx="6" ry="6" width="{w}" height="{h}" '
            f'fill="#FFFFFF" stroke="{color}" stroke-width="1.4"/>'
            f'<rect x="{x}" y="{ry}" width="8" height="{h}" rx="3" ry="3" fill="{col}"/>'
            f'<text x="{x + 18}" y="{ry + 14}" font-family="ui-monospace, Menlo, monospace" '
            f'font-size="11" font-weight="700" fill="{col}">band_{b.lower()}</text>'
            f'<text x="{x + 18}" y="{ry + 25}" font-family="Inter, sans-serif" '
            f'font-size="9" fill="{C_MUTED_2}">is_out_file_valid</text>'
        )
    return "".join(out)

def g_retry_loop(cx, cy, color):
    """Curved arrow loop indicating retry."""
    return (
        f'<path d="M {cx-50} {cy} '
        f'A 30 30 0 1 1 {cx+50} {cy}" '
        f'fill="none" stroke="{color}" stroke-width="2.4" stroke-linecap="round"/>'
        f'<polygon points="{cx+50},{cy} {cx+38},{cy-7} {cx+38},{cy+7}" fill="{color}"/>'
        f'<text x="{cx}" y="{cy + 6}" text-anchor="middle" '
        f'font-family="Inter, sans-serif" font-size="11" font-weight="700" fill="{color}">'
        f'retry</text>'
    )

def g_raster_scene(cx, cy, color, tint):
    """A larger satellite-tile pixel grid with intensity gradient."""
    cells = 8
    s = 14
    span = cells * s
    x0 = cx - span / 2
    y0 = cy - span / 2
    out = [
        f'<rect x="{x0:.1f}" y="{y0:.1f}" width="{span}" height="{span}" '
        f'fill="{tint}" stroke="{color}" stroke-width="2"/>'
    ]
    # Pseudo-satellite intensity field: a gradient + noise
    rng_seed = 7
    def n(r, c):
        # cheap deterministic noise
        return (math.sin((r+1) * 1.7 + (c+1) * 0.9 + rng_seed) + 1) / 2
    for r in range(cells):
        for c in range(cells):
            v = 0.25 + 0.65 * (0.5 * (1 - r / cells) + 0.5 * (c / cells))
            v = (v + 0.35 * n(r, c)) / 1.35
            out.append(
                f'<rect x="{x0 + c*s:.1f}" y="{y0 + r*s:.1f}" '
                f'width="{s}" height="{s}" fill="{color}" '
                f'fill-opacity="{v:.2f}"/>'
            )
    # gridlines
    for i in range(1, cells):
        out.append(
            f'<line x1="{x0 + i*s:.1f}" y1="{y0:.1f}" '
            f'x2="{x0 + i*s:.1f}" y2="{y0 + span:.1f}" '
            f'stroke="#FFFFFF" stroke-width="0.8" stroke-opacity="0.45"/>'
        )
        out.append(
            f'<line x1="{x0:.1f}" y1="{y0 + i*s:.1f}" '
            f'x2="{x0 + span:.1f}" y2="{y0 + i*s:.1f}" '
            f'stroke="#FFFFFF" stroke-width="0.8" stroke-opacity="0.45"/>'
        )
    return "".join(out)

def g_dense_hex_grid(cx, cy, color, tint):
    """Lots of hexes — H3 res-7 tessellation feel."""
    return g_hex_grid(cx, cy, color, tint, R=14, rows=7, cols=8)

def g_timeseries_in_hex(cx, cy, color, tint):
    """Single hex with a small line chart inside."""
    out = [g_single_hex(cx, cy, color, tint, R=58)]
    # chart axes
    cw, ch = 78, 38
    x = cx - cw / 2
    y = cy - ch / 2 + 4
    out.append(
        f'<line x1="{x}" y1="{y + ch}" x2="{x + cw}" y2="{y + ch}" '
        f'stroke="{color}" stroke-width="1.4" stroke-opacity="0.45"/>'
        f'<line x1="{x}" y1="{y}" x2="{x}" y2="{y + ch}" '
        f'stroke="{color}" stroke-width="1.4" stroke-opacity="0.45"/>'
    )
    # series points
    pts = [(0, 0.7), (0.16, 0.45), (0.33, 0.55), (0.5, 0.25),
           (0.66, 0.4), (0.83, 0.2), (1.0, 0.35)]
    polyline = " ".join(f"{x + p*cw:.1f},{y + v*ch:.1f}" for p, v in pts)
    out.append(
        f'<polyline points="{polyline}" fill="none" stroke="{color}" '
        f'stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/>'
    )
    for p, v in pts:
        out.append(
            f'<circle cx="{x + p*cw:.1f}" cy="{y + v*ch:.1f}" r="2.4" fill="{color}"/>'
        )
    return "".join(out)

def g_band_stack(cx, cy):
    """4 offset/stacked rectangles representing R/G/B/NIR bands."""
    bands = [("B08", BAND_COLORS["B08"]), ("B04", BAND_COLORS["B04"]),
             ("B03", BAND_COLORS["B03"]), ("B02", BAND_COLORS["B02"])]
    w, h = 130, 90
    skew = 12
    out = []
    for i, (lbl, col) in enumerate(bands):
        x = cx - w / 2 + (3 - i) * skew
        y = cy - h / 2 + (3 - i) * skew - 30
        out.append(
            f'<rect x="{x}" y="{y}" rx="6" ry="6" width="{w}" height="{h}" '
            f'fill="{col}" fill-opacity="0.18" stroke="{col}" stroke-width="2"/>'
            f'<text x="{x + 10}" y="{y + 16}" '
            f'font-family="ui-monospace, Menlo, monospace" '
            f'font-size="11" font-weight="700" fill="{col}">{lbl}</text>'
        )
    return "".join(out)

def g_multiband_tile(cx, cy, color, tint):
    """A single tile with 4 colored band stripes inside."""
    w, h = 130, 100
    x = cx - w / 2
    y = cy - h / 2
    out = [
        f'<rect x="{x}" y="{y}" rx="8" ry="8" width="{w}" height="{h}" '
        f'fill="{tint}" stroke="{color}" stroke-width="2"/>'
    ]
    band_h = (h - 16) / 4
    bands = [("R", BAND_COLORS["B04"]), ("G", BAND_COLORS["B03"]),
             ("B", BAND_COLORS["B02"]), ("NIR", BAND_COLORS["B08"])]
    for i, (lbl, col) in enumerate(bands):
        ry = y + 8 + i * band_h
        out.append(
            f'<rect x="{x + 8}" y="{ry}" width="{w - 16}" height="{band_h - 4}" '
            f'rx="3" ry="3" fill="{col}" fill-opacity="0.65"/>'
            f'<text x="{x + 14}" y="{ry + (band_h - 4) / 2 + 4:.1f}" '
            f'font-family="ui-monospace, Menlo, monospace" '
            f'font-size="11" font-weight="800" fill="#FFFFFF">{lbl}</text>'
        )
    return "".join(out)

def g_clip_with_buffer(cx, cy, color, tint):
    """A tile (raster grid) with a buffered envelope clipping it."""
    out = [g_raster_scene(cx, cy, color, tint)]
    # Buffered envelope (rounded rect)
    bw, bh = 76, 76
    out.append(
        f'<rect x="{cx - bw/2}" y="{cy - bh/2}" width="{bw}" height="{bh}" '
        f'rx="14" ry="14" fill="none" stroke="{color}" stroke-width="3" '
        f'stroke-dasharray="6,5"/>'
    )
    # corner pin (centroid)
    out.append(
        f'<circle cx="{cx}" cy="{cy}" r="4" fill="{color}"/>'
        f'<circle cx="{cx}" cy="{cy}" r="9" fill="none" stroke="{color}" '
        f'stroke-width="1.4" stroke-opacity="0.5"/>'
    )
    return "".join(out)

def g_gdal_read_box(cx, cy, color, tint):
    """A 'GDAL' box converting raster bytes to a typed tile struct."""
    out = []
    w, h = 130, 100
    x = cx - w / 2
    y = cy - h / 2
    out.append(
        f'<rect x="{x}" y="{y}" rx="10" ry="10" width="{w}" height="{h}" '
        f'fill="#FFFFFF" stroke="{color}" stroke-width="2"/>'
    )
    out.append(
        f'<rect x="{x}" y="{y}" width="{w}" height="26" rx="10" ry="10" fill="{color}"/>'
        f'<rect x="{x}" y="{y+16}" width="{w}" height="10" fill="{color}"/>'
        f'<text x="{cx}" y="{y + 18}" text-anchor="middle" '
        f'font-family="Inter, sans-serif" font-size="12" font-weight="800" '
        f'fill="#FFFFFF">tile struct</text>'
    )
    rows = ["cellid", "raster", "metadata"]
    for i, r in enumerate(rows):
        ry = y + 40 + i * 18
        out.append(
            f'<text x="{x + 14}" y="{ry}" '
            f'font-family="ui-monospace, Menlo, monospace" '
            f'font-size="11" font-weight="600" fill="{color}">{r}</text>'
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
        # Give the caption real breathing room below the chip:
        # chip bottom is at chip_y + 22 (chip h); caption baseline at chip_y + 48
        # leaves ~14px between chip bottom and the top of the text glyphs.
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
    # Notebook number badge (square with rounded corners)
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
    pill_text = f"EO Series  ·  Notebook {num} of 4"
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
                    "databrickslabs/geobrix  ·  Sentinel-2 L2A  ·  Planetary Computer",
                    size=11, fill=C_MUTED_3, anchor="end"))
    return "".join(out)

# --- Per-notebook content -----------------------------------------------------

NOTEBOOKS = {
    1: {
        "title": "Discover EO imagery via STAC",
        "subtitle": "Spatially-indexed catalog search — AOI polygon to per-cell STAC item set",
        "stages": [
            Stage(title="AOI polygon",
                  subtitle="Load TIGER counties from a zipped shapefile, filter to Ketchikan",
                  glyph=g_polygon_ak,
                  chip_text="shapefile_gbx"),
            Stage(title="H3 cells (res-2)",
                  subtitle="Tessellate AOI to coarse H3 cells — one query per cell",
                  glyph=g_hex_grid,
                  chip_text="h3_tessellateaswkb"),
            Stage(title="Planetary Computer",
                  subtitle="StacClient.search queries sentinel-2-l2a items intersecting each H3 cell",
                  glyph=g_stac_cloud,
                  chip_text="StacClient"),
            Stage(title="Asset catalog",
                  subtitle="Persisted as cell_assets_<ts>.delta — auditable & repeatable",
                  glyph=lambda cx, cy, c, t: g_delta_table(cx, cy, c, t, label="cell_assets"),
                  chip_text="Delta"),
        ],
        "footer_chips": ["shapefile_gbx", "h3_tessellateaswkb", "h3_boundaryasgeojson",
                         "StacClient", "Delta time-travel"],
    },
    2: {
        "title": "Parallel band download with idempotent retry",
        "subtitle": "Concurrent Sentinel-2 asset retrieval into Volumes, with self-healing throttle handling",
        "stages": [
            Stage(title="Item list",
                  subtitle="Unique sentinel-2 item_ids consolidated from cell_assets",
                  glyph=g_items_list,
                  chip_text="cell_assets_*.delta"),
            Stage(title="Bands of interest",
                  subtitle="B02 / B03 / B04 / B08 — extensible to any Sentinel-2 band",
                  glyph=lambda cx, cy, c, t: g_band_fanout(cx, cy),
                  chip_text="StacClient.download"),
            Stage(title="Self-healing retry",
                  subtitle="StacClient.repair re-downloads invalid files via a Delta MERGE on the band table",
                  glyph=lambda cx, cy, c, t: g_retry_loop(cx, cy, c),
                  chip_text="is_out_file_valid"),
            Stage(title="Per-band tables",
                  subtitle="One band_<band> Delta table per band, ready for joins",
                  glyph=lambda cx, cy, c, t: g_band_tables(cx, cy, c),
                  chip_text="/Volumes/.../alaska/<band>/"),
        ],
        "footer_chips": ["StacClient.download", "StacClient.repair", "is_out_file_valid",
                         "plot_file", "Delta MERGE"],
    },
    3: {
        "title": "Tessellate rasters to H3 cells",
        "subtitle": "Shred Sentinel-2 scenes into spatially-indexed cell tables — joins across bands and dates",
        "stages": [
            Stage(title="Sentinel-2 scene",
                  subtitle="Per-band GeoTIFFs from the Volume, loaded with the gtiff_gbx reader",
                  glyph=g_raster_scene,
                  chip_text="gtiff_gbx"),
            Stage(title="Typed tile struct",
                  subtitle="bytes + bbox + SRID + standardized nodata in a single materialization",
                  glyph=g_gdal_read_box,
                  chip_text="rst_summary"),
            Stage(title="H3 res-7 tessellation",
                  subtitle="Each scene explodes into many cell-sized tiles via rst_h3_tessellate",
                  glyph=g_dense_hex_grid,
                  chip_text="rst_h3_tessellate"),
            Stage(title="Per-cell timeseries",
                  subtitle="band_b0X_h3 tables enable k-rings, joins, and raster→series projections",
                  glyph=g_timeseries_in_hex,
                  chip_text="rst_apply"),
        ],
        "footer_chips": ["gtiff_gbx", "rst_h3_tessellate",
                         "rst_summary", "rst_apply", "h3_kring", "rst_merge_agg"],
    },
    4: {
        "title": "Stack bands + clip with geometry",
        "subtitle": "Fuse R/G/B/NIR per (cellid, date) and write CRS-safe clipped GeoTIFFs",
        "stages": [
            Stage(title="Per-band cell tables",
                  subtitle="band_b02_h3 / b03 / b04 / b08 joined on (cellid, date)",
                  glyph=lambda cx, cy, c, t: g_band_stack(cx, cy),
                  chip_text="(cellid, date)"),
            Stage(title="Multi-band raster",
                  subtitle="rst_frombands stacks bands in (R, G, B, NIR) order — one tile per cell-date",
                  glyph=g_multiband_tile,
                  chip_text="rst_frombands"),
            Stage(title="GeoTIFF write-back",
                  subtitle="gtiff_gbx writer materializes stacked tiles to /Volumes/.../alaska/out/stacked-tif",
                  glyph=lambda cx, cy, c, t: g_delta_table(cx, cy, c, t, label="stacked-tif"),
                  chip_text="gtiff_gbx"),
            Stage(title="CRS-safe clip",
                  subtitle="EWKB cutline (envelope of buffered centroid) — rst_clip auto-reprojects",
                  glyph=g_clip_with_buffer,
                  chip_text="rst_clip"),
        ],
        "footer_chips": ["rst_frombands", "gtiff_gbx", "rst_clip",
                         "st_envelope", "st_buffer", "st_asewkb"],
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
        out = os.path.join(here, "..", "diagrams", "eo-series", f"eo-series-{num:02d}.svg")
        with open(out, "w") as f:
            f.write(render_notebook(num))
        print(f"wrote {out}")
