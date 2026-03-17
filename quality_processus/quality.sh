#!/usr/bin/env bash

set -euo pipefail

# Usage:
#   ./video_quality_check.sh /srv/exoria/inbox/videos/session_20260316_174007
#
# Dépendances:
#   - bash
#   - ffmpeg
#   - ffprobe
#   - awk
#   - sed
#   - grep
#
# Ce script:
#   - analyse head.mp4 / left.mp4 / right.mp4
#   - calcule plusieurs métriques de qualité
#   - donne une note /100 par vidéo
#   - donne une appréciation globale
#
# Métriques utilisées:
#   - netteté: via filtre blurdetect
#   - luminosité moyenne: via signalstats (YAVG)
#   - clipping noir/blanc: via signalstats (YMIN/YMAX)
#   - variation temporelle: via signalstats (YDIF) pour repérer image figée / très pauvre
#   - durée / nombre de frames valides
#
# Note:
#   - ce n'est pas une "vérité absolue"
#   - c'est un scoring heuristique utile pour tri automatique et alerte qualité

SESSION_DIR="${1:-}"
VIDEOS_DIR="${SESSION_DIR}/videos"

if [[ -z "$SESSION_DIR" ]]; then
  echo "Usage: $0 <session_directory>"
  exit 1
fi

fail() {
  echo "[ERROR] $1" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "Missing command: $1"
}

require_file() {
  [[ -f "$1" ]] || fail "Missing file: $1"
}

get_duration() {
  local file="$1"
  ffprobe -v error -show_entries format=duration -of default=nw=1:nk=1 "$file" \
    | head -n1 | tr -d '\r'
}

get_frame_count() {
  local file="$1"
  local count
  count="$(ffprobe -v error -select_streams v:0 -count_packets \
    -show_entries stream=nb_read_packets -of csv=p=0 "$file" 2>/dev/null | tr -d '\r')"

  if [[ -z "$count" || ! "$count" =~ ^[0-9]+$ ]]; then
    count="$(ffprobe -v error -count_frames -select_streams v:0 \
      -show_entries stream=nb_read_frames -of csv=p=0 "$file" 2>/dev/null | tr -d '\r')"
  fi

  [[ -n "$count" && "$count" =~ ^[0-9]+$ ]] || fail "Impossible de lire le nombre de frames pour $file"
  echo "$count"
}

# Extrait:
#   blur_mean      : moyenne de blurdetect.blur
#   yavg_mean      : moyenne YAVG
#   ydif_mean      : moyenne YDIF
#   ymin_mean      : moyenne YMIN
#   ymax_mean      : moyenne YMAX
#
# blurdetect:
#   plus grand = plus flou
#   plus petit = plus net
#
analyze_video_metrics() {
  local file="$1"

  ffmpeg -v info -i "$file" \
    -vf "signalstats,blurdetect=block_width=32:block_height=32:block_pct=80" \
    -an -f null - 2>&1 | awk '
      /lavfi\.signalstats\.YAVG=/ {
        if (match($0, /lavfi\.signalstats\.YAVG=([0-9.]+)/, m)) {
          yavg_sum += m[1]
          yavg_n++
        }
        if (match($0, /lavfi\.signalstats\.YDIF=([0-9.]+)/, m2)) {
          ydif_sum += m2[1]
          ydif_n++
        }
        if (match($0, /lavfi\.signalstats\.YMIN=([0-9.]+)/, m3)) {
          ymin_sum += m3[1]
          ymin_n++
        }
        if (match($0, /lavfi\.signalstats\.YMAX=([0-9.]+)/, m4)) {
          ymax_sum += m4[1]
          ymax_n++
        }
      }
      /lavfi\.blurdetect\.blur=/ {
        if (match($0, /lavfi\.blurdetect\.blur=([0-9.]+)/, b)) {
          blur_sum += b[1]
          blur_n++
        }
      }
      END {
        if (blur_n == 0) blur_n = 1
        if (yavg_n == 0) yavg_n = 1
        if (ydif_n == 0) ydif_n = 1
        if (ymin_n == 0) ymin_n = 1
        if (ymax_n == 0) ymax_n = 1

        printf("blur_mean=%.6f\n", blur_sum / blur_n)
        printf("yavg_mean=%.6f\n", yavg_sum / yavg_n)
        printf("ydif_mean=%.6f\n", ydif_sum / ydif_n)
        printf("ymin_mean=%.6f\n", ymin_sum / ymin_n)
        printf("ymax_mean=%.6f\n", ymax_sum / ymax_n)
      }
    '
}

# Heuristique de scoring:
#   Score final /100
#   - netteté: 50 pts
#   - exposition/luminosité: 20 pts
#   - clipping: 15 pts
#   - dynamique temporelle: 15 pts
#
# blur_mean:
#   <= 1.5  : excellent
#   <= 3.0  : bon
#   <= 5.0  : moyen
#   >  5.0  : faible
#
# yavg_mean:
#   zone idéale env. [70, 185]
#
# clipping:
#   YMIN trop proche de 0 souvent => noirs bouchés
#   YMAX trop proche de 255 souvent => blancs brûlés
#
# ydif_mean:
#   trop bas => image figée / très peu d'information temporelle
#
score_video() {
  local name="$1"
  local file="$2"

  local duration frames
  duration="$(get_duration "$file")"
  frames="$(get_frame_count "$file")"

  [[ -n "$duration" ]] || fail "Durée illisible pour $file"
  [[ "$frames" -gt 0 ]] || fail "Aucune frame dans $file"

  local metrics
  metrics="$(analyze_video_metrics "$file")"

  local blur_mean yavg_mean ydif_mean ymin_mean ymax_mean
  blur_mean="$(echo "$metrics" | awk -F= '/^blur_mean=/{print $2}')"
  yavg_mean="$(echo "$metrics" | awk -F= '/^yavg_mean=/{print $2}')"
  ydif_mean="$(echo "$metrics" | awk -F= '/^ydif_mean=/{print $2}')"
  ymin_mean="$(echo "$metrics" | awk -F= '/^ymin_mean=/{print $2}')"
  ymax_mean="$(echo "$metrics" | awk -F= '/^ymax_mean=/{print $2}')"

  awk -v name="$name" \
      -v file="$file" \
      -v duration="$duration" \
      -v frames="$frames" \
      -v blur="$blur_mean" \
      -v yavg="$yavg_mean" \
      -v ydif="$ydif_mean" \
      -v ymin="$ymin_mean" \
      -v ymax="$ymax_mean" '
    function clamp(v, lo, hi) {
      if (v < lo) return lo
      if (v > hi) return hi
      return v
    }

    function abs(v) {
      return v < 0 ? -v : v
    }

    function quality_label(score) {
      if (score >= 90) return "excellent"
      if (score >= 75) return "bon"
      if (score >= 60) return "acceptable"
      if (score >= 40) return "faible"
      return "mauvais"
    }

    BEGIN {
      # 1) Netteté / 50
      # plus blur est élevé, plus l image est floue
      if (blur <= 1.5) {
        sharp_score = 50
        sharp_text = "très nette"
      } else if (blur <= 3.0) {
        sharp_score = 40 - ((blur - 1.5) / 1.5) * 10
        sharp_text = "nette"
      } else if (blur <= 5.0) {
        sharp_score = 30 - ((blur - 3.0) / 2.0) * 15
        sharp_text = "moyennement nette"
      } else if (blur <= 8.0) {
        sharp_score = 15 - ((blur - 5.0) / 3.0) * 10
        sharp_text = "floue"
      } else {
        sharp_score = 3
        sharp_text = "très floue"
      }
      sharp_score = clamp(sharp_score, 0, 50)

      # 2) Luminosité / 20
      # cible autour de 128, zone confortable environ 70..185
      if (yavg >= 70 && yavg <= 185) {
        exposure_score = 20
        exposure_text = "exposition correcte"
      } else {
        dist = (yavg < 70) ? (70 - yavg) : (yavg - 185)
        exposure_score = 20 - dist * 0.18
        exposure_score = clamp(exposure_score, 0, 20)
        if (yavg < 70) exposure_text = "trop sombre"
        else exposure_text = "trop lumineuse"
      }

      # 3) Clipping / 15
      # pénalité si noirs bouchés ou blancs brûlés
      clip_penalty = 0

      if (ymin < 8) clip_penalty += (8 - ymin) * 0.7
      if (ymax > 247) clip_penalty += (ymax - 247) * 0.7

      clip_score = 15 - clip_penalty
      clip_score = clamp(clip_score, 0, 15)

      if (clip_score >= 12) clip_text = "plage dynamique correcte"
      else if (clip_score >= 7) clip_text = "clipping modéré"
      else clip_text = "clipping fort"

      # 4) Dynamique temporelle / 15
      # si ydif très faible: vidéo possiblement figée / très pauvre
      if (ydif >= 2.0) {
        motion_score = 15
        motion_text = "variation temporelle saine"
      } else if (ydif >= 1.0) {
        motion_score = 10 + (ydif - 1.0) * 5
        motion_text = "variation modérée"
      } else if (ydif >= 0.3) {
        motion_score = 4 + (ydif - 0.3) * (6 / 0.7)
        motion_text = "variation faible"
      } else {
        motion_score = 1
        motion_text = "quasi figée"
      }
      motion_score = clamp(motion_score, 0, 15)

      total = sharp_score + exposure_score + clip_score + motion_score
      total = clamp(total, 0, 100)

      print "------------------------------------------------------------"
      print "VIDEO: " name
      print "FILE:  " file
      printf("DUREE: %.2f s\n", duration)
      print "FRAMES: " frames
      print ""
      printf("METRIQUES:\n")
      printf("  blur_mean : %.4f\n", blur)
      printf("  yavg_mean : %.4f\n", yavg)
      printf("  ydif_mean : %.4f\n", ydif)
      printf("  ymin_mean : %.4f\n", ymin)
      printf("  ymax_mean : %.4f\n", ymax)
      print ""
      printf("SCORES:\n")
      printf("  nettete   : %.1f / 50   -> %s\n", sharp_score, sharp_text)
      printf("  exposition: %.1f / 20   -> %s\n", exposure_score, exposure_text)
      printf("  clipping  : %.1f / 15   -> %s\n", clip_score, clip_text)
      printf("  dynamique : %.1f / 15   -> %s\n", motion_score, motion_text)
      print ""
      printf("NOTE FINALE: %.1f / 100\n", total)
      printf("APPRECIATION: %s\n", quality_label(total))
    }
  '
}

summarize_session() {
  local scores_file="$1"

  awk '
    BEGIN {
      n = 0
      sum = 0
      min = -1
      max = -1
    }
    {
      score = $1
      name = $2
      scores[n] = score
      names[n] = name
      sum += score
      if (min < 0 || score < min) { min = score; min_name = name }
      if (max < 0 || score > max) { max = score; max_name = name }
      n++
    }
    END {
      if (n == 0) exit 1
      avg = sum / n

      print "============================================================"
      printf("MOYENNE SESSION: %.1f / 100\n", avg)
      printf("MEILLEURE VIDEO: %s (%.1f)\n", max_name, max)
      printf("PIRE VIDEO: %s (%.1f)\n", min_name, min)

      if (avg >= 90) label = "excellent"
      else if (avg >= 75) label = "bon"
      else if (avg >= 60) label = "acceptable"
      else if (avg >= 40) label = "faible"
      else label = "mauvais"

      printf("QUALITE GLOBALE: %s\n", label)
    }
  ' "$scores_file"
}

main() {
  require_cmd ffmpeg
  require_cmd ffprobe
  require_cmd awk
  require_cmd sed
  require_cmd grep

  require_file "$VIDEOS_DIR/head.mp4"
  require_file "$VIDEOS_DIR/left.mp4"
  require_file "$VIDEOS_DIR/right.mp4"

  local tmp_scores
  tmp_scores="$(mktemp)"
  trap 'rm -f "$tmp_scores"' EXIT

  for cam in head left right; do
    local file="$VIDEOS_DIR/${cam}.mp4"
    score_video "$cam" "$file"

    local note
    note="$(score_video "$cam" "$file" | awk -F': ' '/^NOTE FINALE:/{print $2}' | awk "{print \$1}" | tail -n1)"
    [[ -n "$note" ]] || fail "Impossible de calculer la note pour $cam"
    echo "$note $cam" >> "$tmp_scores"
  done

  summarize_session "$tmp_scores"
}

main
