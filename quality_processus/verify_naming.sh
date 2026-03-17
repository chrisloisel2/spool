#!/usr/bin/env python3

import argparse
import json
import math
import os
from dataclasses import dataclass, asdict

import cv2
import numpy as np


@dataclass
class VideoScore:
    path: str
    frames_total: int
    frames_used: int
    certainty: float
    details: dict


def clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, x))


def mean_gray(img: np.ndarray) -> np.ndarray:
    if len(img.shape) == 3:
        return cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    return img


def open_video(path: str):
    cap = cv2.VideoCapture(path)
    if not cap.isOpened():
        raise RuntimeError(f"Impossible d'ouvrir la vidéo: {path}")
    return cap


def get_frame_count(cap: cv2.VideoCapture) -> int:
    n = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    return max(n, 0)


def sample_frame_indices(total: int, step: int) -> list[int]:
    if total <= 0:
        return []
    idx = list(range(0, total, step))
    if idx[-1] != total - 1:
        idx.append(total - 1)
    return idx


def circular_border_score(frame: np.ndarray) -> tuple[float, dict]:
    """
    Détecte la signature 'head':
    - contour noir circulaire / vignette forte
    - centre plus lumineux que la périphérie
    - périphérie globalement homogène
    """
    gray = mean_gray(frame).astype(np.float32) / 255.0
    h, w = gray.shape[:2]
    cy, cx = h / 2.0, w / 2.0

    y, x = np.indices((h, w))
    rx = (x - cx) / (w / 2.0)
    ry = (y - cy) / (h / 2.0)
    r = np.sqrt(rx * rx + ry * ry)

    center = gray[r <= 0.45]
    mid = gray[(r > 0.55) & (r <= 0.75)]
    border = gray[r > 0.82]

    if len(center) == 0 or len(border) == 0:
        return 0.0, {"center_mean": 0.0, "border_mean": 0.0, "radial_drop": 0.0, "border_dark": 0.0}

    center_mean = float(center.mean())
    mid_mean = float(mid.mean()) if len(mid) else center_mean
    border_mean = float(border.mean())

    radial_drop = clamp((center_mean - border_mean) / 0.45)
    border_dark = clamp((0.28 - border_mean) / 0.28) if border_mean < 0.28 else 0.0

    # Uniformité de la couronne noire
    border_std = float(border.std())
    border_uniformity = clamp(1.0 - border_std / 0.18)

    # Signature 'fish-eye circulaire'
    score = 0.45 * radial_drop + 0.35 * border_dark + 0.20 * border_uniformity
    score = clamp(score)

    return score, {
        "center_mean": center_mean,
        "mid_mean": mid_mean,
        "border_mean": border_mean,
        "radial_drop": radial_drop,
        "border_dark": border_dark,
        "border_uniformity": border_uniformity,
    }


def dark_mass_side_score(frame: np.ndarray, expected_side: str) -> tuple[float, dict]:
    """
    Détecte les vues pince:
    - beaucoup moins de vignette circulaire que head
    - masse sombre/occlusion mécanique plus présente d'un côté
    - on regarde surtout la moitié basse + latérale
    expected_side:
      - "left"  -> la masse sombre attendue est à gauche de l'image
      - "right" -> la masse sombre attendue est à droite de l'image
    """
    gray = mean_gray(frame).astype(np.float32) / 255.0
    h, w = gray.shape[:2]

    # Ignore la zone haute, garde la zone utile où les pinces entrent le plus souvent
    roi = gray[int(0.35 * h):, :]
    hh, ww = roi.shape[:2]

    left_band = roi[:, : int(0.33 * ww)]
    center_band = roi[:, int(0.33 * ww): int(0.66 * ww)]
    right_band = roi[:, int(0.66 * ww):]

    # "darkness" élevé => zone sombre / objet noir
    left_dark = float((1.0 - left_band).mean())
    center_dark = float((1.0 - center_band).mean())
    right_dark = float((1.0 - right_band).mean())

    if expected_side == "left":
        side_delta = left_dark - right_dark
    else:
        side_delta = right_dark - left_dark

    side_preference = clamp((side_delta + 0.08) / 0.25)
    object_presence = clamp((max(left_dark, right_dark) - 0.28) / 0.30)
    center_penalty = clamp((center_dark - max(left_dark, right_dark) + 0.05) / 0.20)
    center_bonus = 1.0 - center_penalty

    # Rejette les images de type head
    head_like, _ = circular_border_score(frame)
    not_head = 1.0 - head_like

    score = 0.45 * side_preference + 0.30 * object_presence + 0.15 * center_bonus + 0.10 * not_head
    score = clamp(score)

    return score, {
        "left_dark": left_dark,
        "center_dark": center_dark,
        "right_dark": right_dark,
        "side_delta": side_delta,
        "side_preference": side_preference,
        "object_presence": object_presence,
        "center_bonus": center_bonus,
        "not_head": not_head,
    }


def analyze_video(path: str, mode: str, sample_step: int) -> VideoScore:
    cap = open_video(path)
    total = get_frame_count(cap)
    indices = sample_frame_indices(total, sample_step)

    if not indices:
        cap.release()
        raise RuntimeError(f"Aucune frame lisible: {path}")

    scores = []
    metrics_acc = {}

    for idx in indices:
        cap.set(cv2.CAP_PROP_POS_FRAMES, idx)
        ok, frame = cap.read()
        if not ok or frame is None:
            continue

        if mode == "head":
            score, details = circular_border_score(frame)
        elif mode == "left":
            score, details = dark_mass_side_score(frame, expected_side="left")
        elif mode == "right":
            score, details = dark_mass_side_score(frame, expected_side="right")
        else:
            cap.release()
            raise ValueError(f"Mode inconnu: {mode}")

        scores.append(score)
        for k, v in details.items():
            metrics_acc.setdefault(k, []).append(float(v))

    cap.release()

    if not scores:
        raise RuntimeError(f"Impossible d'analyser les frames: {path}")

    details = {k: float(np.mean(v)) for k, v in metrics_acc.items()}
    certainty = float(np.mean(scores))

    return VideoScore(
        path=path,
        frames_total=total,
        frames_used=len(scores),
        certainty=certainty,
        details=details,
    )


def analyze_session(session_dir: str, sample_step: int) -> dict:
    videos_dir = os.path.join(session_dir, "videos")

    expected = {
        "head": os.path.join(videos_dir, "head.mp4"),
        "left": os.path.join(videos_dir, "left.mp4"),
        "right": os.path.join(videos_dir, "right.mp4"),
    }

    for name, path in expected.items():
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Fichier absent: {path}")

    head_score = analyze_video(expected["head"], "head", sample_step)
    left_score = analyze_video(expected["left"], "left", sample_step)
    right_score = analyze_video(expected["right"], "right", sample_step)

    # Vérification croisée minimale:
    # on mesure aussi si les vidéos pince ressemblent anormalement à head
    left_as_head = analyze_video(expected["left"], "head", sample_step).certainty
    right_as_head = analyze_video(expected["right"], "head", sample_step).certainty
    head_as_left = analyze_video(expected["head"], "left", sample_step).certainty
    head_as_right = analyze_video(expected["head"], "right", sample_step).certainty

    # Certitude finale: score attendu moins confusion inverse
    final_head = clamp(head_score.certainty - 0.5 * max(head_as_left, head_as_right))
    final_left = clamp(left_score.certainty - 0.5 * left_as_head)
    final_right = clamp(right_score.certainty - 0.5 * right_as_head)

    return {
        "session_dir": session_dir,
        "sample_step": sample_step,
        "result": {
            "head_name_is_correct": final_head,
            "left_name_is_correct": final_left,
            "right_name_is_correct": final_right,
        },
        "raw_scores": {
            "head_as_head": head_score.certainty,
            "left_as_left": left_score.certainty,
            "right_as_right": right_score.certainty,
            "left_as_head_confusion": left_as_head,
            "right_as_head_confusion": right_as_head,
            "head_as_left_confusion": head_as_left,
            "head_as_right_confusion": head_as_right,
        },
        "details": {
            "head": asdict(head_score),
            "left": asdict(left_score),
            "right": asdict(right_score),
        },
        "interpretation": {
            "0.90_1.00": "très forte certitude",
            "0.75_0.89": "bonne certitude",
            "0.60_0.74": "acceptable mais à vérifier",
            "0.40_0.59": "douteux",
            "0.00_0.39": "mauvais nommage probable",
        },
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("session_dir", help="Racine de session, ex: /srv/exoria/inbox/videos/session_20260316_174007")
    parser.add_argument("--sample-step", type=int, default=3, help="Analyse une frame sur N. Mettre 1 pour tout analyser.")
    parser.add_argument("--output-json", default="", help="Chemin de sortie JSON")
    args = parser.parse_args()

    if args.sample_step < 1:
        raise SystemExit("--sample-step doit être >= 1")

    report = analyze_session(args.session_dir, args.sample_step)

    txt = json.dumps(report, indent=2, ensure_ascii=False)
    print(txt)

    if args.output_json:
        with open(args.output_json, "w", encoding="utf-8") as f:
            f.write(txt)
            f.write("\n")


if __name__ == "__main__":
    main()
