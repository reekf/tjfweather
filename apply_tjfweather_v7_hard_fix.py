#!/usr/bin/env python3
"""
TJFWeather V7 hard fix patch.

Run from the root of your GitHub repo:
    python3 apply_tjfweather_v7_hard_fix.py
"""
from pathlib import Path
import re
import shutil

ROOT = Path.cwd()
INDEX = ROOT / "index.html"
KEY = "BAivtsZBFQ-lH4pntR3ooI1SEvuuH25UxK8pCnw10vtnQnzfQNkZxHKcaYkJQgS2RcrGIpcO0ULXiBif3Cem6zg"

if not INDEX.exists():
    raise SystemExit("ERROR: index.html not found. Run this from the root of the tjfweather repo.")

text = INDEX.read_text(encoding="utf-8")

# Remove previous V7 injections to keep this idempotent.
text = re.sub(r"\n\s*<!-- TJF V7 HARD FIX START -->.*?<!-- TJF V7 HARD FIX END -->\s*\n", "\n", text, flags=re.S)

# Force every VAPID assignment we can find.
patterns = [
    r'(const\s+FCM_VAPID_PUBLIC_KEY\s*=\s*)["\'][^"\']*["\']',
    r'(let\s+FCM_VAPID_PUBLIC_KEY\s*=\s*)["\'][^"\']*["\']',
    r'(var\s+FCM_VAPID_PUBLIC_KEY\s*=\s*)["\'][^"\']*["\']',
    r'(window\.FCM_VAPID_PUBLIC_KEY\s*=\s*)["\'][^"\']*["\']',
    r'(window\.TJF_FCM_VAPID_PUBLIC_KEY\s*=\s*)["\'][^"\']*["\']',
]
for pat in patterns:
    text = re.sub(pat, r'\1"' + KEY + r'"', text)

# Neutralize common placeholder strings if they are used directly in conditions/messages.
text = text.replace("PASTE_YOUR_PUBLIC_KEY_HERE", KEY)
text = text.replace("REPLACE_WITH_FIREBASE_WEB_PUSH_CERTIFICATE_KEY_PAIR_PUBLIC_KEY", KEY)
text = text.replace("REPLACE_WITH_FIREBASE_WEB_PUSH_CERTIFICATE_PUBLIC_KEY", KEY)

injection = """
<!-- TJF V7 HARD FIX START -->
<link rel="stylesheet" href="design_overrides/tjf_v7_hard_override.css?v=7">
<script>
  window.TJF_FCM_VAPID_PUBLIC_KEY = "BAivtsZBFQ-lH4pntR3ooI1SEvuuH25UxK8pCnw10vtnQnzfQNkZxHKcaYkJQgS2RcrGIpcO0ULXiBif3Cem6zg";
  window.FCM_VAPID_PUBLIC_KEY = "BAivtsZBFQ-lH4pntR3ooI1SEvuuH25UxK8pCnw10vtnQnzfQNkZxHKcaYkJQgS2RcrGIpcO0ULXiBif3Cem6zg";
  window.__TJF_V7_PATCH_MARKER = "TJF V7 HARD FIX";
</script>
<script src="design_overrides/tjf_v7_hard_fix.js?v=7"></script>
<script type="module" src="design_overrides/tjf_v7_fcm_hard_fix.js?v=7"></script>
<!-- TJF V7 HARD FIX END -->
""".replace("BAivtsZBFQ-lH4pntR3ooI1SEvuuH25UxK8pCnw10vtnQnzfQNkZxHKcaYkJQgS2RcrGIpcO0ULXiBif3Cem6zg", KEY)

if "</body>" not in text:
    raise SystemExit("ERROR: index.html does not contain </body>.")
text = text.replace("</body>", injection + "\n</body>")

INDEX.write_text(text, encoding="utf-8")

# Copy assets.
(ROOT / "design_overrides").mkdir(exist_ok=True)
for name in ["tjf_v7_hard_override.css", "tjf_v7_hard_fix.js", "tjf_v7_fcm_hard_fix.js"]:
    src = Path(__file__).resolve().parent / "design_overrides" / name
    dst = ROOT / "design_overrides" / name
    if src.resolve() != dst.resolve():
        shutil.copyfile(src, dst)

for name in ["sw.js", "firebase-messaging-sw.js"]:
    src = Path(__file__).resolve().parent / name
    dst = ROOT / name
    if src.resolve() != dst.resolve():
        shutil.copyfile(src, dst)

print("TJF V7 hard fix applied.")
print("Verify with:")
print('  grep -n "TJF V7\\|FCM_VAPID_PUBLIC_KEY\\|maxNativeZoom\\|tjf_v7" index.html design_overrides/*.js')
print("  git diff -- index.html design_overrides/tjf_v7_hard_override.css design_overrides/tjf_v7_hard_fix.js design_overrides/tjf_v7_fcm_hard_fix.js sw.js firebase-messaging-sw.js")
