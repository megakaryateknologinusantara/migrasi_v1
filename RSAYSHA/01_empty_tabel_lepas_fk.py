import subprocess
import os

# Path direktori tempat script ini berada
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

scripts = [
    "dropFK.py",
    "emptyTabel.py"
]

for s in scripts:
    script_path = os.path.join(BASE_DIR, s)
    print(f"Menjalankan: {s}")
    result = subprocess.run(["python", script_path])

    if result.returncode != 0:
        print(f"❌ Error pada script: {s}")
        break
    else:
        print(f"✔️ Selesai: {s}")
