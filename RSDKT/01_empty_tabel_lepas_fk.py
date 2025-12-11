import subprocess

scripts = [
    "dropFK.py",
    "emptyTabel.py"
]

for s in scripts:
    print(f"Menjalankan: {s}")
    result = subprocess.run(["python", s])

    if result.returncode != 0:
        print(f"❌ Error pada script: {s}")
        break
    else:
        print(f"✔️ Selesai: {s}")
