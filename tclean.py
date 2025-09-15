from pathlib import Path
import queue
import time
import shlex
import subprocess

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

RSYNC = "rsync --verbose --archive --exclude .*"
CARD = Path("/Volumes/NO NAME")
TO_ROOT = Path("/Users/tom/Documents/Work/Music/T.O.™/Recordings/")
RECORDINGS = TO_ROOT / "mirror/LiveTrak L-12"
ARCHIVE = TO_ROOT / "archive"
RELEASE = TO_ROOT / "release"

CHANGED_DIRECTORIES = set[Path]()  # Directories containing new WAV files
SLEEP_TIME = 0.2
RUNNING = True
COMMANDS = queue.Queue[object]()
DRY_RUN = False


def main():
    watch()


def watch():
    o = Observer()
    o.schedule(CardInserted(), str(CARD))
    o.schedule(WaveFilesChanged(), str(RECORDINGS), recursive=True)
    o.start()
    try:
        while RUNNING:
            time.sleep(SLEEP_TIME)
            try:
                task = COMMANDS.get()
            except queue.Empty:
                pass
            else:
                task()
    finally:
        o.stop()
        o.join()


def backup_directory(source):
    if source.name == "Work":
        source = source.parent
    files = source.glob("*.WAV")
    assert files
    assert len(source.name) == 13, source.name
    date, _, time = source.name.partition("_")
    year, month, day = date[:2], date[2:4], date[4:]
    year_month = f"20{year}-{month}"
    base = f"{year_month}/{day}/{time}"

    archive = ARCHIVE / base
    for i in (RELEASE, archive):
        i.mkdir(parents=True, exist_ok=True)

    # Copy all .wav as FLAC
    for f in sorted(files):
        _run("ffmpeg", "-i", f, archive / f"{f.stem}.flac")

    if not (master := [f for f in files if f.name == "MASTER.WAV"]):
        print("No master!")
        return
    # Compress to mp3 at 160k
    h, m, s = time[:2], time[2:4], time[4:]
    release = RELEASE / f"{year_month}/{year_month}-{day}_{h}-{m}-{s}.mp3"
    _run("ffmpeg", "-i", master[0], "-vn", "-b:a", "160k", release)


class CardInserted(FileSystemEventHandler):
    def on_created(self, event: FileSystemEvent) -> None:
        COMMANDS.put(on_created)


def on_created():
    time.sleep(4)
    if not _is_card():
        print("Not a LiveTrak card")
        return

    print("Card inserted!")
    _run(*RSYNC.split(), CARD, RECORDINGS)

    dirs = sorted(CHANGED_DIRECTORIES)
    CHANGED_DIRECTORIES.clear()
    for d in dirs:
        backup_directory(d)


class WaveFilesChanged(FileSystemEventHandler):
    def on_created(self, e: FileSystemEvent) -> None:
        assert isinstance(e.src_path, str)
        s = Path(e.src_path)
        print("WaveFilesChanged", s)
        if s.suffix == ".WAV" and s.parent.name.startswith("FOLDER"):
            CHANGED_DIRECTORIES.add(s.parent)


def _run(*args, check=True, text=True, **kwargs):
    args = [str(a) for a in args]
    if kwargs.get("shell"):
        args = shlex.join(args)
    if not DRY_RUN:
        return subprocess.run(args, check=check, text=text, **kwargs)
    else:
        print("$", *args)


def _is_card():
    if not CARD.exists():
        return False
    d = sorted(f.name for f in CARD.iterdir() if not f.name.startswith("."))
    return d == [f"FOLDER{i:02}" for i in range(1, 11)]


if __name__ == "__main__":
    main()
