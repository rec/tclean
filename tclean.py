from pathlib import Path
import os
import queue
import re
import time
import shlex
import subprocess

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

RSYNC = "rsync --verbose --archive --exclude=\".*\" --prune-empty-dirs"
CARD = "/Volumes/NO NAME"
TO_ROOT = Path("/Users/tom/Documents/Work/Music/T.O.T.M./Recordings/")

# relative to TO_ROOT
RECORDINGS = "mirror/LiveTrak L-12"
ARCHIVE = "archive"
RELEASE = "release"

CHANGED_DIRECTORIES = set[Path]()  # Directories containing new WAV files
SLEEP_TIME = 0.2
RUNNING = True
COMMANDS = queue.Queue[object]()
DRY_RUN = False
VERBOSE = True

FILE_RE = re.compile(r"\d{6}_\d{6}")

FFMPEG = "ffmpeg", "-y", "-i",


def main():
    watch()


def watch():
    os.chdir(str(TO_ROOT))
    for i in (RELEASE, ARCHIVE, RECORDINGS):
        Path(i).mkdir(parents=True, exist_ok=True)

    o = Observer()
    o.schedule(CardInserted(), CARD)
    o.schedule(WaveFilesChanged(), str(TO_ROOT), recursive=True)
    o.start()
    try:
        i = 0
        on_created()

        while RUNNING:
            if not (i := i + 1) % 10:
                print(".")
            time.sleep(SLEEP_TIME)
            try:
                task = COMMANDS.get(block=False)
            except queue.Empty:
                pass
            else:
                task()

    finally:
        o.stop()
        o.join()


def backup_directory(source):
    if not (files := sorted(source.glob("*.WAV"))):
        print("No wave files in", source)
        return

    if source.name == "Work":
        source = source.parent

    if not FILE_RE.match(source.name):
        print("Directory doesn't match pattern", source)
        return

    print("Backing up", source)

    date, _, time = source.name.partition("_")
    year, month, day = date[:2], date[2:4], date[4:]
    year_month = f"20{year}-{month}"
    base = f"{year_month}/{source.name}"

    archive = Path(f"{ARCHIVE}/{base}")
    archive.mkdir(parents=True, exist_ok=True)

    # Copy all .wav as FLAC
    for f in files:
        _run(*FFMPEG, f, archive / f"{f.stem}.flac")

    if (ztd := source / "PRJDATA.ZDT").exists():
        _run("cp", ztd, archive)
    else:
        print(ztd, "does not exist!")

    if master := [f for f in files if f.name == "MASTER.WAV"]:
        # Compress to mp3 at 160k
        h, m, s = time[:2], time[2:4], time[4:]
        release = Path(RELEASE) / f"{year_month}/{year_month}-{day}_{h}-{m}-{s}.mp3"
        release.parent.mkdir(exist_ok=True, parents=True)
        _run(*FFMPEG, master[0], "-vn", "-b:a", "160k", release)
    else:
        print("No master!")


class CardInserted(FileSystemEventHandler):
    def on_created(self, event: FileSystemEvent) -> None:
        COMMANDS.put(lambda: time.sleep(4) or on_created())


def on_created():
    if not _is_card():
        print("Not a LiveTrak card")
        return

    print("Card inserted!")
    _run(*RSYNC.split(), CARD, RECORDINGS, capture_output=True, check=False)

    dirs = sorted(CHANGED_DIRECTORIES)
    CHANGED_DIRECTORIES.clear()

    for d in dirs:
        backup_directory(d)


class WaveFilesChanged(FileSystemEventHandler):
    def on_any_event(self, e: FileSystemEvent) -> None:
        CHANGED_DIRECTORIES.add(Path(e.src_path).parent)


def _run(*args, check=True, text=True, capture_output=not VERBOSE, **kwargs):
    args = [str(a) for a in args]
    print("$", *args)
    if kwargs.get("shell"):
        args = shlex.join(args)
    if not DRY_RUN:
        return subprocess.run(args, check=check, text=text, **kwargs)
    else:
        print("$", *args)


def _is_card():
    card = Path(CARD)
    if not card.exists():
        return False
    d = sorted(f.name for f in card.iterdir() if not f.name.startswith("."))
    return d == [f"FOLDER{i:02}" for i in range(1, 11)]


if __name__ == "__main__":
    main()
